package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	historyv1 "github.com/linkflow/engine/api/gen/linkflow/history/v1"
	"github.com/linkflow/engine/internal/worker/adapter"
)

type WorkflowExecutor struct {
	historyClient    *adapter.HistoryClient
	logger           *slog.Logger
	executorRegistry *Registry
}

func NewWorkflowExecutor(client *adapter.HistoryClient, logger *slog.Logger) *WorkflowExecutor {
	return &WorkflowExecutor{
		historyClient: client,
		logger:        logger,
	}
}

func (e *WorkflowExecutor) SetRegistry(registry *Registry) {
	e.executorRegistry = registry
}

func (e *WorkflowExecutor) NodeType() string {
	return "workflow"
}

// Execute is now pure decision logic. It returns a list of Commands marshaled in Output.
func (e *WorkflowExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	e.logger.Info("deciding workflow", slog.String("workflow_id", req.WorkflowID))

	// 1. Fetch History
	namespace := req.Namespace
	if namespace == "" {
		namespace = "default"
	}
	resp, err := e.historyClient.GetHistory(ctx, namespace, req.WorkflowID, req.RunID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch history: %w", err)
	}

	events := resp.GetHistory().GetEvents()
	if len(events) == 0 {
		return nil, fmt.Errorf("history is empty")
	}

	// TODO: Implement sticky execution caching to avoid full history replay.
	// For now, log a warning when history is large to track performance impact.
	if len(events) > 500 {
		e.logger.Warn("large history replay detected; consider implementing sticky execution",
			slog.String("workflow_id", req.WorkflowID),
			slog.Int("event_count", len(events)),
		)
	}

	// 2. Parse Payload from ExecutionStarted
	var payload JobPayload
	var payloadFound bool

	for _, event := range events {
		if event.GetEventType() == commonv1.EventType_EVENT_TYPE_EXECUTION_STARTED {
			attr := event.GetExecutionStartedAttributes()
			// Assume payload is in first input
			if attr != nil && attr.GetInput() != nil && len(attr.GetInput().GetPayloads()) > 0 {
				inputData := attr.GetInput().GetPayloads()[0].GetData()
				if err := json.Unmarshal(inputData, &payload); err == nil {
					payloadFound = true
				}
			}
			break
		}
	}

	if !payloadFound {
		return nil, fmt.Errorf("workflow definition not found in execution input")
	}

	// 3. Replay History to build State
	nodeStates := make(map[string]string) // NodeID -> Status
	nodeOutputs := make(map[string][]byte)
	eventIDToNodeID := make(map[int64]string)

	for _, event := range events {
		switch event.GetEventType() {
		case commonv1.EventType_EVENT_TYPE_NODE_SCHEDULED:
			attr := event.GetNodeScheduledAttributes()
			nodeStates[attr.GetNodeId()] = "Scheduled"
			eventIDToNodeID[event.GetEventId()] = attr.GetNodeId()

		case commonv1.EventType_EVENT_TYPE_NODE_COMPLETED:
			attr := event.GetNodeCompletedAttributes()
			if nodeID, ok := eventIDToNodeID[attr.GetScheduledEventId()]; ok {
				nodeStates[nodeID] = "Completed"
				if attr.GetResult() != nil && len(attr.GetResult().GetPayloads()) > 0 {
					nodeOutputs[nodeID] = attr.GetResult().GetPayloads()[0].GetData()
				}
			}

		case commonv1.EventType_EVENT_TYPE_NODE_FAILED:
			attr := event.GetNodeFailedAttributes()
			if nodeID, ok := eventIDToNodeID[attr.GetScheduledEventId()]; ok {
				nodeStates[nodeID] = "Failed"
			}
		}
	}

	// 4. Decide Next Steps
	commands := []*historyv1.Command{}
	graph := payload.Workflow

	// Build node lookup map for quick access
	nodeMap := make(map[string]*Node)
	for i := range graph.Nodes {
		nodeMap[graph.Nodes[i].ID] = &graph.Nodes[i]
	}

	// Track skipped nodes (due to conditional branching or failed-continue dependencies)
	skippedNodes := make(map[string]bool)

	// Check for failed nodes — apply per-node error policy
	hasFatalFailure := false
	var failureMessage string
	for nodeID, status := range nodeStates {
		if status == "Failed" {
			node := nodeMap[nodeID]
			errorPolicy := "stop"
			if node != nil {
				errorPolicy = node.GetOnError()
			}

			if errorPolicy == "continue" {
				e.logger.Info("node failed with continue-on-error policy",
					slog.String("node_id", nodeID),
				)
				// Mark downstream dependents of this failed node as skipped
				e.skipDependents(nodeID, graph.Edges, nodeStates, skippedNodes)
			} else {
				hasFatalFailure = true
				failureMessage = fmt.Sprintf("node '%s' failed", nodeID)
				break
			}
		}
	}

	if hasFatalFailure {
		cmd := &historyv1.Command{
			CommandType: historyv1.CommandType_COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
			Attributes: &historyv1.Command_FailWorkflowExecutionAttributes{
				FailWorkflowExecutionAttributes: &historyv1.FailWorkflowExecutionCommandAttributes{
					Failure: &commonv1.Failure{
						Message: failureMessage,
					},
				},
			},
		}
		commands = append(commands, cmd)

		outputBytes, err := json.Marshal(commands)
		if err != nil {
			return nil, err
		}
		return &ExecuteResponse{Output: outputBytes}, nil
	}

	allNodesDone := true
	nodesToSchedule := []Node{}
	inputs := make(map[string]json.RawMessage)

	for _, node := range graph.Nodes {
		state := nodeStates[node.ID]
		isTerminal := state == "Completed" || state == "Failed" || skippedNodes[node.ID]

		if !isTerminal {
			allNodesDone = false
		}

		// Skip already scheduled/completed/failed/skipped nodes
		if state != "" || skippedNodes[node.ID] {
			continue
		}

		// Determine if this is a trigger node
		isTrigger := node.Type == "trigger_manual" || node.Type == "trigger_webhook" || node.Type == "trigger_schedule"

		// Find incoming edges
		var incomingEdges []Edge
		for _, edge := range graph.Edges {
			if edge.Target == node.ID {
				incomingEdges = append(incomingEdges, edge)
			}
		}

		if isTrigger || len(incomingEdges) == 0 {
			// Root node (trigger or no incoming edges) — schedule with trigger data
			nodesToSchedule = append(nodesToSchedule, node)
			triggerDataBytes, _ := json.Marshal(payload.TriggerData)
			inputs[node.ID] = triggerDataBytes
			continue
		}

		// Check if all dependencies are resolved (completed, failed-continue, or skipped)
		canRun := true
		shouldSkip := false
		mergedInput := make(map[string]json.RawMessage)

		for _, edge := range incomingEdges {
			sourceState := nodeStates[edge.Source]
			sourceSkipped := skippedNodes[edge.Source]

			// If upstream is skipped, this node should also be skipped
			if sourceSkipped {
				shouldSkip = true
				break
			}

			// If upstream is a failed node with "continue" policy, skip this node
			if sourceState == "Failed" {
				shouldSkip = true
				break
			}

			if sourceState != "Completed" {
				canRun = false
				break
			}

			// Conditional branching: check if this edge should be taken
			sourceNode := nodeMap[edge.Source]
			if sourceNode != nil && sourceNode.IsConditionType() && edge.SourceHandle != "" {
				// Parse the condition node's output to get the selected branch
				if output, ok := nodeOutputs[edge.Source]; ok {
					var condResult struct {
						Output string `json:"output"`
					}
					if err := json.Unmarshal(output, &condResult); err == nil {
						if edge.SourceHandle != condResult.Output {
							// This branch was not selected by the condition
							shouldSkip = true
							e.logger.Info("skipping node due to unmatched condition branch",
								slog.String("node_id", node.ID),
								slog.String("edge_source_handle", edge.SourceHandle),
								slog.String("condition_output", condResult.Output),
							)
							break
						}
					}
				}
			}

			if output, ok := nodeOutputs[edge.Source]; ok {
				mergedInput[edge.Source] = output
			}
		}

		if shouldSkip {
			skippedNodes[node.ID] = true
			e.skipDependents(node.ID, graph.Edges, nodeStates, skippedNodes)
			continue
		}

		if canRun {
			nodesToSchedule = append(nodesToSchedule, node)
			if len(mergedInput) == 1 {
				for _, v := range mergedInput {
					inputs[node.ID] = v
				}
			} else {
				mergedBytes, _ := json.Marshal(mergedInput)
				inputs[node.ID] = mergedBytes
			}
		}
	}

	// Generate ScheduleActivity Commands
	for _, node := range nodesToSchedule {
		cmd := e.buildScheduleCommand(node, inputs[node.ID], payload.Deterministic)
		if cmd != nil {
			commands = append(commands, cmd)
		}
	}

	// 5. Check for Workflow Completion
	// Re-check: all nodes must be completed, failed (continue), or skipped
	if allNodesDone {
		resultStatus := "completed"
		// Check if any nodes failed with continue policy
		for nodeID := range nodeStates {
			if nodeStates[nodeID] == "Failed" {
				resultStatus = "partial_failure"
				break
			}
		}

		resultBytes, _ := json.Marshal(map[string]string{"status": resultStatus})
		cmd := &historyv1.Command{
			CommandType: historyv1.CommandType_COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &historyv1.Command_CompleteWorkflowExecutionAttributes{
				CompleteWorkflowExecutionAttributes: &historyv1.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonv1.Payloads{
						Payloads: []*commonv1.Payload{{Data: resultBytes}},
					},
				},
			},
		}
		commands = append(commands, cmd)
	}

	// Marshal commands to Output
	outputBytes, err := json.Marshal(commands)
	if err != nil {
		return nil, err
	}

	return &ExecuteResponse{
		Output: outputBytes,
	}, nil
}

// skipDependents recursively marks all downstream nodes of a given node as skipped.
func (e *WorkflowExecutor) skipDependents(nodeID string, edges []Edge, nodeStates map[string]string, skippedNodes map[string]bool) {
	for _, edge := range edges {
		if edge.Source == nodeID {
			targetID := edge.Target
			if !skippedNodes[targetID] && nodeStates[targetID] == "" {
				skippedNodes[targetID] = true
				e.skipDependents(targetID, edges, nodeStates, skippedNodes)
			}
		}
	}
}

// buildScheduleCommand creates a ScheduleActivityTask command for a node.
func (e *WorkflowExecutor) buildScheduleCommand(node Node, inputData json.RawMessage, deterministic DeterministicContext) *historyv1.Command {
	if inputData == nil {
		inputData = []byte("{}")
	}

	var nodeData struct {
		Config json.RawMessage `json:"config"`
	}
	configBytes := node.Data
	if err := json.Unmarshal(node.Data, &nodeData); err == nil && len(nodeData.Config) > 0 {
		configBytes = nodeData.Config
	}
	if len(configBytes) == 0 {
		configBytes = []byte("{}")
	}

	envelopeBytes, err := json.Marshal(struct {
		Input         json.RawMessage     `json:"input"`
		Config        json.RawMessage     `json:"config"`
		NodeID        string              `json:"node_id"`
		Type          string              `json:"node_type"`
		Deterministic DeterministicContext `json:"deterministic"`
	}{
		Input:         json.RawMessage(inputData),
		Config:        json.RawMessage(configBytes),
		NodeID:        node.ID,
		Type:          node.Type,
		Deterministic: deterministic,
	})
	if err != nil {
		e.logger.Error("failed to marshal activity envelope",
			slog.String("node_id", node.ID),
			slog.String("error", err.Error()),
		)
		return nil
	}

	return &historyv1.Command{
		CommandType: historyv1.CommandType_COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &historyv1.Command_ScheduleActivityTaskAttributes{
			ScheduleActivityTaskAttributes: &historyv1.ScheduleActivityTaskCommandAttributes{
				NodeId:   node.ID,
				NodeType: node.Type,
				Name:     node.GetName(),
				Input: &commonv1.Payloads{
					Payloads: []*commonv1.Payload{{Data: envelopeBytes}},
				},
				TaskQueue: "default",
				Config:    configBytes,
			},
		},
	}
}
