package executor

import "encoding/json"

type JobPayload struct {
	JobID         string                 `json:"job_id"`
	CallbackToken string                 `json:"callback_token"`
	ExecutionID   int                    `json:"execution_id"`
	WorkflowID    int                    `json:"workflow_id"`
	WorkspaceID   int                    `json:"workspace_id"`
	Workflow      WorkflowDefinition     `json:"workflow"`
	TriggerData   map[string]interface{} `json:"trigger_data"`
	Credentials   map[string]interface{} `json:"credentials"`
	Variables     map[string]interface{} `json:"variables"`
	CallbackURL   string                 `json:"callback_url"`
	ProgressURL   string                 `json:"progress_url"`
	Deterministic DeterministicContext   `json:"deterministic"`
}

type WorkflowDefinition struct {
	Nodes    []Node                 `json:"nodes"`
	Edges    []Edge                 `json:"edges"`
	Settings map[string]interface{} `json:"settings"`
}

type Node struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Position Position        `json:"position"`
	Data     json.RawMessage `json:"data"`
}

// GetName extracts the node name from the Data field, or returns the ID as fallback
func (n *Node) GetName() string {
	var data struct {
		Label string `json:"label"`
		Name  string `json:"name"`
	}
	if err := json.Unmarshal(n.Data, &data); err == nil {
		if data.Label != "" {
			return data.Label
		}
		if data.Name != "" {
			return data.Name
		}
	}
	return n.ID
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type Edge struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	SourceHandle string `json:"sourceHandle,omitempty"`
	TargetHandle string `json:"targetHandle,omitempty"`
	Label        string `json:"label,omitempty"`
	Condition    string `json:"condition,omitempty"`
}

// GetOnError extracts the onError policy from a node's Data field.
// Returns "stop" (default), "continue".
func (n *Node) GetOnError() string {
	var data struct {
		OnError string `json:"onError"`
	}
	if err := json.Unmarshal(n.Data, &data); err == nil && data.OnError != "" {
		return data.OnError
	}
	return "stop"
}

// IsConditionType returns true if the node is a condition or logic_condition type.
func (n *Node) IsConditionType() bool {
	return n.Type == "condition" || n.Type == "logic_condition"
}
