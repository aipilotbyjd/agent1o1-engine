package graph

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrCycleDetected = errors.New("cycle detected in workflow graph")
	ErrNoEntryNode   = errors.New("no entry node found")
	ErrInvalidEdge   = errors.New("invalid edge")
)

// DAG represents a directed acyclic graph of workflow nodes.
type DAG struct {
	Nodes        map[string]*Node
	Edges        map[string][]string // source -> targets
	ReverseEdges map[string][]string // target -> sources

	// EdgeMap stores edge metadata (source -> target -> EdgeInfo)
	// This enables conditional branching by preserving sourceHandle
	EdgeMap map[string]map[string]*EdgeInfo

	EntryNodes []string
	ExitNodes  []string

	// Topological ordering
	Order  []string
	Levels map[string]int
}

// EdgeInfo stores metadata about an edge.
type EdgeInfo struct {
	SourceHandle string `json:"sourceHandle"`
	TargetHandle string `json:"targetHandle"`
	Label        string `json:"label"`
	Condition    string `json:"condition"`
}

// Node represents a node in the DAG.
type Node struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Name       string          `json:"name"`
	Config     json.RawMessage `json:"config"`
	Position   Position        `json:"position"`
	Conditions []Condition     `json:"conditions"`
}

// Position represents node position in the editor.
type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Condition represents a conditional edge.
type Condition struct {
	TargetNode string `json:"target_node"`
	Expression string `json:"expression"` // CEL expression
	Order      int    `json:"order"`
}

// WorkflowDefinition represents a workflow from the editor.
type WorkflowDefinition struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Nodes []NodeDef `json:"nodes"`
	Edges []EdgeDef `json:"edges"`
}

// NodeDef represents a node definition from the editor.
type NodeDef struct {
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	Position Position `json:"position"`
	Data     NodeData `json:"data"`
}

// NodeData represents node data from the editor.
type NodeData struct {
	Label  string          `json:"label"`
	Config json.RawMessage `json:"config"`
}

// EdgeDef represents an edge definition from the editor.
type EdgeDef struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	SourceHandle string `json:"sourceHandle,omitempty"`
	TargetHandle string `json:"targetHandle,omitempty"`
	Label        string `json:"label,omitempty"`
	Condition    string `json:"condition,omitempty"`
}

// BuildDAG builds a DAG from a workflow definition.
func BuildDAG(workflow *WorkflowDefinition) (*DAG, error) {
	dag := &DAG{
		Nodes:        make(map[string]*Node),
		Edges:        make(map[string][]string),
		ReverseEdges: make(map[string][]string),
		EdgeMap:      make(map[string]map[string]*EdgeInfo),
		Levels:       make(map[string]int),
	}

	// Add nodes
	for _, n := range workflow.Nodes {
		dag.Nodes[n.ID] = &Node{
			ID:       n.ID,
			Type:     n.Type,
			Name:     n.Data.Label,
			Config:   n.Data.Config,
			Position: n.Position,
		}
	}

	// Add edges
	for _, e := range workflow.Edges {
		if _, exists := dag.Nodes[e.Source]; !exists {
			return nil, fmt.Errorf("%w: source node %s not found", ErrInvalidEdge, e.Source)
		}
		if _, exists := dag.Nodes[e.Target]; !exists {
			return nil, fmt.Errorf("%w: target node %s not found", ErrInvalidEdge, e.Target)
		}

		dag.Edges[e.Source] = append(dag.Edges[e.Source], e.Target)
		dag.ReverseEdges[e.Target] = append(dag.ReverseEdges[e.Target], e.Source)

		// Store edge metadata for conditional branching
		if dag.EdgeMap[e.Source] == nil {
			dag.EdgeMap[e.Source] = make(map[string]*EdgeInfo)
		}
		dag.EdgeMap[e.Source][e.Target] = &EdgeInfo{
			SourceHandle: e.SourceHandle,
			TargetHandle: e.TargetHandle,
			Label:        e.Label,
			Condition:    e.Condition,
		}

		// Handle conditions
		if e.Condition != "" {
			node := dag.Nodes[e.Source]
			node.Conditions = append(node.Conditions, Condition{
				TargetNode: e.Target,
				Expression: e.Condition,
				Order:      len(node.Conditions),
			})
		}
	}

	// Find entry nodes (no incoming edges)
	for id := range dag.Nodes {
		if len(dag.ReverseEdges[id]) == 0 {
			dag.EntryNodes = append(dag.EntryNodes, id)
		}
	}

	if len(dag.EntryNodes) == 0 {
		return nil, ErrNoEntryNode
	}

	// Find exit nodes (no outgoing edges)
	for id := range dag.Nodes {
		if len(dag.Edges[id]) == 0 {
			dag.ExitNodes = append(dag.ExitNodes, id)
		}
	}

	// Compute topological order
	if err := dag.computeTopologicalOrder(); err != nil {
		return nil, err
	}

	return dag, nil
}

func (d *DAG) computeTopologicalOrder() error {
	visited := make(map[string]bool)
	temp := make(map[string]bool)
	order := make([]string, 0, len(d.Nodes))

	var visit func(string) error
	visit = func(id string) error {
		if temp[id] {
			return ErrCycleDetected
		}
		if visited[id] {
			return nil
		}

		temp[id] = true

		for _, next := range d.Edges[id] {
			if err := visit(next); err != nil {
				return err
			}
		}

		delete(temp, id)
		visited[id] = true
		order = append([]string{id}, order...)

		return nil
	}

	for id := range d.Nodes {
		if !visited[id] {
			if err := visit(id); err != nil {
				return err
			}
		}
	}

	d.Order = order

	// Compute levels for parallel execution
	for _, id := range order {
		level := 0
		for _, prev := range d.ReverseEdges[id] {
			if d.Levels[prev] >= level {
				level = d.Levels[prev] + 1
			}
		}
		d.Levels[id] = level
	}

	return nil
}

// GetParallelNodes returns nodes that can execute in parallel at a given level.
func (d *DAG) GetParallelNodes(level int) []string {
	var nodes []string
	for id, l := range d.Levels {
		if l == level {
			nodes = append(nodes, id)
		}
	}
	return nodes
}

// GetMaxLevel returns the maximum level in the DAG.
func (d *DAG) GetMaxLevel() int {
	maxLevel := 0
	for _, level := range d.Levels {
		if level > maxLevel {
			maxLevel = level
		}
	}
	return maxLevel
}

// GetNextNodes returns nodes ready to execute given completed nodes.
func (d *DAG) GetNextNodes(completed map[string]bool) []string {
	var ready []string

	for id := range d.Nodes {
		if completed[id] {
			continue
		}

		// Check if all dependencies are satisfied
		allDependenciesMet := true
		for _, dep := range d.ReverseEdges[id] {
			if !completed[dep] {
				allDependenciesMet = false
				break
			}
		}

		if allDependenciesMet {
			ready = append(ready, id)
		}
	}

	return ready
}

// GetDependencies returns all dependencies for a node.
func (d *DAG) GetDependencies(nodeID string) []string {
	return d.ReverseEdges[nodeID]
}

// GetDependents returns all nodes that depend on a node.
func (d *DAG) GetDependents(nodeID string) []string {
	return d.Edges[nodeID]
}

// GetEdgeInfo returns the edge metadata between two nodes.
func (d *DAG) GetEdgeInfo(source, target string) *EdgeInfo {
	if targetMap, ok := d.EdgeMap[source]; ok {
		return targetMap[target]
	}
	return nil
}

// GetPath returns the path from entry to a specific node.
func (d *DAG) GetPath(nodeID string) []string {
	path := []string{nodeID}

	var traverse func(id string)
	traverse = func(id string) {
		deps := d.ReverseEdges[id]
		if len(deps) > 0 {
			// Take the first dependency (for simple paths)
			path = append([]string{deps[0]}, path...)
			traverse(deps[0])
		}
	}

	traverse(nodeID)
	return path
}

// Validate validates the DAG structure.
func (d *DAG) Validate() []ValidationError {
	var errors []ValidationError

	// Check for isolated nodes
	for id := range d.Nodes {
		if len(d.Edges[id]) == 0 && len(d.ReverseEdges[id]) == 0 {
			errors = append(errors, ValidationError{
				NodeID:  id,
				Message: "isolated node with no connections",
			})
		}
	}

	// Check for multiple entry points of different types
	triggerCount := 0
	for _, id := range d.EntryNodes {
		node := d.Nodes[id]
		if isTriggerNode(node.Type) {
			triggerCount++
		}
	}

	if triggerCount == 0 {
		errors = append(errors, ValidationError{
			Message: "workflow must have at least one trigger node",
		})
	}

	return errors
}

// ValidationError represents a DAG validation error.
type ValidationError struct {
	NodeID  string
	Message string
}

func isTriggerNode(nodeType string) bool {
	triggers := map[string]bool{
		"trigger_manual":   true,
		"trigger_webhook":  true,
		"trigger_schedule": true,
		"trigger_event":    true,
	}
	return triggers[nodeType]
}

// Clone creates a deep copy of the DAG.
func (d *DAG) Clone() *DAG {
	clone := &DAG{
		Nodes:        make(map[string]*Node, len(d.Nodes)),
		Edges:        make(map[string][]string, len(d.Edges)),
		ReverseEdges: make(map[string][]string, len(d.ReverseEdges)),
		EdgeMap:      make(map[string]map[string]*EdgeInfo, len(d.EdgeMap)),
		Levels:       make(map[string]int, len(d.Levels)),
		EntryNodes:   append([]string{}, d.EntryNodes...),
		ExitNodes:    append([]string{}, d.ExitNodes...),
		Order:        append([]string{}, d.Order...),
	}

	for id, node := range d.Nodes {
		nodeCopy := *node
		nodeCopy.Conditions = append([]Condition{}, node.Conditions...)
		clone.Nodes[id] = &nodeCopy
	}

	for id, targets := range d.Edges {
		clone.Edges[id] = append([]string{}, targets...)
	}

	for id, sources := range d.ReverseEdges {
		clone.ReverseEdges[id] = append([]string{}, sources...)
	}

	for id, level := range d.Levels {
		clone.Levels[id] = level
	}

	// Clone EdgeMap
	for source, targetMap := range d.EdgeMap {
		clone.EdgeMap[source] = make(map[string]*EdgeInfo, len(targetMap))
		for target, info := range targetMap {
			infoCopy := *info
			clone.EdgeMap[source][target] = &infoCopy
		}
	}

	return clone
}
