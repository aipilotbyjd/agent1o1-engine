package matching

import (
	"testing"
)

func TestGenerateTaskID(t *testing.T) {
	tests := []struct {
		name             string
		namespace        string
		workflowID       string
		runID            string
		taskType         int32
		scheduledEventID int64
		expected         string
	}{
		{
			name:             "basic task ID",
			namespace:        "default",
			workflowID:       "workflow-1",
			runID:            "run-1",
			taskType:         1,
			scheduledEventID: 100,
			expected:         "default:workflow-1:run-1:1:100",
		},
		{
			name:             "empty namespace",
			namespace:        "",
			workflowID:       "workflow-2",
			runID:            "run-2",
			taskType:         2,
			scheduledEventID: 200,
			expected:         ":workflow-2:run-2:2:200",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateTaskID(tt.namespace, tt.workflowID, tt.runID, tt.taskType, tt.scheduledEventID)
			if got != tt.expected {
				t.Errorf("generateTaskID() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestGenerateTaskIDDeterministic(t *testing.T) {
	// Same inputs should produce same output
	id1 := generateTaskID("ns", "wf", "run", 1, 10)
	id2 := generateTaskID("ns", "wf", "run", 1, 10)

	if id1 != id2 {
		t.Errorf("generateTaskID should be deterministic: got %q and %q", id1, id2)
	}
}

func TestGenerateTaskIDUniqueness(t *testing.T) {
	// Different inputs should produce different outputs
	id1 := generateTaskID("ns", "wf", "run", 1, 10)
	id2 := generateTaskID("ns", "wf", "run", 1, 11) // different event ID

	if id1 == id2 {
		t.Errorf("generateTaskID should produce unique IDs for different inputs")
	}
}

func TestGenerateSecureToken(t *testing.T) {
	token1, err := generateSecureToken()
	if err != nil {
		t.Fatalf("generateSecureToken() error = %v", err)
	}

	if len(token1) == 0 {
		t.Error("generateSecureToken() returned empty token")
	}

	// Token should be hex-encoded (64 chars for 32 bytes)
	if len(token1) != 64 {
		t.Errorf("generateSecureToken() token length = %d, want 64", len(token1))
	}

	// Tokens should be unique
	token2, err := generateSecureToken()
	if err != nil {
		t.Fatalf("generateSecureToken() error = %v", err)
	}

	if string(token1) == string(token2) {
		t.Error("generateSecureToken() should produce unique tokens")
	}
}
