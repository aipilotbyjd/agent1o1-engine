package executor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPExecutorReplayFixtureHit(t *testing.T) {
	t.Parallel()

	exec := NewHTTPExecutor()
	config := HTTPConfig{
		Method: "POST",
		URL:    "https://example.com/api",
		Headers: map[string]string{
			"Authorization": "Bearer token",
		},
		Body: json.RawMessage(`{"hello":"world"}`),
	}
	configBytes, _ := json.Marshal(config)

	requestPayload, _ := json.Marshal(map[string]interface{}{
		"method":  config.Method,
		"url":     config.URL,
		"headers": map[string]string{"Authorization": "Bearer token"},
		"body":    json.RawMessage(config.Body),
	})
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(requestPayload))

	expectedOutput := json.RawMessage(`{"status_code":200,"headers":{"Content-Type":"application/json"},"body":{"ok":true}}`)

	resp, err := exec.Execute(context.Background(), &ExecuteRequest{
		NodeType: "action_http_request",
		NodeID:   "node-1",
		Config:   configBytes,
		Input:    json.RawMessage(`{}`),
		Attempt:  1,
		Deterministic: &DeterministicContext{
			Mode: "replay",
			Fixtures: []DeterministicFixture{
				{
					RequestFingerprint: fingerprint,
					Response:           expectedOutput,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("expected no execute error, got: %+v", resp.Error)
	}
	if string(resp.Output) != string(expectedOutput) {
		t.Fatalf("unexpected output: %s", string(resp.Output))
	}
	if len(resp.ConnectorAttempts) != 1 {
		t.Fatalf("expected 1 connector attempt, got %d", len(resp.ConnectorAttempts))
	}
}

func TestHTTPExecutorReplayFixtureMiss(t *testing.T) {
	t.Parallel()

	exec := NewHTTPExecutor()
	config := HTTPConfig{Method: "GET", URL: "https://example.com/miss"}
	configBytes, _ := json.Marshal(config)

	resp, err := exec.Execute(context.Background(), &ExecuteRequest{
		NodeType: "action_http_request",
		NodeID:   "node-2",
		Config:   configBytes,
		Input:    json.RawMessage(`{}`),
		Attempt:  1,
		Deterministic: &DeterministicContext{
			Mode:     "replay",
			Fixtures: []DeterministicFixture{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Error == nil {
		t.Fatalf("expected deterministic fixture miss error")
	}
	if len(resp.ConnectorAttempts) != 1 {
		t.Fatalf("expected 1 connector attempt, got %d", len(resp.ConnectorAttempts))
	}
	if resp.ConnectorAttempts[0].ErrorCode != "MISSING_REPLAY_FIXTURE" {
		t.Fatalf("unexpected error code: %s", resp.ConnectorAttempts[0].ErrorCode)
	}
}

func TestHTTPExecutorCaptureGeneratesFixture(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	exec := NewHTTPExecutor()
	config := HTTPConfig{Method: "GET", URL: ts.URL}
	configBytes, _ := json.Marshal(config)

	resp, err := exec.Execute(context.Background(), &ExecuteRequest{
		NodeType: "action_http_request",
		NodeID:   "node-3",
		Config:   configBytes,
		Input:    json.RawMessage(`{}`),
		Attempt:  1,
		Deterministic: &DeterministicContext{
			Mode: "capture",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("expected no execute error, got: %+v", resp.Error)
	}
	if len(resp.DeterministicFixtures) != 1 {
		t.Fatalf("expected 1 deterministic fixture, got %d", len(resp.DeterministicFixtures))
	}
	if len(resp.ConnectorAttempts) != 1 {
		t.Fatalf("expected 1 connector attempt, got %d", len(resp.ConnectorAttempts))
	}
}
