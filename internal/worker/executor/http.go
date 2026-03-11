package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"time"
)

type HTTPExecutor struct {
	client *http.Client
}

type HTTPConfig struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
	Timeout int               `json:"timeout"`
}

type HTTPResponse struct {
	StatusCode int               `json:"status_code"`
	Headers    map[string]string `json:"headers"`
	Body       json.RawMessage   `json:"body"`
}

// NewHTTPExecutor creates a new HTTP executor with connection pooling.
func NewHTTPExecutor() *HTTPExecutor {
	// Configure transport with connection pooling for better performance
	transport := &http.Transport{
		MaxIdleConns:        100,              // Max idle connections across all hosts
		MaxIdleConnsPerHost: 20,               // Max idle connections per host
		MaxConnsPerHost:     50,               // Max total connections per host
		IdleConnTimeout:     90 * time.Second, // How long idle connections stay in pool
		DisableCompression:  false,            // Enable compression
		ForceAttemptHTTP2:   true,             // Prefer HTTP/2 when available
	}

	return &HTTPExecutor{
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}
}

func (e *HTTPExecutor) NodeType() string {
	return "action_http_request"
}

func (e *HTTPExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)
	connectorAttempts := make([]ConnectorAttempt, 0, 1)
	fixtures := make([]DeterministicFixture, 0, 1)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting HTTP execution for node %s", req.NodeID),
	})

	var config HTTPConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse HTTP config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	if config.Method == "" {
		config.Method = "GET"
	}

	if config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(config.Timeout)*time.Second)
		defer cancel()
	}

	parsedURL, urlErr := url.Parse(config.URL)
	if urlErr != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "only http and https URLs are allowed",
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	if isBlockedAddress(parsedURL.Hostname()) {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "requests to private/internal networks are not allowed",
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	requestBytes, _ := json.Marshal(map[string]interface{}{
		"method":  config.Method,
		"url":     config.URL,
		"headers": canonicalHeaders(config.Headers),
		"body":    json.RawMessage(config.Body),
	})
	requestFingerprint := fmt.Sprintf("%x", sha256.Sum256(requestBytes))

	if req.Deterministic != nil && req.Deterministic.Mode == "replay" {
		for _, fixture := range req.Deterministic.Fixtures {
			if fixture.RequestFingerprint != requestFingerprint {
				continue
			}

			connectorAttempts = append(connectorAttempts, ConnectorAttempt{
				NodeID:             req.NodeID,
				ConnectorKey:       "action_http_request",
				ConnectorOperation: "request",
				Provider:           "http",
				AttemptNo:          req.Attempt,
				IsRetry:            req.Attempt > 1,
				Status:             "success",
				DurationMS:         0,
				RequestFingerprint: requestFingerprint,
				HappenedAt:         time.Now().UTC(),
				Meta: map[string]interface{}{
					"replay_mode": true,
					"fixture_hit": true,
				},
			})

			return &ExecuteResponse{
				Output:                fixture.Response,
				ConnectorAttempts:     connectorAttempts,
				DeterministicFixtures: fixtures,
				Logs:                  logs,
				Duration:              time.Since(start),
			}, nil
		}

		connectorAttempts = append(connectorAttempts, ConnectorAttempt{
			NodeID:             req.NodeID,
			ConnectorKey:       "action_http_request",
			ConnectorOperation: "request",
			Provider:           "http",
			AttemptNo:          req.Attempt,
			IsRetry:            req.Attempt > 1,
			Status:             "client_error",
			ErrorCode:          "MISSING_REPLAY_FIXTURE",
			ErrorMessage:       "no deterministic fixture found for HTTP request fingerprint",
			RequestFingerprint: requestFingerprint,
			HappenedAt:         time.Now().UTC(),
			Meta: map[string]interface{}{
				"replay_mode": true,
				"fixture_hit": false,
			},
		})

		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "missing deterministic replay fixture for HTTP request",
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	var bodyReader io.Reader
	if len(config.Body) > 0 {
		bodyReader = bytes.NewReader(config.Body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, config.Method, config.URL, bodyReader)
	if err != nil {
		connectorAttempts = append(connectorAttempts, ConnectorAttempt{
			NodeID:             req.NodeID,
			ConnectorKey:       "action_http_request",
			ConnectorOperation: "request",
			Provider:           "http",
			AttemptNo:          req.Attempt,
			IsRetry:            req.Attempt > 1,
			Status:             "client_error",
			ErrorCode:          "HTTP_REQUEST_BUILD_FAILED",
			ErrorMessage:       err.Error(),
			RequestFingerprint: requestFingerprint,
			HappenedAt:         time.Now().UTC(),
		})
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to create HTTP request: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	for key, value := range config.Headers {
		httpReq.Header.Set(key, value)
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Sending %s request to %s", config.Method, config.URL),
	})

	resp, err := e.client.Do(httpReq)
	if err != nil {
		errorType := ErrorTypeRetryable
		attemptStatus := "network_error"
		errorCode := "HTTP_REQUEST_FAILED"
		if ctx.Err() == context.DeadlineExceeded {
			errorType = ErrorTypeTimeout
			attemptStatus = "timeout"
			errorCode = "HTTP_TIMEOUT"
		}

		connectorAttempts = append(connectorAttempts, ConnectorAttempt{
			NodeID:             req.NodeID,
			ConnectorKey:       "action_http_request",
			ConnectorOperation: "request",
			Provider:           "http",
			AttemptNo:          req.Attempt,
			IsRetry:            req.Attempt > 1,
			Status:             attemptStatus,
			ErrorCode:          errorCode,
			ErrorMessage:       err.Error(),
			RequestFingerprint: requestFingerprint,
			HappenedAt:         time.Now().UTC(),
		})

		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("HTTP request failed: %v", err),
				Type:    errorType,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}
	defer resp.Body.Close()

	const maxResponseBody = 10 * 1024 * 1024 // 10MB
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		connectorAttempts = append(connectorAttempts, ConnectorAttempt{
			NodeID:             req.NodeID,
			ConnectorKey:       "action_http_request",
			ConnectorOperation: "request",
			Provider:           "http",
			AttemptNo:          req.Attempt,
			IsRetry:            req.Attempt > 1,
			Status:             "network_error",
			StatusCode:         int32(resp.StatusCode),
			ErrorCode:          "HTTP_READ_BODY_FAILED",
			ErrorMessage:       err.Error(),
			RequestFingerprint: requestFingerprint,
			HappenedAt:         time.Now().UTC(),
		})
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to read response body: %v", err),
				Type:    ErrorTypeRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	if int64(len(body)) > maxResponseBody {
		connectorAttempts = append(connectorAttempts, ConnectorAttempt{
			NodeID:             req.NodeID,
			ConnectorKey:       "action_http_request",
			ConnectorOperation: "request",
			Provider:           "http",
			AttemptNo:          req.Attempt,
			IsRetry:            req.Attempt > 1,
			Status:             "client_error",
			StatusCode:         int32(resp.StatusCode),
			ErrorCode:          "HTTP_RESPONSE_TOO_LARGE",
			ErrorMessage:       fmt.Sprintf("response body exceeds %d bytes limit", maxResponseBody),
			RequestFingerprint: requestFingerprint,
			HappenedAt:         time.Now().UTC(),
		})
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("response body exceeds %d bytes limit", maxResponseBody),
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Received response with status %d", resp.StatusCode),
	})

	headers := make(map[string]string)
	for key := range resp.Header {
		headers[key] = resp.Header.Get(key)
	}

	// Handle body - ensure it's valid JSON for marshaling
	var jsonBody json.RawMessage
	if len(body) == 0 {
		// Empty body - use empty object
		jsonBody = json.RawMessage(`{}`)
	} else if json.Valid(body) {
		// Body is valid JSON - use as-is
		jsonBody = body
	} else {
		// Body is not valid JSON (e.g., HTML, plain text) - wrap in a JSON object
		wrapped := map[string]string{"body": string(body)}
		jsonBody, _ = json.Marshal(wrapped)
	}

	httpResp := HTTPResponse{
		StatusCode: resp.StatusCode,
		Headers:    headers,
		Body:       jsonBody,
	}

	output, err := json.Marshal(httpResp)
	if err != nil {
		connectorAttempts = append(connectorAttempts, ConnectorAttempt{
			NodeID:             req.NodeID,
			ConnectorKey:       "action_http_request",
			ConnectorOperation: "request",
			Provider:           "http",
			AttemptNo:          req.Attempt,
			IsRetry:            req.Attempt > 1,
			Status:             "client_error",
			StatusCode:         int32(resp.StatusCode),
			ErrorCode:          "HTTP_RESPONSE_MARSHAL_FAILED",
			ErrorMessage:       err.Error(),
			RequestFingerprint: requestFingerprint,
			HappenedAt:         time.Now().UTC(),
		})
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal response: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	fixtures = append(fixtures, DeterministicFixture{
		RequestFingerprint: requestFingerprint,
		NodeID:             req.NodeID,
		NodeType:           req.NodeType,
		Request:            requestBytes,
		Response:           output,
	})

	attemptStatus := "success"
	if resp.StatusCode >= 500 {
		attemptStatus = "server_error"
	} else if resp.StatusCode >= 400 {
		attemptStatus = "client_error"
	}
	connectorAttempts = append(connectorAttempts, ConnectorAttempt{
		NodeID:             req.NodeID,
		ConnectorKey:       "action_http_request",
		ConnectorOperation: "request",
		Provider:           "http",
		AttemptNo:          req.Attempt,
		IsRetry:            req.Attempt > 1,
		Status:             attemptStatus,
		StatusCode:         int32(resp.StatusCode),
		DurationMS:         time.Since(start).Milliseconds(),
		RequestFingerprint: requestFingerprint,
		HappenedAt:         time.Now().UTC(),
	})

	if resp.StatusCode >= 500 {
		return &ExecuteResponse{
			Output: output,
			Error: &ExecutionError{
				Message: fmt.Sprintf("server error: status %d", resp.StatusCode),
				Type:    ErrorTypeRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	if resp.StatusCode >= 400 {
		return &ExecuteResponse{
			Output: output,
			Error: &ExecutionError{
				Message: fmt.Sprintf("client error: status %d", resp.StatusCode),
				Type:    ErrorTypeNonRetryable,
			},
			ConnectorAttempts:     connectorAttempts,
			DeterministicFixtures: fixtures,
			Logs:                  logs,
			Duration:              time.Since(start),
		}, nil
	}

	return &ExecuteResponse{
		Output:                output,
		ConnectorAttempts:     connectorAttempts,
		DeterministicFixtures: fixtures,
		Logs:                  logs,
		Duration:              time.Since(start),
	}, nil
}

func canonicalHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return map[string]string{}
	}

	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	canonical := make(map[string]string, len(headers))
	for _, key := range keys {
		canonical[key] = headers[key]
	}

	return canonical
}

// isBlockedAddress checks if a resolved IP is in a private/reserved range (SSRF protection).
func isBlockedAddress(host string) bool {
	ips, err := net.LookupHost(host)
	if err != nil {
		return false // let the HTTP request fail naturally
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return true
		}
		// Block metadata endpoints (169.254.169.254)
		if ip.Equal(net.ParseIP("169.254.169.254")) {
			return true
		}
	}
	return false
}
