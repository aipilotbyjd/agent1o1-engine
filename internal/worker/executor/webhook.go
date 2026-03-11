package executor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// WebhookExecutor handles webhook calls to external services.
type WebhookExecutor struct {
	client *http.Client
}

// WebhookConfig represents the configuration for a webhook node.
type WebhookConfig struct {
	// Request configuration
	URL      string            `json:"url"`
	Method   string            `json:"method"`
	Headers  map[string]string `json:"headers"`
	Query    map[string]string `json:"query"`
	Body     json.RawMessage   `json:"body"`
	FormData map[string]string `json:"form_data"`

	// Authentication
	AuthType     string `json:"auth_type"`      // none, basic, bearer, api_key, hmac
	Username     string `json:"username"`       // for basic auth
	Password     string `json:"password"`       // for basic auth
	Token        string `json:"token"`          // for bearer auth
	APIKey       string `json:"api_key"`        // for api_key auth
	APIKeyHeader string `json:"api_key_header"` // header name for api_key
	HMACSecret   string `json:"hmac_secret"`    // for hmac signing
	HMACHeader   string `json:"hmac_header"`    // header name for hmac signature

	// Options
	Timeout            int   `json:"timeout"` // in seconds
	FollowRedirects    bool  `json:"follow_redirects"`
	MaxRedirects       int   `json:"max_redirects"`
	RetryStatusCodes   []int `json:"retry_status_codes"`
	SuccessStatusCodes []int `json:"success_status_codes"`

	// Response handling
	ResponseType string `json:"response_type"` // json, text, binary
}

// WebhookResponse represents the result of a webhook call.
type WebhookResponse struct {
	StatusCode    int               `json:"status_code"`
	Status        string            `json:"status"`
	Headers       map[string]string `json:"headers"`
	Body          json.RawMessage   `json:"body,omitempty"`
	TextBody      string            `json:"text_body,omitempty"`
	ContentType   string            `json:"content_type"`
	ContentLength int64             `json:"content_length"`
	Duration      string            `json:"duration"`
}

// NewWebhookExecutor creates a new webhook executor with connection pooling.
func NewWebhookExecutor() *WebhookExecutor {
	// Configure transport with connection pooling for better performance
	transport := &http.Transport{
		MaxIdleConns:        100, // Webhooks can hit many different hosts
		MaxIdleConnsPerHost: 10,  // Keep some connections warm per host
		MaxConnsPerHost:     20,  // Limit concurrent connections
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		ForceAttemptHTTP2:   true,
	}

	return &WebhookExecutor{
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
			CheckRedirect: func(_ *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("stopped after 10 redirects")
				}
				return nil
			},
		},
	}
}

func (e *WebhookExecutor) NodeType() string {
	return "trigger_webhook"
}

func (e *WebhookExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting webhook execution for node %s", req.NodeID),
	})

	var config WebhookConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse webhook config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Validate URL
	if config.URL == "" {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "URL is required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Set defaults
	if config.Method == "" {
		config.Method = "POST"
	}
	if config.Timeout == 0 {
		config.Timeout = 30
	}
	if config.SuccessStatusCodes == nil {
		config.SuccessStatusCodes = []int{200, 201, 202, 204}
	}
	if config.RetryStatusCodes == nil {
		config.RetryStatusCodes = []int{429, 500, 502, 503, 504}
	}

	// Build URL with query parameters
	finalURL := config.URL
	if len(config.Query) > 0 {
		u, err := url.Parse(config.URL)
		if err == nil {
			q := u.Query()
			for k, v := range config.Query {
				q.Set(k, v)
			}
			u.RawQuery = q.Encode()
			finalURL = u.String()
		}
	}

	// Build request body
	var bodyReader io.Reader
	var contentType string

	if len(config.FormData) > 0 {
		// Form data
		formValues := url.Values{}
		for k, v := range config.FormData {
			formValues.Set(k, v)
		}
		bodyReader = strings.NewReader(formValues.Encode())
		contentType = "application/x-www-form-urlencoded"
	} else if len(config.Body) > 0 {
		bodyReader = bytes.NewReader(config.Body)
		contentType = "application/json"
	}

	// Create context with timeout
	timeout := time.Duration(config.Timeout) * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Create request
	httpReq, err := http.NewRequestWithContext(ctx, config.Method, finalURL, bodyReader)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to create request: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Set headers
	if contentType != "" {
		httpReq.Header.Set("Content-Type", contentType)
	}
	for k, v := range config.Headers {
		httpReq.Header.Set(k, v)
	}

	// Apply authentication
	e.applyAuth(httpReq, &config, &logs)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Sending %s request to %s", config.Method, finalURL),
	})

	// Execute request
	resp, err := e.client.Do(httpReq)
	if err != nil {
		errorType := ErrorTypeRetryable
		if ctx.Err() == context.DeadlineExceeded {
			errorType = ErrorTypeTimeout
		}
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("request failed: %v", err),
				Type:    errorType,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to read response: %v", err),
				Type:    ErrorTypeRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Build response
	webhookResp := WebhookResponse{
		StatusCode:    resp.StatusCode,
		Status:        resp.Status,
		Headers:       make(map[string]string),
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: resp.ContentLength,
		Duration:      time.Since(start).String(),
	}

	for k := range resp.Header {
		webhookResp.Headers[k] = resp.Header.Get(k)
	}

	// Handle response body based on type
	responseType := config.ResponseType
	if responseType == "" {
		if strings.Contains(webhookResp.ContentType, "application/json") {
			responseType = "json"
		} else {
			responseType = "text"
		}
	}

	switch responseType {
	case "json":
		// Try to parse as JSON
		var jsonBody interface{}
		if err := json.Unmarshal(respBody, &jsonBody); err == nil {
			webhookResp.Body = respBody
		} else {
			webhookResp.TextBody = string(respBody)
		}
	case "text":
		webhookResp.TextBody = string(respBody)
	default:
		webhookResp.Body = respBody
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Received response: %d %s", resp.StatusCode, resp.Status),
	})

	// Check if status code requires retry
	for _, retryCod := range config.RetryStatusCodes {
		if resp.StatusCode == retryCod {
			return &ExecuteResponse{
				Error: &ExecutionError{
					Message: fmt.Sprintf("received retryable status code: %d", resp.StatusCode),
					Type:    ErrorTypeRetryable,
				},
				Logs:     logs,
				Duration: time.Since(start),
			}, nil
		}
	}

	// Check if status code is success
	isSuccess := false
	for _, successCode := range config.SuccessStatusCodes {
		if resp.StatusCode == successCode {
			isSuccess = true
			break
		}
	}

	output, _ := json.Marshal(webhookResp)

	if !isSuccess {
		return &ExecuteResponse{
			Output: output,
			Error: &ExecutionError{
				Message: fmt.Sprintf("request failed with status: %d", resp.StatusCode),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}

func (e *WebhookExecutor) applyAuth(req *http.Request, config *WebhookConfig, logs *[]LogEntry) {
	switch config.AuthType {
	case "basic":
		req.SetBasicAuth(config.Username, config.Password)
		*logs = append(*logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "DEBUG",
			Message:   "Applied basic authentication",
		})

	case "bearer":
		req.Header.Set("Authorization", "Bearer "+config.Token)
		*logs = append(*logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "DEBUG",
			Message:   "Applied bearer token authentication",
		})

	case "api_key":
		header := config.APIKeyHeader
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, config.APIKey)
		*logs = append(*logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "DEBUG",
			Message:   fmt.Sprintf("Applied API key authentication via %s header", header),
		})

	case "hmac":
		// Read body for signing
		var bodyBytes []byte
		if req.Body != nil {
			var err error
			bodyBytes, err = io.ReadAll(req.Body)
			if err != nil {
				*logs = append(*logs, LogEntry{
					Timestamp: time.Now(),
					Level:     "WARN",
					Message:   fmt.Sprintf("failed to read request body for HMAC: %v", err),
				})
			}
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		// Create HMAC signature
		mac := hmac.New(sha256.New, []byte(config.HMACSecret))
		mac.Write(bodyBytes)
		signature := hex.EncodeToString(mac.Sum(nil))

		header := config.HMACHeader
		if header == "" {
			header = "X-Webhook-Signature"
		}
		req.Header.Set(header, "sha256="+signature)
		*logs = append(*logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "DEBUG",
			Message:   fmt.Sprintf("Applied HMAC signature via %s header", header),
		})
	}
}
