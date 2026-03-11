package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// AIExecutor handles AI/LLM operations (OpenAI, Anthropic, etc.)
type AIExecutor struct {
	client        *http.Client
	defaultOpenAI string
	defaultClaude string
}

// AIConfig represents the configuration for an AI node.
type AIConfig struct {
	// Provider selection
	Provider string `json:"provider"` // openai, anthropic, custom

	// API Keys (optional if using defaults)
	APIKey string `json:"api_key"`

	// Model configuration
	Model       string  `json:"model"`
	MaxTokens   int     `json:"max_tokens"`
	Temperature float64 `json:"temperature"`
	TopP        float64 `json:"top_p"`

	// Messages
	SystemPrompt string      `json:"system_prompt"`
	Messages     []AIMessage `json:"messages"`
	Prompt       string      `json:"prompt"` // Simple single prompt

	// Streaming (for future)
	Stream bool `json:"stream"`

	// Custom endpoint
	Endpoint string `json:"endpoint"`
}

// AIMessage represents a chat message.
type AIMessage struct {
	Role    string `json:"role"` // system, user, assistant
	Content string `json:"content"`
}

// AIResponse represents the result of an AI call.
type AIResponse struct {
	Content      string  `json:"content"`
	Model        string  `json:"model"`
	Provider     string  `json:"provider"`
	FinishReason string  `json:"finish_reason"`
	Usage        AIUsage `json:"usage"`
	Timestamp    string  `json:"timestamp"`
}

// AIUsage represents token usage.
type AIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// NewAIExecutor creates a new AI executor with connection pooling.
func NewAIExecutor() *AIExecutor {
	// Configure transport with connection pooling for better performance
	transport := &http.Transport{
		MaxIdleConns:        50,                // AI calls typically to fewer hosts
		MaxIdleConnsPerHost: 10,                // Keep connections warm to OpenAI/Anthropic
		MaxConnsPerHost:     20,                // Limit concurrent connections per host
		IdleConnTimeout:     120 * time.Second, // Longer idle for AI since calls are slow
		DisableCompression:  false,
		ForceAttemptHTTP2:   true,
	}

	// Get default API keys from environment
	defaultOpenAI := os.Getenv("OPENAI_API_KEY")
	defaultClaude := os.Getenv("ANTHROPIC_API_KEY")

	return &AIExecutor{
		client: &http.Client{
			Timeout:   120 * time.Second, // AI calls can be slow
			Transport: transport,
		},
		defaultOpenAI: defaultOpenAI,
		defaultClaude: defaultClaude,
	}
}

// WithOpenAIKey sets the default OpenAI API key.
func (e *AIExecutor) WithOpenAIKey(key string) *AIExecutor {
	e.defaultOpenAI = key
	return e
}

// WithAnthropicKey sets the default Anthropic API key.
func (e *AIExecutor) WithAnthropicKey(key string) *AIExecutor {
	e.defaultClaude = key
	return e
}

func (e *AIExecutor) NodeType() string {
	return "ai"
}

func (e *AIExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting AI execution for node %s", req.NodeID),
	})

	var config AIConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse AI config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Set defaults
	if config.Provider == "" {
		config.Provider = "openai"
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = 1024
	}
	if config.Temperature == 0 {
		config.Temperature = 0.7
	}

	// Apply default API keys
	if config.APIKey == "" {
		switch config.Provider {
		case "openai":
			config.APIKey = e.defaultOpenAI
		case "anthropic":
			config.APIKey = e.defaultClaude
		}
	}

	if config.APIKey == "" {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "API key is required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Build messages
	messages := config.Messages
	if len(messages) == 0 && config.Prompt != "" {
		messages = []AIMessage{{Role: "user", Content: config.Prompt}}
	}
	if config.SystemPrompt != "" {
		messages = append([]AIMessage{{Role: "system", Content: config.SystemPrompt}}, messages...)
	}

	if len(messages) == 0 {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "messages or prompt is required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	var aiResp AIResponse
	var err error

	switch config.Provider {
	case "openai":
		aiResp, err = e.callOpenAI(ctx, config, messages, &logs)
	case "anthropic":
		aiResp, err = e.callAnthropic(ctx, config, messages, &logs)
	default:
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("unknown provider: %s", config.Provider),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if err != nil {
		errorType := ErrorTypeRetryable
		// Rate limits and server errors are retryable
		errStr := err.Error()
		if contains(errStr, "invalid_api_key") ||
			contains(errStr, "invalid_request") ||
			contains(errStr, "context_length_exceeded") {
			errorType = ErrorTypeNonRetryable
		}

		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: err.Error(),
				Type:    errorType,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	aiResp.Timestamp = time.Now().Format(time.RFC3339)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("AI response received, %d tokens used", aiResp.Usage.TotalTokens),
	})

	output, err := json.Marshal(aiResp)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal response: %v", err),
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

func (e *AIExecutor) callOpenAI(ctx context.Context, config AIConfig, messages []AIMessage, logs *[]LogEntry) (AIResponse, error) {
	var response AIResponse
	response.Provider = "openai"

	model := config.Model
	if model == "" {
		model = "gpt-4o"
	}
	response.Model = model

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Calling OpenAI API with model %s", model),
	})

	// Build request payload
	payload := map[string]interface{}{
		"model":       model,
		"messages":    messages,
		"max_tokens":  config.MaxTokens,
		"temperature": config.Temperature,
	}
	if config.TopP > 0 {
		payload["top_p"] = config.TopP
	}

	body, _ := json.Marshal(payload)

	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1/chat/completions"
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return response, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+config.APIKey)

	resp, err := e.client.Do(req)
	if err != nil {
		return response, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		var errResp struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
			} `json:"error"`
		}
		json.Unmarshal(respBody, &errResp)
		return response, fmt.Errorf("OpenAI API error: %s (%s)", errResp.Error.Message, errResp.Error.Type)
	}

	var openAIResp struct {
		ID      string `json:"id"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(respBody, &openAIResp); err != nil {
		return response, fmt.Errorf("failed to parse OpenAI response: %w", err)
	}

	if len(openAIResp.Choices) > 0 {
		response.Content = openAIResp.Choices[0].Message.Content
		response.FinishReason = openAIResp.Choices[0].FinishReason
	}

	response.Usage = AIUsage{
		PromptTokens:     openAIResp.Usage.PromptTokens,
		CompletionTokens: openAIResp.Usage.CompletionTokens,
		TotalTokens:      openAIResp.Usage.TotalTokens,
	}

	return response, nil
}

func (e *AIExecutor) callAnthropic(ctx context.Context, config AIConfig, messages []AIMessage, logs *[]LogEntry) (AIResponse, error) {
	var response AIResponse
	response.Provider = "anthropic"

	model := config.Model
	if model == "" {
		model = "claude-3-5-sonnet-20241022"
	}
	response.Model = model

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Calling Anthropic API with model %s", model),
	})

	// Extract system message and convert to Anthropic format
	var systemPrompt string
	var anthropicMessages []map[string]string

	for _, msg := range messages {
		if msg.Role == "system" {
			systemPrompt = msg.Content
		} else {
			anthropicMessages = append(anthropicMessages, map[string]string{
				"role":    msg.Role,
				"content": msg.Content,
			})
		}
	}

	// Build request payload
	payload := map[string]interface{}{
		"model":      model,
		"messages":   anthropicMessages,
		"max_tokens": config.MaxTokens,
	}
	if systemPrompt != "" {
		payload["system"] = systemPrompt
	}
	if config.Temperature > 0 {
		payload["temperature"] = config.Temperature
	}
	if config.TopP > 0 {
		payload["top_p"] = config.TopP
	}

	body, _ := json.Marshal(payload)

	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = "https://api.anthropic.com/v1/messages"
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return response, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", config.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := e.client.Do(req)
	if err != nil {
		return response, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		var errResp struct {
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		json.Unmarshal(respBody, &errResp)
		return response, fmt.Errorf("Anthropic API error: %s (%s)", errResp.Error.Message, errResp.Error.Type)
	}

	var anthropicResp struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(respBody, &anthropicResp); err != nil {
		return response, fmt.Errorf("failed to parse Anthropic response: %w", err)
	}

	// Extract text content
	for _, content := range anthropicResp.Content {
		if content.Type == "text" {
			response.Content += content.Text
		}
	}

	response.FinishReason = anthropicResp.StopReason
	response.Usage = AIUsage{
		PromptTokens:     anthropicResp.Usage.InputTokens,
		CompletionTokens: anthropicResp.Usage.OutputTokens,
		TotalTokens:      anthropicResp.Usage.InputTokens + anthropicResp.Usage.OutputTokens,
	}

	return response, nil
}
