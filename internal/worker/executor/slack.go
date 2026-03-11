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

// SlackExecutor handles Slack message sending.
type SlackExecutor struct {
	client       *http.Client
	defaultToken string
}

// SlackConfig represents the configuration for a Slack node.
type SlackConfig struct {
	// Authentication
	Token      string `json:"token"`       // Bot or User OAuth token
	WebhookURL string `json:"webhook_url"` // Incoming webhook URL (alternative to token)

	// Message destination
	Channel  string `json:"channel"`   // Channel ID or name
	ThreadTS string `json:"thread_ts"` // Reply to thread (optional)

	// Message content
	Text        string       `json:"text"`        // Plain text message
	Blocks      []SlackBlock `json:"blocks"`      // Block Kit blocks (optional)
	Attachments []Attachment `json:"attachments"` // Legacy attachments (optional)

	// Options
	AsUser      bool   `json:"as_user"`      // Post as authenticated user
	Username    string `json:"username"`     // Override bot username
	IconEmoji   string `json:"icon_emoji"`   // Override icon emoji
	IconURL     string `json:"icon_url"`     // Override icon URL
	UnfurlLinks bool   `json:"unfurl_links"` // Enable URL unfurling
	UnfurlMedia bool   `json:"unfurl_media"` // Enable media unfurling
}

// SlackBlock represents a Block Kit block.
type SlackBlock struct {
	Type     string      `json:"type"`
	Text     *TextObject `json:"text,omitempty"`
	BlockID  string      `json:"block_id,omitempty"`
	Elements []Element   `json:"elements,omitempty"`
}

// TextObject represents a text object in Block Kit.
type TextObject struct {
	Type  string `json:"type"` // plain_text or mrkdwn
	Text  string `json:"text"`
	Emoji bool   `json:"emoji,omitempty"`
}

// Element represents a block element.
type Element struct {
	Type     string      `json:"type"`
	Text     *TextObject `json:"text,omitempty"`
	ActionID string      `json:"action_id,omitempty"`
	URL      string      `json:"url,omitempty"`
	Style    string      `json:"style,omitempty"`
}

// Attachment represents a legacy attachment.
type Attachment struct {
	Color      string `json:"color,omitempty"`
	Fallback   string `json:"fallback,omitempty"`
	AuthorName string `json:"author_name,omitempty"`
	AuthorIcon string `json:"author_icon,omitempty"`
	Title      string `json:"title,omitempty"`
	TitleLink  string `json:"title_link,omitempty"`
	Text       string `json:"text,omitempty"`
	Footer     string `json:"footer,omitempty"`
	FooterIcon string `json:"footer_icon,omitempty"`
	Timestamp  int64  `json:"ts,omitempty"`
}

// SlackResponse represents the Slack API response.
type SlackResponse struct {
	OK        bool              `json:"ok"`
	Channel   string            `json:"channel,omitempty"`
	Timestamp string            `json:"ts,omitempty"`
	Message   *SlackMessageResp `json:"message,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// SlackMessageResp represents the message in the response.
type SlackMessageResp struct {
	Text      string `json:"text"`
	Username  string `json:"username"`
	BotID     string `json:"bot_id"`
	Type      string `json:"type"`
	Timestamp string `json:"ts"`
}

// NewSlackExecutor creates a new Slack executor with connection pooling.
func NewSlackExecutor() *SlackExecutor {
	// Configure transport with connection pooling for better performance
	transport := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 10, // Most calls to slack.com
		MaxConnsPerHost:     20,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		ForceAttemptHTTP2:   true,
	}

	// Get default token from environment
	defaultToken := os.Getenv("SLACK_BOT_TOKEN")

	return &SlackExecutor{
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		defaultToken: defaultToken,
	}
}

// WithDefaultToken sets the default token.
func (e *SlackExecutor) WithDefaultToken(token string) *SlackExecutor {
	e.defaultToken = token
	return e
}

func (e *SlackExecutor) NodeType() string {
	return "slack"
}

func (e *SlackExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting Slack execution for node %s", req.NodeID),
	})

	var config SlackConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse Slack config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Apply default token
	if config.Token == "" {
		config.Token = e.defaultToken
	}

	// Validate
	if config.Token == "" && config.WebhookURL == "" {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "either token or webhook_url is required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if config.Text == "" && len(config.Blocks) == 0 {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "either text or blocks is required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	var slackResp SlackResponse
	var err error

	if config.WebhookURL != "" {
		slackResp, err = e.sendWebhook(ctx, &config, &logs)
	} else {
		slackResp, err = e.sendAPI(ctx, &config, &logs)
	}

	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("Slack API error: %v", err),
				Type:    ErrorTypeRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if !slackResp.OK && slackResp.Error != "" {
		errorType := ErrorTypeRetryable
		// Non-retryable Slack errors
		nonRetryableErrors := []string{
			"channel_not_found", "not_in_channel", "is_archived",
			"msg_too_long", "no_text", "invalid_auth", "account_inactive",
			"token_revoked", "not_authed", "invalid_arguments",
		}
		for _, e := range nonRetryableErrors {
			if slackResp.Error == e {
				errorType = ErrorTypeNonRetryable
				break
			}
		}

		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("Slack error: %s", slackResp.Error),
				Type:    errorType,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Message sent successfully to %s", slackResp.Channel),
	})

	output, err := json.Marshal(slackResp)
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

func (e *SlackExecutor) sendWebhook(ctx context.Context, config *SlackConfig, logs *[]LogEntry) (SlackResponse, error) {
	payload := map[string]interface{}{
		"text": config.Text,
	}

	if len(config.Blocks) > 0 {
		payload["blocks"] = config.Blocks
	}
	if len(config.Attachments) > 0 {
		payload["attachments"] = config.Attachments
	}
	if config.Username != "" {
		payload["username"] = config.Username
	}
	if config.IconEmoji != "" {
		payload["icon_emoji"] = config.IconEmoji
	}
	if config.IconURL != "" {
		payload["icon_url"] = config.IconURL
	}
	if config.Channel != "" {
		payload["channel"] = config.Channel
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return SlackResponse{}, fmt.Errorf("failed to marshal payload: %w", err)
	}

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   "Sending message via webhook",
	})

	req, err := http.NewRequestWithContext(ctx, "POST", config.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return SlackResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return SlackResponse{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return SlackResponse{}, fmt.Errorf("failed to read response body: %w", err)
	}

	// Webhook returns "ok" as text, not JSON
	if string(respBody) == "ok" {
		return SlackResponse{OK: true}, nil
	}

	// Try to parse as JSON error
	var slackResp SlackResponse
	if err := json.Unmarshal(respBody, &slackResp); err != nil {
		return SlackResponse{OK: false, Error: string(respBody)}, nil
	}

	return slackResp, nil
}

func (e *SlackExecutor) sendAPI(ctx context.Context, config *SlackConfig, logs *[]LogEntry) (SlackResponse, error) {
	payload := map[string]interface{}{
		"channel": config.Channel,
		"text":    config.Text,
	}

	if len(config.Blocks) > 0 {
		payload["blocks"] = config.Blocks
	}
	if len(config.Attachments) > 0 {
		payload["attachments"] = config.Attachments
	}
	if config.ThreadTS != "" {
		payload["thread_ts"] = config.ThreadTS
	}
	if config.AsUser {
		payload["as_user"] = true
	}
	if config.Username != "" {
		payload["username"] = config.Username
	}
	if config.IconEmoji != "" {
		payload["icon_emoji"] = config.IconEmoji
	}
	if config.IconURL != "" {
		payload["icon_url"] = config.IconURL
	}
	payload["unfurl_links"] = config.UnfurlLinks
	payload["unfurl_media"] = config.UnfurlMedia

	body, err := json.Marshal(payload)
	if err != nil {
		return SlackResponse{}, fmt.Errorf("failed to marshal payload: %w", err)
	}

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Sending message to channel %s via API", config.Channel),
	})

	req, err := http.NewRequestWithContext(ctx, "POST", "https://slack.com/api/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return SlackResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+config.Token)

	resp, err := e.client.Do(req)
	if err != nil {
		return SlackResponse{}, err
	}
	defer resp.Body.Close()

	var slackResp SlackResponse
	if err := json.NewDecoder(resp.Body).Decode(&slackResp); err != nil {
		return SlackResponse{}, err
	}

	return slackResp, nil
}
