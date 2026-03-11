package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"time"
)

// DiscordExecutor handles Discord webhook messages.
type DiscordExecutor struct {
	client       *http.Client
	defaultToken string
}

// DiscordConfig represents the configuration for a Discord node.
type DiscordConfig struct {
	WebhookURL string `json:"webhook_url"`

	// Message content
	Content   string         `json:"content"`
	Username  string         `json:"username"`
	AvatarURL string         `json:"avatar_url"`
	TTS       bool           `json:"tts"`
	Embeds    []DiscordEmbed `json:"embeds"`
}

// DiscordEmbed represents a Discord embed.
type DiscordEmbed struct {
	Title       string              `json:"title,omitempty"`
	Description string              `json:"description,omitempty"`
	URL         string              `json:"url,omitempty"`
	Color       int                 `json:"color,omitempty"`
	Timestamp   string              `json:"timestamp,omitempty"`
	Footer      *DiscordEmbedFooter `json:"footer,omitempty"`
	Author      *DiscordEmbedAuthor `json:"author,omitempty"`
	Fields      []DiscordEmbedField `json:"fields,omitempty"`
	Thumbnail   *DiscordEmbedMedia  `json:"thumbnail,omitempty"`
	Image       *DiscordEmbedMedia  `json:"image,omitempty"`
}

// DiscordEmbedFooter represents a footer in an embed.
type DiscordEmbedFooter struct {
	Text    string `json:"text"`
	IconURL string `json:"icon_url,omitempty"`
}

// DiscordEmbedAuthor represents an author in an embed.
type DiscordEmbedAuthor struct {
	Name    string `json:"name"`
	URL     string `json:"url,omitempty"`
	IconURL string `json:"icon_url,omitempty"`
}

// DiscordEmbedField represents a field in an embed.
type DiscordEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

// DiscordEmbedMedia represents media in an embed.
type DiscordEmbedMedia struct {
	URL string `json:"url"`
}

// NewDiscordExecutor creates a new Discord executor with connection pooling.
func NewDiscordExecutor() *DiscordExecutor {
	// Configure SSRF-safe transport with connection pooling
	transport := newSSRFSafeTransport()

	// Get default token from environment
	defaultToken := os.Getenv("DISCORD_BOT_TOKEN")

	return &DiscordExecutor{
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		defaultToken: defaultToken,
	}
}

func (e *DiscordExecutor) NodeType() string {
	return "discord"
}

func (e *DiscordExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting Discord execution for node %s", req.NodeID),
	})

	var config DiscordConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse Discord config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if config.WebhookURL == "" {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "webhook_url is required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if config.Content == "" && len(config.Embeds) == 0 {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "content or embeds is required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Build payload
	payload := map[string]interface{}{
		"content": config.Content,
	}
	if config.Username != "" {
		payload["username"] = config.Username
	}
	if config.AvatarURL != "" {
		payload["avatar_url"] = config.AvatarURL
	}
	if config.TTS {
		payload["tts"] = true
	}
	if len(config.Embeds) > 0 {
		payload["embeds"] = config.Embeds
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal payload: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   "Sending Discord webhook message",
	})

	httpReq, err := http.NewRequestWithContext(ctx, "POST", config.WebhookURL, bytes.NewReader(body))
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
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("request failed: %v", err),
				Type:    ErrorTypeRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if err != nil {
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "WARN",
			Message:   fmt.Sprintf("failed to read response body: %v", err),
		})
	}

	if resp.StatusCode == 429 {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "rate limited by Discord",
				Type:    ErrorTypeRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if resp.StatusCode >= 400 {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("Discord error: %s", string(respBody)),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   "Discord message sent successfully",
	})

	output, _ := json.Marshal(map[string]interface{}{
		"success":     true,
		"status_code": resp.StatusCode,
	})

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}

// TwilioExecutor handles Twilio SMS messages.
type TwilioExecutor struct {
	client      *http.Client
	accountSid  string
	authToken   string
	defaultFrom string
}

// TwilioConfig represents the configuration for a Twilio node.
type TwilioConfig struct {
	AccountSID string `json:"account_sid"`
	AuthToken  string `json:"auth_token"`
	From       string `json:"from"`
	To         string `json:"to"`
	Body       string `json:"body"`
	MediaURL   string `json:"media_url"`
}

// NewTwilioExecutor creates a new Twilio executor with connection pooling.
func NewTwilioExecutor() *TwilioExecutor {
	// Configure SSRF-safe transport with connection pooling for Twilio API
	transport := newSSRFSafeTransport()

	// Get credentials from environment
	accountSid := os.Getenv("TWILIO_ACCOUNT_SID")
	authToken := os.Getenv("TWILIO_AUTH_TOKEN")
	defaultFrom := os.Getenv("TWILIO_PHONE_NUMBER")

	return &TwilioExecutor{
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		accountSid:  accountSid,
		authToken:   authToken,
		defaultFrom: defaultFrom,
	}
}

// WithCredentials sets default credentials.
func (e *TwilioExecutor) WithCredentials(sid, token string) *TwilioExecutor {
	e.accountSid = sid
	e.authToken = token
	return e
}

func (e *TwilioExecutor) NodeType() string {
	return "twilio"
}

func (e *TwilioExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting Twilio execution for node %s", req.NodeID),
	})

	var config TwilioConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse Twilio config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Apply defaults
	if config.AccountSID == "" {
		config.AccountSID = e.accountSid
	}
	if config.AuthToken == "" {
		config.AuthToken = e.authToken
	}

	// Validate
	if config.AccountSID == "" || config.AuthToken == "" {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "account_sid and auth_token are required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if config.From == "" || config.To == "" || config.Body == "" {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "from, to, and body are required",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Build form data
	formData := fmt.Sprintf("From=%s&To=%s&Body=%s", config.From, config.To, config.Body)
	if config.MediaURL != "" {
		formData += "&MediaUrl=" + config.MediaURL
	}

	url := fmt.Sprintf("https://api.twilio.com/2010-04-01/Accounts/%s/Messages.json", config.AccountSID)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Sending SMS to %s", config.To),
	})

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBufferString(formData))
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

	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	httpReq.SetBasicAuth(config.AccountSID, config.AuthToken)

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("request failed: %v", err),
				Type:    ErrorTypeRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "WARN",
			Message:   fmt.Sprintf("failed to read response body: %v", err),
		})
	}

	if resp.StatusCode >= 400 {
		errorType := ErrorTypeRetryable
		if resp.StatusCode == 400 || resp.StatusCode == 401 {
			errorType = ErrorTypeNonRetryable
		}
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("Twilio error: %s", string(respBody)),
				Type:    errorType,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   "SMS sent successfully",
	})

	return &ExecuteResponse{
		Output:   respBody,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}

// StorageExecutor handles file storage operations (local filesystem, S3-compatible via HTTP).
type StorageExecutor struct {
	client    *http.Client
	localRoot string
}

// StorageConfig represents the configuration for a storage node.
type StorageConfig struct {
	Provider  string `json:"provider"`  // "local" or "s3"
	Operation string `json:"operation"` // upload, download, delete, list, exists

	// Path/Key configuration
	Key       string `json:"key"`        // File path or S3 key
	DestKey   string `json:"dest_key"`   // Destination for copy/move
	LocalPath string `json:"local_path"` // Local file path for upload/download

	// Content (for direct upload)
	Content     string `json:"content"`        // Text content to upload
	ContentB64  string `json:"content_base64"` // Base64 encoded content
	ContentType string `json:"content_type"`   // MIME type

	// S3 Configuration
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Endpoint  string `json:"endpoint"` // For S3-compatible services

	// List Configuration
	Prefix    string `json:"prefix"`
	MaxKeys   int    `json:"max_keys"`
	Delimiter string `json:"delimiter"`

	// Options
	Timeout int  `json:"timeout"`
	Public  bool `json:"public"`
}

// StorageResponse represents the result of a storage operation.
type StorageResponse struct {
	Success     bool              `json:"success"`
	Operation   string            `json:"operation"`
	Key         string            `json:"key,omitempty"`
	Size        int64             `json:"size,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Content     string            `json:"content,omitempty"`
	Files       []StorageFileInfo `json:"files,omitempty"`
	Duration    string            `json:"duration"`
}

// StorageFileInfo represents file information.
type StorageFileInfo struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
}

// NewStorageExecutor creates a new storage executor.
func NewStorageExecutor() *StorageExecutor {
	localRoot := os.Getenv("LINKFLOW_STORAGE_ROOT")
	if localRoot == "" {
		localRoot = "/tmp/linkflow-storage"
	}

	transport := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     20,
		IdleConnTimeout:     90 * time.Second,
	}

	return &StorageExecutor{
		client: &http.Client{
			Timeout:   60 * time.Second,
			Transport: transport,
		},
		localRoot: localRoot,
	}
}

func (e *StorageExecutor) NodeType() string {
	return "storage"
}

func (e *StorageExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting storage execution for node %s", req.NodeID),
	})

	var config StorageConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse storage config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Default provider
	if config.Provider == "" {
		config.Provider = "local"
	}

	var response StorageResponse
	var err error

	switch config.Provider {
	case "local":
		response, err = e.executeLocal(ctx, config, &logs)
	case "s3":
		// S3 operations would require AWS SDK - for now return informative error
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "S3 storage requires AWS SDK. Add github.com/aws/aws-sdk-go-v2 to go.mod and rebuild.",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	default:
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("unsupported storage provider: %s (supported: local, s3)", config.Provider),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if err != nil {
		errorType := ErrorTypeRetryable
		errStr := err.Error()
		if contains(errStr, "not found") || contains(errStr, "no such") || contains(errStr, "permission denied") {
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

	response.Success = true
	response.Operation = config.Operation
	response.Duration = time.Since(start).String()

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Storage operation '%s' completed successfully", config.Operation),
	})

	output, _ := json.Marshal(response)

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}

func (e *StorageExecutor) executeLocal(ctx context.Context, config StorageConfig, logs *[]LogEntry) (StorageResponse, error) {
	var response StorageResponse

	// Sanitize path to prevent directory traversal
	cleanKey := filepath.Clean(config.Key)
	fullPath := filepath.Join(e.localRoot, cleanKey)

	// Verify the resolved path is still under localRoot
	absRoot, err := filepath.Abs(e.localRoot)
	if err != nil {
		return response, fmt.Errorf("failed to resolve storage root: %w", err)
	}
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return response, fmt.Errorf("failed to resolve storage path: %w", err)
	}
	if !strings.HasPrefix(absPath, absRoot+string(filepath.Separator)) && absPath != absRoot {
		return response, fmt.Errorf("path traversal detected: key %q resolves outside storage root", config.Key)
	}

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "DEBUG",
		Message:   fmt.Sprintf("Local storage %s: %s", config.Operation, fullPath),
	})

	switch config.Operation {
	case "upload", "write":
		return e.localWrite(fullPath, config, logs)
	case "download", "read":
		return e.localRead(fullPath, config, logs)
	case "delete":
		return e.localDelete(fullPath, logs)
	case "list":
		return e.localList(fullPath, logs)
	case "exists":
		return e.localExists(fullPath, logs)
	default:
		return response, fmt.Errorf("unsupported local operation: %s", config.Operation)
	}
}

func (e *StorageExecutor) localWrite(fullPath string, config StorageConfig, logs *[]LogEntry) (StorageResponse, error) {
	var response StorageResponse

	// Create parent directories
	dir := fullPath[:len(fullPath)-len(config.Key)+len(config.Key[:max(0, lastIndex(config.Key, '/'))])]
	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return response, fmt.Errorf("failed to create directory: %w", err)
		}
	}

	var data []byte
	if config.Content != "" {
		data = []byte(config.Content)
	} else if config.ContentB64 != "" {
		var err error
		data, err = decodeBase64(config.ContentB64)
		if err != nil {
			return response, fmt.Errorf("invalid base64 content: %w", err)
		}
	} else if config.LocalPath != "" {
		var err error
		data, err = os.ReadFile(config.LocalPath)
		if err != nil {
			return response, fmt.Errorf("failed to read source file: %w", err)
		}
	} else {
		return response, fmt.Errorf("content, content_base64, or local_path is required for write")
	}

	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return response, fmt.Errorf("failed to write file: %w", err)
	}

	response.Key = config.Key
	response.Size = int64(len(data))

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Wrote %d bytes to %s", len(data), fullPath),
	})

	return response, nil
}

func (e *StorageExecutor) localRead(fullPath string, config StorageConfig, logs *[]LogEntry) (StorageResponse, error) {
	var response StorageResponse

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return response, fmt.Errorf("failed to read file: %w", err)
	}

	if config.LocalPath != "" {
		if err := os.WriteFile(config.LocalPath, data, 0644); err != nil {
			return response, fmt.Errorf("failed to write to destination: %w", err)
		}
	} else {
		response.Content = string(data)
	}

	response.Key = config.Key
	response.Size = int64(len(data))

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Read %d bytes from %s", len(data), fullPath),
	})

	return response, nil
}

func (e *StorageExecutor) localDelete(fullPath string, logs *[]LogEntry) (StorageResponse, error) {
	var response StorageResponse

	if err := os.Remove(fullPath); err != nil {
		return response, fmt.Errorf("failed to delete file: %w", err)
	}

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Deleted %s", fullPath),
	})

	return response, nil
}

func (e *StorageExecutor) localList(dirPath string, logs *[]LogEntry) (StorageResponse, error) {
	var response StorageResponse

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return response, fmt.Errorf("failed to list directory: %w", err)
	}

	response.Files = make([]StorageFileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		response.Files = append(response.Files, StorageFileInfo{
			Key:          entry.Name(),
			Size:         info.Size(),
			LastModified: info.ModTime().Format(time.RFC3339),
		})
	}

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Listed %d files in %s", len(response.Files), dirPath),
	})

	return response, nil
}

func (e *StorageExecutor) localExists(fullPath string, logs *[]LogEntry) (StorageResponse, error) {
	var response StorageResponse

	info, err := os.Stat(fullPath)
	if os.IsNotExist(err) {
		response.Success = false
		return response, nil
	}
	if err != nil {
		return response, fmt.Errorf("failed to stat file: %w", err)
	}

	response.Success = true
	response.Size = info.Size()

	return response, nil
}

func lastIndex(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
