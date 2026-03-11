package frontend

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// WorkflowCache fetches and caches workflow definitions from the Laravel API.
// Instead of embedding full workflow definitions in every Redis message,
// the engine fetches them on first use and caches by version hash.
type WorkflowCache struct {
	apiBaseURL string
	secretKey  string
	httpClient *http.Client
	logger     *slog.Logger
	cache      sync.Map // versionHash -> *cachedWorkflow
}

type cachedWorkflow struct {
	data      *WorkflowDefinition
	expiresAt time.Time
}

// WorkflowDefinition holds the workflow graph definition.
type WorkflowDefinition struct {
	WorkflowID  int                      `json:"workflow_id"`
	VersionID   int                      `json:"version_id"`
	VersionHash string                   `json:"version_hash"`
	Nodes       []map[string]interface{} `json:"nodes"`
	Edges       []map[string]interface{} `json:"edges"`
	Settings    map[string]interface{}   `json:"settings"`
}

// WorkflowCacheConfig holds configuration.
type WorkflowCacheConfig struct {
	APIBaseURL string
	SecretKey  string
	CacheTTL   time.Duration
	Timeout    time.Duration
}

// DefaultWorkflowCacheConfig returns sane defaults.
func DefaultWorkflowCacheConfig() WorkflowCacheConfig {
	return WorkflowCacheConfig{
		APIBaseURL: "http://linkflow-api:8000",
		SecretKey:  "",
		CacheTTL:   30 * time.Minute, // Workflow versions are immutable, so cache longer
		Timeout:    15 * time.Second,
	}
}

// NewWorkflowCache creates a new WorkflowCache.
func NewWorkflowCache(cfg WorkflowCacheConfig, logger *slog.Logger) *WorkflowCache {
	return &WorkflowCache{
		apiBaseURL: cfg.APIBaseURL,
		secretKey:  cfg.SecretKey,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        20,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     60 * time.Second,
			},
		},
		logger: logger,
	}
}

// Get retrieves a workflow definition, returning cached data if available.
func (c *WorkflowCache) Get(ctx context.Context, workflowID int, versionHash string) (*WorkflowDefinition, error) {
	// Check cache for immutable version
	if versionHash != "" {
		if cached, ok := c.cache.Load(versionHash); ok {
			cw := cached.(*cachedWorkflow)
			if time.Now().Before(cw.expiresAt) {
				c.logger.Debug("workflow cache hit",
					slog.Int("workflow_id", workflowID),
					slog.String("version_hash", versionHash),
				)
				return cw.data, nil
			}
			c.cache.Delete(versionHash)
		}
	}

	// Fetch from API
	url := fmt.Sprintf("%s/api/v1/internal/workflows/definition", c.apiBaseURL)
	
	reqBody := map[string]interface{}{
		"workflow_id":  workflowID,
		"version_hash": versionHash,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Sign the request
	timestamp := time.Now().UTC().Format(time.RFC3339)
	req.Header.Set("X-LinkFlow-Timestamp", timestamp)
	if c.secretKey != "" {
		payload := fmt.Sprintf("%s.%s", timestamp, string(bodyBytes))
		signature := c.sign(payload)
		req.Header.Set("X-LinkFlow-Signature", signature)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("workflow request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("workflow API returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Success  bool                `json:"success"`
		Workflow *WorkflowDefinition `json:"workflow"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode workflow response: %w", err)
	}

	if !result.Success || result.Workflow == nil {
		return nil, fmt.Errorf("workflow %d not found", workflowID)
	}

	// Cache by version hash (immutable)
	hash := result.Workflow.VersionHash
	if hash == "" {
		hash = versionHash
	}
	if hash != "" {
		c.cache.Store(hash, &cachedWorkflow{
			data:      result.Workflow,
			expiresAt: time.Now().Add(30 * time.Minute),
		})
	}

	c.logger.Info("workflow fetched from API",
		slog.Int("workflow_id", workflowID),
		slog.String("version_hash", hash),
	)

	return result.Workflow, nil
}

func (c *WorkflowCache) sign(payload string) string {
	h := hmac.New(sha256.New, []byte(c.secretKey))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}

// Stats returns cache statistics.
func (c *WorkflowCache) Stats() map[string]interface{} {
	count := 0
	c.cache.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	return map[string]interface{}{
		"cached_workflows": count,
	}
}

// Invalidate removes a specific cached workflow by hash.
func (c *WorkflowCache) Invalidate(versionHash string) {
	c.cache.Delete(versionHash)
}
