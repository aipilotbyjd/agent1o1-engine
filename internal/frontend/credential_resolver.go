package frontend

import (
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

// CredentialResolver fetches credentials from the Laravel API on-demand
// instead of passing them through Redis. This keeps secrets out of the
// message broker and adds an audit trail.
type CredentialResolver struct {
	apiBaseURL string
	secretKey  string
	httpClient *http.Client
	logger     *slog.Logger
	cache      sync.Map // nodeID -> *cachedCredential
}

type cachedCredential struct {
	data      *ResolvedCredential
	expiresAt time.Time
}

// ResolvedCredential represents a credential fetched from the API.
type ResolvedCredential struct {
	ID   int                    `json:"id"`
	Type string                 `json:"type"`
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

// CredentialResolverConfig holds configuration.
type CredentialResolverConfig struct {
	APIBaseURL string
	SecretKey  string
	CacheTTL   time.Duration
	Timeout    time.Duration
}

// DefaultCredentialResolverConfig returns sane defaults.
func DefaultCredentialResolverConfig() CredentialResolverConfig {
	return CredentialResolverConfig{
		APIBaseURL: "http://linkflow-api:8000",
		SecretKey:  "",
		CacheTTL:   5 * time.Minute,
		Timeout:    10 * time.Second,
	}
}

// NewCredentialResolver creates a new CredentialResolver.
func NewCredentialResolver(cfg CredentialResolverConfig, logger *slog.Logger) *CredentialResolver {
	return &CredentialResolver{
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

// Resolve fetches a credential from the Laravel API by execution's nodeID.
// It caches results for a short TTL to avoid hammering the API.
func (r *CredentialResolver) Resolve(ctx context.Context, executionID int, nodeID string, callbackToken string) (*ResolvedCredential, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("%d:%s", executionID, nodeID)
	if cached, ok := r.cache.Load(cacheKey); ok {
		cc := cached.(*cachedCredential)
		if time.Now().Before(cc.expiresAt) {
			r.logger.Debug("credential cache hit",
				slog.String("node_id", nodeID),
				slog.Int("execution_id", executionID),
			)
			return cc.data, nil
		}
		r.cache.Delete(cacheKey)
	}

	url := fmt.Sprintf("%s/api/v1/internal/credentials/%d/%s", r.apiBaseURL, executionID, nodeID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create credential request: %w", err)
	}

	// Sign the request
	timestamp := time.Now().UTC().Format(time.RFC3339)
	req.Header.Set("X-LinkFlow-Timestamp", timestamp)
	req.Header.Set("X-LinkFlow-Callback-Token", callbackToken)

	if r.secretKey != "" {
		payload := fmt.Sprintf("%s.%d.%s", timestamp, executionID, nodeID)
		signature := r.sign(payload)
		req.Header.Set("X-LinkFlow-Signature", signature)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("credential request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("credential API returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Success    bool                `json:"success"`
		Credential *ResolvedCredential `json:"credential"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode credential response: %w", err)
	}

	if !result.Success || result.Credential == nil {
		return nil, fmt.Errorf("credential not found for node %s", nodeID)
	}

	// Cache the result
	r.cache.Store(cacheKey, &cachedCredential{
		data:      result.Credential,
		expiresAt: time.Now().Add(5 * time.Minute),
	})

	r.logger.Info("credential resolved from API",
		slog.String("node_id", nodeID),
		slog.Int("execution_id", executionID),
		slog.String("type", result.Credential.Type),
	)

	return result.Credential, nil
}

func (r *CredentialResolver) sign(payload string) string {
	h := hmac.New(sha256.New, []byte(r.secretKey))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}

// InvalidateCache removes a specific cached credential.
func (r *CredentialResolver) InvalidateCache(executionID int, nodeID string) {
	cacheKey := fmt.Sprintf("%d:%s", executionID, nodeID)
	r.cache.Delete(cacheKey)
}

// ClearCache removes all cached credentials.
func (r *CredentialResolver) ClearCache() {
	r.cache.Range(func(key, value interface{}) bool {
		r.cache.Delete(key)
		return true
	})
}
