package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linkflow/engine/internal/crypto"
)

var (
	ErrCredentialNotFound = errors.New("credential not found")
	ErrDecryptionFailed   = errors.New("credential decryption failed")
)

// Credential represents a decrypted credential.
type Credential struct {
	ID        int64
	Name      string
	Type      string
	Data      map[string]string
	ExpiresAt *time.Time
}

// CredentialResolver resolves and decrypts credentials.
type CredentialResolver struct {
	pool             *pgxpool.Pool
	encryptor        *crypto.Encryptor
	laravelEncryptor *crypto.LaravelEncryptor
	cache            *credentialCache

	cacheTTL time.Duration
}

// CredentialConfig holds resolver configuration.
type CredentialConfig struct {
	MasterKey    string
	LaravelAppKey string // Laravel APP_KEY for decrypting Laravel-encrypted credentials
	CacheTTL     time.Duration
}

// NewCredentialResolver creates a new credential resolver.
func NewCredentialResolver(pool *pgxpool.Pool, config CredentialConfig) (*CredentialResolver, error) {
	encryptor, err := crypto.NewEncryptorFromString(config.MasterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create encryptor: %w", err)
	}

	var laravelEnc *crypto.LaravelEncryptor
	if config.LaravelAppKey != "" {
		laravelEnc, err = crypto.NewLaravelEncryptor(config.LaravelAppKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Laravel encryptor: %w", err)
		}
	}

	cacheTTL := config.CacheTTL
	if cacheTTL == 0 {
		cacheTTL = 5 * time.Minute
	}

	return &CredentialResolver{
		pool:             pool,
		encryptor:        encryptor,
		laravelEncryptor: laravelEnc,
		cache:            newCredentialCache(cacheTTL),
		cacheTTL:         cacheTTL,
	}, nil
}

// Resolve resolves credentials by IDs.
func (r *CredentialResolver) Resolve(
	ctx context.Context,
	namespaceID string,
	credentialIDs []string,
) (map[string]*Credential, error) {
	result := make(map[string]*Credential)
	var missing []string

	// Check cache first
	for _, id := range credentialIDs {
		if cred := r.cache.get(namespaceID, id); cred != nil {
			result[id] = cred
		} else {
			missing = append(missing, id)
		}
	}

	if len(missing) == 0 {
		return result, nil
	}

	// Fetch missing from database
	// Use Laravel's column names: 'data' for encrypted credential data,
	// 'workspace_id' for tenant scoping (not namespace_id)
	query := `
		SELECT id, name, credential_type, data
		FROM credentials
		WHERE workspace_id = $1 AND id = ANY($2) AND deleted_at IS NULL
	`

	rows, err := r.pool.Query(ctx, query, namespaceID, missing)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch credentials: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, name, credType, encryptedValue string

		if err := rows.Scan(&id, &name, &credType, &encryptedValue); err != nil {
			return nil, fmt.Errorf("failed to scan credential: %w", err)
		}

		// Decrypt — try Laravel format first, fall back to native GCM
		decrypted, err := r.decryptCredential(encryptedValue)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt credential %s: %w", id, err)
		}

		var data map[string]string
		if err := json.Unmarshal([]byte(decrypted), &data); err != nil {
			return nil, fmt.Errorf("failed to parse credential data: %w", err)
		}

		cred := &Credential{
			Name: name,
			Type: credType,
			Data: data,
		}

		result[id] = cred
		r.cache.set(namespaceID, id, cred)
	}

	return result, nil
}

// ResolveByName resolves a credential by name.
func (r *CredentialResolver) ResolveByName(
	ctx context.Context,
	namespaceID string,
	name string,
) (*Credential, error) {
	// Check cache
	if cred := r.cache.getByName(namespaceID, name); cred != nil {
		return cred, nil
	}

	query := `
		SELECT id, name, credential_type, data
		FROM credentials
		WHERE workspace_id = $1 AND name = $2 AND deleted_at IS NULL
	`

	var id, credName, credType, encryptedValue string
	err := r.pool.QueryRow(ctx, query, namespaceID, name).Scan(&id, &credName, &credType, &encryptedValue)
	if err != nil {
		return nil, ErrCredentialNotFound
	}

	decrypted, err := r.decryptCredential(encryptedValue)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	var data map[string]string
	if err := json.Unmarshal([]byte(decrypted), &data); err != nil {
		return nil, fmt.Errorf("failed to parse credential data: %w", err)
	}

	cred := &Credential{
		Name: credName,
		Type: credType,
		Data: data,
	}

	r.cache.setByName(namespaceID, name, cred)
	return cred, nil
}

// decryptCredential attempts to decrypt a credential value.
// It tries Laravel's AES-256-CBC format first, then falls back to native AES-256-GCM.
func (r *CredentialResolver) decryptCredential(encryptedValue string) (string, error) {
	// Try Laravel format first (base64-encoded JSON envelope)
	if r.laravelEncryptor != nil {
		if decrypted, err := r.laravelEncryptor.Decrypt(encryptedValue); err == nil {
			return decrypted, nil
		}
	}

	// Fall back to native GCM encryption
	return r.encryptor.DecryptString(encryptedValue)
}

// InvalidateCache invalidates cached credentials.
func (r *CredentialResolver) InvalidateCache(namespaceID string, credentialIDs ...string) {
	if len(credentialIDs) == 0 {
		r.cache.clearNamespace(namespaceID)
	} else {
		for _, id := range credentialIDs {
			r.cache.invalidate(namespaceID, id)
		}
	}
}

// credentialCache is a simple in-memory credential cache.
type credentialCache struct {
	items map[string]*cacheItem
	ttl   time.Duration
	mu    sync.RWMutex
}

type cacheItem struct {
	credential *Credential
	expiresAt  time.Time
}

func newCredentialCache(ttl time.Duration) *credentialCache {
	cache := &credentialCache{
		items: make(map[string]*cacheItem),
		ttl:   ttl,
	}

	// Start cleanup goroutine
	go cache.cleanup()

	return cache
}

func (c *credentialCache) key(namespace, id string) string {
	return namespace + ":" + id
}

func (c *credentialCache) get(namespace, id string) *Credential {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[c.key(namespace, id)]
	if !exists || time.Now().After(item.expiresAt) {
		return nil
	}
	return item.credential
}

func (c *credentialCache) getByName(namespace, name string) *Credential {
	return c.get(namespace, "name:"+name)
}

func (c *credentialCache) set(namespace, id string, cred *Credential) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[c.key(namespace, id)] = &cacheItem{
		credential: cred,
		expiresAt:  time.Now().Add(c.ttl),
	}
}

func (c *credentialCache) setByName(namespace, name string, cred *Credential) {
	c.set(namespace, "name:"+name, cred)
}

func (c *credentialCache) invalidate(namespace, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, c.key(namespace, id))
}

func (c *credentialCache) clearNamespace(namespace string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prefix := namespace + ":"
	for key := range c.items {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			delete(c.items, key)
		}
	}
}

func (c *credentialCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for key, item := range c.items {
			if now.After(item.expiresAt) {
				delete(c.items, key)
			}
		}
		c.mu.Unlock()
	}
}
