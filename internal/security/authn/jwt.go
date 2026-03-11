package authn

import (
	"context"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrTokenExpired     = errors.New("token expired")
	ErrTokenInvalid     = errors.New("token invalid")
	ErrTokenMalformed   = errors.New("token malformed")
	ErrSignatureInvalid = errors.New("signature invalid")
)

// Claims represents JWT claims.
type Claims struct {
	Subject   string   `json:"sub"`
	Issuer    string   `json:"iss"`
	Audience  []string `json:"aud"`
	ExpiresAt int64    `json:"exp"`
	IssuedAt  int64    `json:"iat"`
	NotBefore int64    `json:"nbf"`

	// Custom claims
	WorkspaceID string   `json:"workspace_id,omitempty"`
	UserID      string   `json:"user_id,omitempty"`
	Roles       []string `json:"roles,omitempty"`
	Scopes      []string `json:"scopes,omitempty"`
}

// IsExpired checks if the token is expired.
func (c *Claims) IsExpired() bool {
	return time.Now().Unix() > c.ExpiresAt
}

// HasScope checks if the token has a specific scope.
func (c *Claims) HasScope(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// HasRole checks if the token has a specific role.
func (c *Claims) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// JWTValidator validates JWT tokens.
type JWTValidator struct {
	issuer    string
	audience  string
	publicKey *rsa.PublicKey
	secretKey []byte // For HMAC

	// JWKS support
	jwksURL   string
	jwksCache map[string]*rsa.PublicKey
	jwksMu    sync.RWMutex
}

// JWTConfig holds JWT validator configuration.
type JWTConfig struct {
	Issuer    string
	Audience  string
	PublicKey string // PEM-encoded public key
	SecretKey string // For HMAC
	JWKSURL   string // For dynamic key fetching
}

// NewJWTValidator creates a new JWT validator.
func NewJWTValidator(config JWTConfig) (*JWTValidator, error) {
	v := &JWTValidator{
		issuer:    config.Issuer,
		audience:  config.Audience,
		jwksURL:   config.JWKSURL,
		jwksCache: make(map[string]*rsa.PublicKey),
	}

	if config.PublicKey != "" {
		block, _ := pem.Decode([]byte(config.PublicKey))
		if block == nil {
			return nil, errors.New("failed to parse PEM block")
		}

		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key: %w", err)
		}

		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("not an RSA public key")
		}

		v.publicKey = rsaPub
	}

	if config.SecretKey != "" {
		v.secretKey = []byte(config.SecretKey)
	}

	return v, nil
}

// Implements full signature verification for HMAC-SHA256 (HS256).
func (v *JWTValidator) Validate(ctx context.Context, token string) (*Claims, error) {
	// Parse token parts (header.payload.signature)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrTokenMalformed
	}

	headerPart, payloadPart, signaturePart := parts[0], parts[1], parts[2]

	// Decode and validate header
	headerBytes, err := base64URLDecode(headerPart)
	if err != nil {
		return nil, ErrTokenMalformed
	}

	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid,omitempty"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, ErrTokenMalformed
	}

	// Verify signature based on algorithm
	if err := v.verifySignature(header.Alg, headerPart+"."+payloadPart, signaturePart); err != nil {
		return nil, err
	}

	// Decode payload
	payloadBytes, err := base64URLDecode(payloadPart)
	if err != nil {
		return nil, ErrTokenMalformed
	}

	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, ErrTokenMalformed
	}

	// Validate expiration
	if claims.IsExpired() {
		return nil, ErrTokenExpired
	}

	// Validate not-before time
	if claims.NotBefore > 0 && time.Now().Unix() < claims.NotBefore {
		return nil, ErrTokenInvalid
	}

	// Validate issuer
	if v.issuer != "" && claims.Issuer != v.issuer {
		return nil, ErrTokenInvalid
	}

	// Validate audience
	if v.audience != "" && !containsString(claims.Audience, v.audience) {
		return nil, ErrTokenInvalid
	}

	return &claims, nil
}

// verifySignature verifies the JWT signature based on the algorithm.
func (v *JWTValidator) verifySignature(alg, signatureInput, signaturePart string) error {
	// Decode the provided signature
	providedSig, err := base64URLDecode(signaturePart)
	if err != nil {
		return ErrSignatureInvalid
	}

	switch alg {
	case "HS256":
		if len(v.secretKey) == 0 {
			return errors.New("HMAC secret key not configured")
		}
		expectedSig := v.computeHMAC256([]byte(signatureInput))
		if !hmac.Equal(expectedSig, providedSig) {
			return ErrSignatureInvalid
		}

	case "RS256":
		if v.publicKey == nil {
			return errors.New("RSA public key not configured")
		}
		// For RS256, we would verify using the public key
		// This requires crypto/rsa.VerifyPKCS1v15
		return errors.New("RS256 verification not yet implemented")

	case "none":
		// NEVER allow "none" algorithm - this is a common JWT attack vector
		return errors.New("algorithm 'none' is not allowed")

	default:
		return fmt.Errorf("unsupported algorithm: %s", alg)
	}

	return nil
}

// computeHMAC256 computes HMAC-SHA256.
func (v *JWTValidator) computeHMAC256(data []byte) []byte {
	h := hmac.New(sha256.New, v.secretKey)
	h.Write(data)
	return h.Sum(nil)
}

// Query parameter extraction has been removed as it's a security risk.
func ExtractToken(r *http.Request) (string, error) {
	// Check Authorization header (preferred method)
	auth := r.Header.Get("Authorization")
	if auth != "" {
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
			token := strings.TrimSpace(parts[1])
			if token != "" {
				return token, nil
			}
		}
	}

	// Check HttpOnly cookie (for browser-based authentication)
	// Note: Cookie should be set with HttpOnly, Secure, and SameSite flags
	cookie, err := r.Cookie("__Host-token")
	if err == nil && cookie.Value != "" {
		return cookie.Value, nil
	}

	// Fallback to legacy cookie name for backwards compatibility
	cookie, err = r.Cookie("token")
	if err == nil && cookie.Value != "" {
		return cookie.Value, nil
	}

	// SECURITY: Query parameter token extraction has been intentionally removed
	// Tokens in URLs can be logged, cached, leaked via Referer headers, and
	// stored in browser history. This is a significant security risk.

	return "", errors.New("no token found")
}

const maxAPIKeyCacheSize = 10000

// APIKeyValidator validates API keys.
type APIKeyValidator struct {
	keys       map[string]*APIKey
	cacheOrder []string
	keysMu     sync.RWMutex

	loader APIKeyLoader
}

// APIKey represents an API key.
type APIKey struct {
	ID          string
	Key         string
	Name        string
	WorkspaceID string
	Scopes      []string
	ExpiresAt   *time.Time
	CreatedAt   time.Time
}

// APIKeyLoader loads API keys.
type APIKeyLoader interface {
	Load(ctx context.Context, keyHash string) (*APIKey, error)
}

// NewAPIKeyValidator creates a new API key validator.
func NewAPIKeyValidator(loader APIKeyLoader) *APIKeyValidator {
	return &APIKeyValidator{
		keys:       make(map[string]*APIKey),
		cacheOrder: make([]string, 0, maxAPIKeyCacheSize),
		loader:     loader,
	}
}

// Validate validates an API key.
func (v *APIKeyValidator) Validate(ctx context.Context, key string) (*APIKey, error) {
	// Check cache
	v.keysMu.RLock()
	cached, exists := v.keys[key]
	v.keysMu.RUnlock()

	if exists {
		if cached.ExpiresAt != nil && time.Now().After(*cached.ExpiresAt) {
			v.keysMu.Lock()
			delete(v.keys, key)
			v.keysMu.Unlock()
			return nil, ErrTokenExpired
		}
		return cached, nil
	}

	// Load from storage
	if v.loader == nil {
		return nil, ErrTokenInvalid
	}

	apiKey, err := v.loader.Load(ctx, hashKey(key))
	if err != nil {
		return nil, ErrTokenInvalid
	}

	// Cache with bounded size to prevent memory exhaustion
	v.keysMu.Lock()
	if len(v.keys) >= maxAPIKeyCacheSize {
		// Evict oldest entry
		oldest := v.cacheOrder[0]
		v.cacheOrder = v.cacheOrder[1:]
		delete(v.keys, oldest)
	}
	v.keys[key] = apiKey
	v.cacheOrder = append(v.cacheOrder, key)
	v.keysMu.Unlock()

	return apiKey, nil
}

// ExtractAPIKey extracts API key from request.
func ExtractAPIKey(r *http.Request) (string, error) {
	// Check header
	key := r.Header.Get("X-API-Key")
	if key != "" {
		return key, nil
	}

	// Check Authorization header with ApiKey scheme
	auth := r.Header.Get("Authorization")
	if auth != "" {
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "apikey") {
			return parts[1], nil
		}
	}

	return "", errors.New("no API key found")
}

// Helper functions

// This properly handles the URL-safe alphabet and missing padding.
func base64URLDecode(s string) ([]byte, error) {
	// base64.RawURLEncoding handles URL-safe alphabet without padding
	// We need to handle cases where padding might be present
	s = strings.TrimRight(s, "=")
	return base64.RawURLEncoding.DecodeString(s)
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// The hash is used for storage and comparison to avoid storing plaintext keys.
func hashKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}
