package interceptor

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	authorizationHeader = "authorization"
	bearerPrefix        = "Bearer "
)

type AuthInterceptor struct {
	skipMethods map[string]bool
	secretKey   []byte
	issuer      string
	audience    string
}

type AuthConfig struct {
	SkipMethods []string
	SecretKey   string // JWT signing secret (min 32 chars)
	Issuer      string // Expected token issuer
	Audience    string // Expected token audience
}

// ErrInvalidSecretKey is returned when the JWT secret key is invalid.
var ErrInvalidSecretKey = errors.New("JWT_SECRET must be at least 32 characters for security")

// NewAuthInterceptor creates a new authentication interceptor.
// Returns an error if the secret key is too short (minimum 32 characters required).
func NewAuthInterceptor(cfg AuthConfig) (*AuthInterceptor, error) {
	skipMethods := make(map[string]bool)
	for _, method := range cfg.SkipMethods {
		skipMethods[method] = true
	}

	// Get secret key from config or environment
	secretKey := cfg.SecretKey
	if secretKey == "" {
		secretKey = os.Getenv("JWT_SECRET")
	}

	// Validate secret key length (min 32 chars for security)
	if len(secretKey) < 32 {
		return nil, ErrInvalidSecretKey
	}

	return &AuthInterceptor{
		skipMethods: skipMethods,
		secretKey:   []byte(secretKey),
		issuer:      cfg.Issuer,
		audience:    cfg.Audience,
	}, nil
}

func (a *AuthInterceptor) UnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if a.skipMethods[info.FullMethod] {
		return handler(ctx, req)
	}

	token, err := a.extractToken(ctx)
	if err != nil {
		return nil, err
	}

	claims, err := a.ValidateToken(token)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	ctx = context.WithValue(ctx, claimsContextKey{}, claims)

	return handler(ctx, req)
}

func (a *AuthInterceptor) StreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if a.skipMethods[info.FullMethod] {
		return handler(srv, ss)
	}

	token, err := a.extractToken(ss.Context())
	if err != nil {
		return err
	}

	_, err = a.ValidateToken(token)
	if err != nil {
		return status.Error(codes.Unauthenticated, "invalid token")
	}

	return handler(srv, ss)
}

func (a *AuthInterceptor) extractToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get(authorizationHeader)
	if len(authHeaders) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authHeaders[0]
	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return "", status.Error(codes.Unauthenticated, "invalid authorization header format")
	}

	return strings.TrimPrefix(authHeader, bearerPrefix), nil
}

// Claims represents the JWT claims structure.
type Claims struct {
	// Standard JWT claims
	Subject   string   `json:"sub"`
	Issuer    string   `json:"iss"`
	Audience  []string `json:"aud"`
	ExpiresAt int64    `json:"exp"`
	IssuedAt  int64    `json:"iat"`
	NotBefore int64    `json:"nbf"`

	// Custom claims
	Namespace   string   `json:"namespace,omitempty"`
	WorkspaceID string   `json:"workspace_id,omitempty"`
	UserID      string   `json:"user_id,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
	Roles       []string `json:"roles,omitempty"`
}

// This implements proper HMAC-SHA256 signature verification.
func (a *AuthInterceptor) ValidateToken(token string) (*Claims, error) {
	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "empty token")
	}

	// Split token into parts (header.payload.signature)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, status.Error(codes.Unauthenticated, "malformed token: expected 3 parts")
	}

	headerPart, payloadPart, signaturePart := parts[0], parts[1], parts[2]

	// Verify signature using HMAC-SHA256
	signatureInput := headerPart + "." + payloadPart
	expectedSignature := a.computeHMAC([]byte(signatureInput))

	// Decode the provided signature
	providedSignature, err := base64URLDecode(signaturePart)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid signature encoding")
	}

	// Constant-time comparison to prevent timing attacks
	if !hmac.Equal(expectedSignature, providedSignature) {
		return nil, status.Error(codes.Unauthenticated, "invalid signature")
	}

	// Decode and parse the payload
	payloadBytes, err := base64URLDecode(payloadPart)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid payload encoding")
	}

	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid payload format")
	}

	// Validate expiration time
	now := time.Now().Unix()
	if claims.ExpiresAt > 0 && now > claims.ExpiresAt {
		return nil, status.Error(codes.Unauthenticated, "token expired")
	}

	// Validate not-before time
	if claims.NotBefore > 0 && now < claims.NotBefore {
		return nil, status.Error(codes.Unauthenticated, "token not yet valid")
	}

	// Validate issuer if configured
	if a.issuer != "" && claims.Issuer != a.issuer {
		return nil, status.Error(codes.Unauthenticated, "invalid token issuer")
	}

	// Validate audience if configured
	if a.audience != "" {
		validAudience := false
		for _, aud := range claims.Audience {
			if aud == a.audience {
				validAudience = true
				break
			}
		}
		if !validAudience {
			return nil, status.Error(codes.Unauthenticated, "invalid token audience")
		}
	}

	return &claims, nil
}

// computeHMAC computes HMAC-SHA256 of the input.
func (a *AuthInterceptor) computeHMAC(data []byte) []byte {
	h := hmac.New(sha256.New, a.secretKey)
	h.Write(data)
	return h.Sum(nil)
}

// base64URLDecode decodes base64url-encoded data (RFC 4648).
func base64URLDecode(s string) ([]byte, error) {
	// Add padding if necessary
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.URLEncoding.DecodeString(s)
}

type claimsContextKey struct{}

func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey{}).(*Claims)
	return claims, ok
}
