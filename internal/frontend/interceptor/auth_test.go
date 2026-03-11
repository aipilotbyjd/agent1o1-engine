package interceptor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

func TestNewAuthInterceptor_ValidSecret(t *testing.T) {
	secret := "this-is-a-very-long-secret-key-for-testing-purposes"

	auth, err := NewAuthInterceptor(AuthConfig{
		SecretKey: secret,
	})

	if err != nil {
		t.Fatalf("NewAuthInterceptor() error = %v", err)
	}

	if auth == nil {
		t.Error("NewAuthInterceptor() returned nil")
	}
}

func TestNewAuthInterceptor_ShortSecret(t *testing.T) {
	_, err := NewAuthInterceptor(AuthConfig{
		SecretKey: "short",
	})

	if err != ErrInvalidSecretKey {
		t.Errorf("NewAuthInterceptor() error = %v, want ErrInvalidSecretKey", err)
	}
}

func TestValidateToken_ValidToken(t *testing.T) {
	secret := "this-is-a-very-long-secret-key-for-testing-purposes"

	auth, err := NewAuthInterceptor(AuthConfig{
		SecretKey: secret,
	})
	if err != nil {
		t.Fatalf("NewAuthInterceptor() error = %v", err)
	}

	// Create a valid token
	token := createTestToken(t, secret, Claims{
		Subject:   "user-123",
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
		IssuedAt:  time.Now().Unix(),
	})

	claims, err := auth.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken() error = %v", err)
	}

	if claims.Subject != "user-123" {
		t.Errorf("Subject = %q, want %q", claims.Subject, "user-123")
	}
}

func TestValidateToken_ExpiredToken(t *testing.T) {
	secret := "this-is-a-very-long-secret-key-for-testing-purposes"

	auth, err := NewAuthInterceptor(AuthConfig{
		SecretKey: secret,
	})
	if err != nil {
		t.Fatalf("NewAuthInterceptor() error = %v", err)
	}

	// Create an expired token
	token := createTestToken(t, secret, Claims{
		Subject:   "user-123",
		ExpiresAt: time.Now().Add(-time.Hour).Unix(), // expired
		IssuedAt:  time.Now().Add(-2 * time.Hour).Unix(),
	})

	_, err = auth.ValidateToken(token)
	if err == nil {
		t.Error("ValidateToken() should fail for expired token")
	}
}

func TestValidateToken_InvalidSignature(t *testing.T) {
	secret := "this-is-a-very-long-secret-key-for-testing-purposes"
	wrongSecret := "different-secret-key-that-is-also-long-enough"

	auth, err := NewAuthInterceptor(AuthConfig{
		SecretKey: secret,
	})
	if err != nil {
		t.Fatalf("NewAuthInterceptor() error = %v", err)
	}

	// Create a token with wrong secret
	token := createTestToken(t, wrongSecret, Claims{
		Subject:   "user-123",
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	})

	_, err = auth.ValidateToken(token)
	if err == nil {
		t.Error("ValidateToken() should fail for invalid signature")
	}
}

func TestValidateToken_WrongIssuer(t *testing.T) {
	secret := "this-is-a-very-long-secret-key-for-testing-purposes"

	auth, err := NewAuthInterceptor(AuthConfig{
		SecretKey: secret,
		Issuer:    "expected-issuer",
	})
	if err != nil {
		t.Fatalf("NewAuthInterceptor() error = %v", err)
	}

	// Create a token with wrong issuer
	token := createTestToken(t, secret, Claims{
		Subject:   "user-123",
		Issuer:    "wrong-issuer",
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	})

	_, err = auth.ValidateToken(token)
	if err == nil {
		t.Error("ValidateToken() should fail for wrong issuer")
	}
}

func TestValidateToken_WrongAudience(t *testing.T) {
	secret := "this-is-a-very-long-secret-key-for-testing-purposes"

	auth, err := NewAuthInterceptor(AuthConfig{
		SecretKey: secret,
		Audience:  "expected-audience",
	})
	if err != nil {
		t.Fatalf("NewAuthInterceptor() error = %v", err)
	}

	// Create a token with wrong audience
	token := createTestToken(t, secret, Claims{
		Subject:   "user-123",
		Audience:  []string{"wrong-audience"},
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	})

	_, err = auth.ValidateToken(token)
	if err == nil {
		t.Error("ValidateToken() should fail for wrong audience")
	}
}

func TestValidateToken_MalformedToken(t *testing.T) {
	secret := "this-is-a-very-long-secret-key-for-testing-purposes"

	auth, err := NewAuthInterceptor(AuthConfig{
		SecretKey: secret,
	})
	if err != nil {
		t.Fatalf("NewAuthInterceptor() error = %v", err)
	}

	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"no dots", "notokenhere"},
		{"one dot", "part1.part2"},
		{"invalid base64", "!!!.@@@.###"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := auth.ValidateToken(tt.token)
			if err == nil {
				t.Error("ValidateToken() should fail for malformed token")
			}
		})
	}
}

// Helper function to create test JWT tokens
func createTestToken(t *testing.T, secret string, claims Claims) string {
	t.Helper()

	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}

	headerJSON, _ := json.Marshal(header)
	claimsJSON, _ := json.Marshal(claims)

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)

	signatureInput := headerB64 + "." + claimsB64

	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(signatureInput))
	signature := h.Sum(nil)
	signatureB64 := base64.RawURLEncoding.EncodeToString(signature)

	return headerB64 + "." + claimsB64 + "." + signatureB64
}
