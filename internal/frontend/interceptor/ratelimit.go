package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/linkflow/engine/internal/frontend/ratelimit"
)

const (
	namespaceHeader  = "x-namespace"
	defaultNamespace = "default"
)

type RateLimitInterceptor struct {
	limiter *ratelimit.Limiter
}

func NewRateLimitInterceptor(limiter *ratelimit.Limiter) *RateLimitInterceptor {
	return &RateLimitInterceptor{
		limiter: limiter,
	}
}

func (r *RateLimitInterceptor) UnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	namespace := r.extractNamespace(ctx)

	if !r.limiter.Allow(namespace) {
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	return handler(ctx, req)
}

func (r *RateLimitInterceptor) StreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	namespace := r.extractNamespace(ss.Context())

	if !r.limiter.Allow(namespace) {
		return status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	return handler(srv, ss)
}

func (r *RateLimitInterceptor) extractNamespace(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return defaultNamespace
	}

	namespaces := md.Get(namespaceHeader)
	if len(namespaces) == 0 {
		return defaultNamespace
	}

	return namespaces[0]
}

type NamespaceExtractor interface {
	ExtractNamespace(req interface{}) string
}

type RequestBasedRateLimitInterceptor struct {
	limiter   *ratelimit.Limiter
	extractor NamespaceExtractor
}

func NewRequestBasedRateLimitInterceptor(
	limiter *ratelimit.Limiter,
	extractor NamespaceExtractor,
) *RequestBasedRateLimitInterceptor {
	return &RequestBasedRateLimitInterceptor{
		limiter:   limiter,
		extractor: extractor,
	}
}

func (r *RequestBasedRateLimitInterceptor) UnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	namespace := defaultNamespace
	if r.extractor != nil {
		namespace = r.extractor.ExtractNamespace(req)
	}

	if !r.limiter.Allow(namespace) {
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	return handler(ctx, req)
}
