package interceptor

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LoggingInterceptor struct {
	logger *slog.Logger
}

func NewLoggingInterceptor(logger *slog.Logger) *LoggingInterceptor {
	return &LoggingInterceptor{
		logger: logger,
	}
}

func (l *LoggingInterceptor) UnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	code := codes.OK
	if err != nil {
		if st, ok := status.FromError(err); ok {
			code = st.Code()
		} else {
			code = codes.Unknown
		}
	}

	l.logRequest(ctx, info.FullMethod, duration, code, err)

	return resp, err
}

func (l *LoggingInterceptor) StreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()

	err := handler(srv, ss)

	duration := time.Since(start)
	code := codes.OK
	if err != nil {
		if st, ok := status.FromError(err); ok {
			code = st.Code()
		} else {
			code = codes.Unknown
		}
	}

	l.logRequest(ss.Context(), info.FullMethod, duration, code, err)

	return err
}

func (l *LoggingInterceptor) logRequest(
	ctx context.Context,
	method string,
	duration time.Duration,
	code codes.Code,
	err error,
) {
	attrs := []slog.Attr{
		slog.String("method", method),
		slog.Duration("duration", duration),
		slog.String("code", code.String()),
	}

	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		l.logger.LogAttrs(ctx, slog.LevelError, "grpc request failed", attrs...)
	} else {
		l.logger.LogAttrs(ctx, slog.LevelInfo, "grpc request completed", attrs...)
	}
}

type DetailedLoggingInterceptor struct {
	logger        *slog.Logger
	logPayload    bool
	slowThreshold time.Duration
}

type DetailedLoggingConfig struct {
	LogPayload    bool
	SlowThreshold time.Duration
}

func NewDetailedLoggingInterceptor(logger *slog.Logger, cfg DetailedLoggingConfig) *DetailedLoggingInterceptor {
	threshold := cfg.SlowThreshold
	if threshold == 0 {
		threshold = time.Second
	}

	return &DetailedLoggingInterceptor{
		logger:        logger,
		logPayload:    cfg.LogPayload,
		slowThreshold: threshold,
	}
}

func (l *DetailedLoggingInterceptor) UnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	if l.logPayload {
		l.logger.DebugContext(ctx, "grpc request started",
			slog.String("method", info.FullMethod),
			slog.Any("request", req),
		)
	}

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	code := codes.OK
	if err != nil {
		if st, ok := status.FromError(err); ok {
			code = st.Code()
		} else {
			code = codes.Unknown
		}
	}

	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelError
	} else if duration > l.slowThreshold {
		level = slog.LevelWarn
	}

	attrs := []slog.Attr{
		slog.String("method", info.FullMethod),
		slog.Duration("duration", duration),
		slog.String("code", code.String()),
	}

	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}

	if duration > l.slowThreshold {
		attrs = append(attrs, slog.Bool("slow", true))
	}

	if l.logPayload && resp != nil {
		attrs = append(attrs, slog.Any("response", resp))
	}

	l.logger.LogAttrs(ctx, level, "grpc request", attrs...)

	return resp, err
}
