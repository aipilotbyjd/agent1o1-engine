package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	matchingv1 "github.com/linkflow/engine/api/gen/linkflow/matching/v1"
	"github.com/linkflow/engine/internal/matching"
	"github.com/linkflow/engine/internal/version"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		port           = flag.Int("port", 7235, "gRPC server port")
		httpPort       = flag.Int("http-port", 8080, "HTTP server port")
		partitionCount = flag.Int("partition-count", 4, "Number of partitions")
		redisAddr      = flag.String("redis-addr", getEnv("REDIS_ADDR", "localhost:6379"), "Redis address")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	printBanner("Matching", logger)

	if *partitionCount < 1 || *partitionCount > math.MaxInt32 {
		logger.Error("invalid partition count", slog.Int("partition_count", *partitionCount))
		os.Exit(1)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: *redisAddr,
	})

	svc := matching.NewService(matching.Config{
		NumPartitions: int32(*partitionCount),
		Replicas:      100,
		Logger:        logger,
		RedisClient:   redisClient,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		logger.Error("failed to start matching service", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		if err := svc.Stop(); err != nil {
			logger.Error("failed to stop matching service", slog.String("error", err.Error()))
		}
	}()

	server := grpc.NewServer()
	matchingv1.RegisterMatchingServiceServer(server, matching.NewGRPCServer(svc))
	reflection.Register(server)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Error("failed to listen", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Setup HTTP Server for Health Checks
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", *httpPort),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Start gRPC server
	go func() {
		logger.Info("starting gRPC server", slog.Int("port", *port), slog.Int("partition_count", *partitionCount))
		if err := server.Serve(lis); err != nil {
			logger.Error("gRPC server failed", slog.String("error", err.Error()))
			cancel()
		}
	}()

	// Start HTTP server
	go func() {
		logger.Info("starting HTTP server", slog.Int("port", *httpPort))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", slog.String("error", err.Error()))
			cancel()
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received signal, initiating graceful shutdown", slog.String("signal", sig.String()))
	case <-ctx.Done():
		logger.Info("context cancelled, initiating shutdown")
	}

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop accepting new connections
	server.GracefulStop()
	logger.Info("gRPC server stopped")

	// Shutdown HTTP server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", slog.String("error", err.Error()))
	} else {
		logger.Info("HTTP server stopped")
	}

	cancel()
	logger.Info("matching service stopped")
}

func printBanner(service string, logger *slog.Logger) {
	logger.Info(fmt.Sprintf("LinkFlow %s Service", service),
		slog.String("version", version.Version),
		slog.String("commit", version.GitCommit),
		slog.String("build_time", version.BuildTime),
	)
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
