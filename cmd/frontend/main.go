package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/linkflow/engine/internal/frontend"
	"github.com/linkflow/engine/internal/frontend/adapter"
	"github.com/linkflow/engine/internal/frontend/handler"
	"github.com/linkflow/engine/internal/frontend/interceptor"
	"github.com/linkflow/engine/internal/version"
)

func main() {
	var (
		port         = flag.Int("port", 7233, "gRPC server port")
		httpPort     = flag.Int("http-port", 8080, "HTTP server port")
		historyAddr  = flag.String("history-addr", getEnv("HISTORY_ADDR", "localhost:7234"), "History service address")
		matchingAddr = flag.String("matching-addr", getEnv("MATCHING_ADDR", "localhost:7235"), "Matching service address")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	printBanner("Frontend", logger)

	// Initialize Redis
	redisURL := os.Getenv("REDIS_URL")
	var redisOpt *redis.Options
	if redisURL != "" {
		var err error
		redisOpt, err = redis.ParseURL(redisURL)
		if err != nil {
			logger.Error("failed to parse REDIS_URL", slog.String("error", err.Error()))
			os.Exit(1)
		}
	} else {
		redisOpt = &redis.Options{
			Addr: "localhost:6379",
		}
	}
	rdb := redis.NewClient(redisOpt)

	// Initialize gRPC Connections
	historyConn, err := grpc.NewClient(*historyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to history service", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer historyConn.Close()

	matchingConn, err := grpc.NewClient(*matchingAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to matching service", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer matchingConn.Close()

	// Initialize Real Clients
	historyClient := adapter.NewHistoryClient(historyConn)
	matchingClient := adapter.NewMatchingClient(matchingConn)

	loggingInterceptor := interceptor.NewLoggingInterceptor(logger)
	authInterceptor, err := interceptor.NewAuthInterceptor(interceptor.AuthConfig{
		SkipMethods: []string{"/grpc.health.v1.Health/Check"},
	})
	if err != nil {
		logger.Error("failed to create auth interceptor", slog.String("error", err.Error()))
		os.Exit(1)
	}

	svc := frontend.NewService(historyClient, matchingClient, logger, frontend.DefaultServiceConfig())

	// Start Redis Consumer
	consumer := frontend.NewRedisConsumerWithConfig(rdb, svc, logger, frontend.ConsumerConfig{
		Retry:          frontend.DefaultConsumerConfig().Retry,
		DLQStreamKey:   getEnv("JOB_DLQ_STREAM", frontend.DefaultConsumerConfig().DLQStreamKey),
		GroupName:      getEnv("JOB_CONSUMER_GROUP", frontend.DefaultConsumerConfig().GroupName),
		PartitionCount: getEnvInt("ENGINE_PARTITION_COUNT", frontend.DefaultConsumerConfig().PartitionCount),
		ClaimMinIdle:   getEnvDuration("JOB_CLAIM_MIN_IDLE", frontend.DefaultConsumerConfig().ClaimMinIdle),
		ClaimBatch:     int64(getEnvInt("JOB_CLAIM_BATCH", int(frontend.DefaultConsumerConfig().ClaimBatch))),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumer in background
	go consumer.Start(ctx)

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			loggingInterceptor.UnaryInterceptor,
			authInterceptor.UnaryInterceptor,
		),
		grpc.ChainStreamInterceptor(
			loggingInterceptor.StreamInterceptor,
			authInterceptor.StreamInterceptor,
		),
	)

	reflection.Register(server)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Error("failed to listen", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ctx and cancel already defined above

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", slog.String("signal", sig.String()))
		cancel()
		server.GracefulStop()
	}()

	logger.Info("starting gRPC server", slog.Int("port", *port))

	go func() {
		if err := server.Serve(lis); err != nil {
			logger.Error("server failed", slog.String("error", err.Error()))
			cancel()
		}
	}()

	// Start HTTP Server for Health Checks and Engine API
	go func() {
		mux := http.NewServeMux()

		// Register Engine API routes
		frontendHandler := handler.NewHTTPHandler(svc, logger)
		frontendHandler.RegisterRoutes(mux)

		httpServer := &http.Server{
			Addr:              fmt.Sprintf(":%d", *httpPort),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		logger.Info("starting HTTP server", slog.Int("port", *httpPort))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", slog.String("error", err.Error()))
			cancel()
		}
	}()

	<-ctx.Done()
	logger.Info("frontend service stopped")
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

func getEnvInt(key string, fallback int) int {
	value, exists := os.LookupEnv(key)
	if !exists || value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value, exists := os.LookupEnv(key)
	if !exists || value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return parsed
}
