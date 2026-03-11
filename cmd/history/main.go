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
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/jackc/pgx/v5/pgxpool"
	historyv1 "github.com/linkflow/engine/api/gen/linkflow/history/v1"
	matchingv1 "github.com/linkflow/engine/api/gen/linkflow/matching/v1"
	"github.com/linkflow/engine/internal/history"
	"github.com/linkflow/engine/internal/history/shard"
	"github.com/linkflow/engine/internal/history/store"
	"github.com/linkflow/engine/internal/history/visibility"
	"github.com/linkflow/engine/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		port         = flag.Int("port", 7234, "gRPC server port")
		httpPort     = flag.Int("http-port", 8080, "HTTP server port")
		shardCount   = flag.Int("shard-count", 16, "Number of shards")
		dbUrl        = flag.String("db-url", getEnv("DATABASE_URL", "postgres://linkflow-postgres:5432/linkflow"), "Database URL")
		matchingAddr = flag.String("matching-addr", getEnv("MATCHING_ADDR", "localhost:7235"), "Matching service address")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	printBanner("History", logger)

	// Connect to database
	dbpool, err := pgxpool.New(context.Background(), *dbUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	if err := dbpool.Ping(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to ping database: %v\n", err)
		os.Exit(1)
	}
	logger.Info("connected to database")

	// Connect to Matching Service
	matchingConn, err := grpc.NewClient(*matchingAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to matching service", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer matchingConn.Close()
	matchingClient := matchingv1.NewMatchingServiceClient(matchingConn)

	shardController := shard.NewController(int32(*shardCount))

	// Initialize stores
	eventStore := store.NewPostgresEventStore(dbpool, int32(*shardCount))
	stateStore := store.NewPostgresMutableStateStore(dbpool, int32(*shardCount))
	visibilityStore := visibility.NewPostgresStore(dbpool)

	svc := history.NewService(
		shardController,
		eventStore,
		stateStore,
		visibilityStore,
		matchingClient,
		logger,
	)

	server := grpc.NewServer()
	historyv1.RegisterHistoryServiceServer(server, history.NewGRPCServer(svc))
	reflection.Register(server)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("failed to start service: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", slog.String("signal", sig.String()))
		cancel()
		if err := svc.Stop(ctx); err != nil {
			logger.Error("failed to stop service", slog.String("error", err.Error()))
		}
		server.GracefulStop()
	}()

	logger.Info("starting gRPC server", slog.Int("port", *port), slog.Int("shard_count", *shardCount))

	go func() {
		if err := server.Serve(lis); err != nil {
			logger.Error("server failed", slog.String("error", err.Error()))
			cancel()
		}
	}()

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
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

		logger.Info("starting HTTP server", slog.Int("port", *httpPort))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", slog.String("error", err.Error()))
			cancel()
		}
	}()

	<-ctx.Done()
	logger.Info("history service stopped")
	return nil
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
