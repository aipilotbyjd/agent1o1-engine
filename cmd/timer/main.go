package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	historyv1 "github.com/linkflow/engine/api/gen/linkflow/history/v1"
	"github.com/linkflow/engine/internal/timer"
	"github.com/linkflow/engine/internal/timer/store"
	"github.com/linkflow/engine/internal/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	var (
		port        = flag.Int("port", 7237, "Timer service port")
		httpPort    = flag.Int("http-port", 8083, "HTTP server port") // Changed to 8083 to avoid conflict
		historyAddr = flag.String("history-addr", getEnv("HISTORY_ADDR", "localhost:7234"), "History service address")
		connString  = flag.String("db-url", getEnv("DATABASE_URL", "postgres://linkflow:linkflow@localhost:5432/linkflow?sslmode=disable"), "Database connection string")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	printBanner("Timer", logger)

	// Connect to PostgreSQL
	config, err := pgxpool.ParseConfig(*connString)
	if err != nil {
		logger.Error("failed to parsing database config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		logger.Error("failed to connect to database", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pool.Close()

	// Connect to History Service
	conn, err := grpc.NewClient(*historyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to history service", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer conn.Close()

	historyClient := historyv1.NewHistoryServiceClient(conn)

	// Initialize Store and Service
	timerStore := store.NewPostgresStore(pool)

	svcConfig := timer.DefaultConfig()
	svcConfig.Logger = logger

	svc := timer.NewService(
		timerStore,
		&grpcHistoryClient{client: historyClient, logger: logger},
		svcConfig,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Service
	if err := svc.Start(ctx); err != nil {
		logger.Error("failed to start timer service", slog.String("error", err.Error()))
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP Server for Health Checks
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			if svc.IsRunning() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("Not Running"))
			}
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

	logger.Info("timer service started", slog.Int("port", *port), slog.String("history_addr", *historyAddr))

	// Wait for signal
	sig := <-sigCh
	logger.Info("received signal, shutting down", slog.String("signal", sig.String()))

	svc.Stop(ctx)
	cancel()
	logger.Info("timer service stopped")
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

// grpcHistoryClient implements timer.HistoryClient
type grpcHistoryClient struct {
	client historyv1.HistoryServiceClient
	logger *slog.Logger
}

func (c *grpcHistoryClient) RecordTimerFired(ctx context.Context, namespaceID, workflowID, runID, timerID string) error {
	// Retry logic for optimistic locking
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		err := c.tryRecordTimerFired(ctx, namespaceID, workflowID, runID, timerID)
		if err == nil {
			return nil
		}
		// If error is basic, return. If it's optimistic lock, retry.
		// Since we don't have easy check for error codes here without partial error string matching or gRPC status code
		// We will just retry everything for now or log and fail.
		// Realistically, we should check status.Code(err) == codes.Aborted

		c.logger.Warn("failed to record timer fired, retrying", slog.Int("attempt", i+1), slog.String("error", err.Error()))
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("failed after retries")
}

func (c *grpcHistoryClient) tryRecordTimerFired(ctx context.Context, namespaceID, workflowID, runID, timerID string) error {
	reqState := &historyv1.GetMutableStateRequest{
		Namespace: namespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}
	respState, err := c.client.GetMutableState(ctx, reqState)
	if err != nil {
		return fmt.Errorf("failed to get mutable state: %w", err)
	}

	event := &historyv1.HistoryEvent{
		EventId:   respState.NextEventId,
		EventType: commonv1.EventType_EVENT_TYPE_TIMER_FIRED,
		EventTime: timestamppb.Now(),
		Attributes: &historyv1.HistoryEvent_TimerFiredAttributes{
			TimerFiredAttributes: &historyv1.TimerFiredEventAttributes{
				TimerId:        timerID,
				StartedEventId: 0, // NOTE: We don't have this info. History validation might fail or accept 0.
			},
		},
	}

	_, err = c.client.RecordEvent(ctx, &historyv1.RecordEventRequest{
		Namespace: namespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Event: event,
	})

	return err
}
