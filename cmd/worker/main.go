package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/linkflow/engine/internal/version"
	"github.com/linkflow/engine/internal/worker"
	"github.com/linkflow/engine/internal/worker/adapter"
	"github.com/linkflow/engine/internal/worker/executor"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		httpPort  = flag.Int("http-port", 8080, "HTTP server port")
		taskQueue = flag.String("task-queue", getEnv("TASK_QUEUE", "default"), "Task queue name")

		matchingAddr = flag.String("matching-addr", getEnv("MATCHING_ADDR", "localhost:7235"), "Matching service address")
		historyAddr  = flag.String("history-addr", getEnv("HISTORY_ADDR", "localhost:7234"), "History service address")
		numWorkers   = flag.Int("num-workers", 4, "Number of worker goroutines")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	printBanner("Worker", logger)

	if getEnv("CALLBACK_SECRET", "") == "" {
		logger.Warn("CALLBACK_SECRET is not set; API callbacks will fail when signature verification is enabled")
	}

	// Connect to History Service
	var grpcOpts []grpc.DialOption
	if tlsCert := getEnv("GRPC_TLS_CERT_FILE", ""); tlsCert != "" {
		creds, err := credentials.NewClientTLSFromFile(tlsCert, "")
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(creds))
	} else if getEnv("GRPC_TLS_INSECURE", "false") == "true" || getEnv("LINKFLOW_ENV", "development") == "development" {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		logger.Warn("gRPC using insecure plaintext connection; set GRPC_TLS_CERT_FILE for production")
	} else {
		// Default to system TLS in production
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})))
	}
	historyConn, err := grpc.NewClient(*historyAddr, grpcOpts...)
	if err != nil {
		logger.Error("failed to connect to history service", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer historyConn.Close()
	historyClient := adapter.NewHistoryClient(historyConn)

	svc, err := worker.NewService(worker.Config{
		TaskQueues:      strings.Split(*taskQueue, ","),
		NumPollers:      *numWorkers,
		Identity:        fmt.Sprintf("worker-%d", os.Getpid()),
		MatchingAddr:    *matchingAddr,
		PollInterval:    time.Second,
		Logger:          logger,
		CallbackKey:     getEnv("CALLBACK_SECRET", ""),
		CallbackTimeout: 10 * time.Second,
		HistoryClient:   historyClient,
	})
	if err != nil {
		return fmt.Errorf("failed to create worker service: %w", err)
	}

	// Create executor registry for node execution
	nodeRegistry := executor.NewRegistry()

	// Register Workflow Executor (will get registry set after all executors are registered)
	workflowExecutor := executor.NewWorkflowExecutor(historyClient, logger)
	svc.RegisterExecutor(workflowExecutor)

	httpExecutor := executor.NewHTTPExecutor()
	svc.RegisterExecutor(httpExecutor)
	nodeRegistry.MustRegister(httpExecutor)

	// Register additional executors
	transformExecutor := executor.NewTransformExecutor()
	svc.RegisterExecutor(transformExecutor)
	nodeRegistry.MustRegister(transformExecutor)

	loopExecutor := executor.NewLoopExecutor()
	svc.RegisterExecutor(loopExecutor)
	nodeRegistry.MustRegister(loopExecutor)

	conditionExecutor := executor.NewConditionExecutor()
	svc.RegisterExecutor(conditionExecutor)
	nodeRegistry.MustRegister(conditionExecutor)

	emailExecutor := executor.NewEmailExecutor()
	svc.RegisterExecutor(emailExecutor)
	nodeRegistry.MustRegister(emailExecutor)

	delayExecutor := executor.NewDelayExecutor()
	svc.RegisterExecutor(delayExecutor)
	nodeRegistry.MustRegister(delayExecutor)

	aiExecutor := executor.NewAIExecutor()
	svc.RegisterExecutor(aiExecutor)
	nodeRegistry.MustRegister(aiExecutor)

	webhookExecutor := executor.NewWebhookExecutor()
	svc.RegisterExecutor(webhookExecutor)
	nodeRegistry.MustRegister(webhookExecutor)

	manualExecutor := executor.NewManualExecutor()
	svc.RegisterExecutor(manualExecutor)
	nodeRegistry.MustRegister(manualExecutor)

	slackExecutor := executor.NewSlackExecutor()
	svc.RegisterExecutor(slackExecutor)
	nodeRegistry.MustRegister(slackExecutor)

	discordExecutor := executor.NewDiscordExecutor()
	svc.RegisterExecutor(discordExecutor)
	nodeRegistry.MustRegister(discordExecutor)

	twilioExecutor := executor.NewTwilioExecutor()
	svc.RegisterExecutor(twilioExecutor)
	nodeRegistry.MustRegister(twilioExecutor)

	// Script executor for action_script nodes
	scriptExecutor := executor.NewScriptExecutor()
	svc.RegisterExecutor(scriptExecutor)
	nodeRegistry.MustRegister(scriptExecutor)

	// Output executor for output_log nodes
	outputExecutor := executor.NewOutputExecutor()
	svc.RegisterExecutor(outputExecutor)
	nodeRegistry.MustRegister(outputExecutor)

	// Logic condition alias (handles logic_condition node type)
	logicConditionExecutor := executor.NewLogicConditionExecutor()
	svc.RegisterExecutor(logicConditionExecutor)
	nodeRegistry.MustRegister(logicConditionExecutor)

	// Database executor for action_database nodes
	databaseExecutor := executor.NewDatabaseExecutor()
	svc.RegisterExecutor(databaseExecutor)
	nodeRegistry.MustRegister(databaseExecutor)

	// Storage executor for action_storage nodes
	storageExecutor := executor.NewStorageExecutor()
	svc.RegisterExecutor(storageExecutor)
	nodeRegistry.MustRegister(storageExecutor)

	// Code executor for action_code nodes
	codeExecutor := executor.NewCodeExecutor()
	svc.RegisterExecutor(codeExecutor)
	nodeRegistry.MustRegister(codeExecutor)

	// Approval executor for action_approval nodes
	approvalExecutor := executor.NewApprovalExecutor()
	svc.RegisterExecutor(approvalExecutor)
	nodeRegistry.MustRegister(approvalExecutor)

	// Set the registry on workflow executor so it can execute individual nodes
	workflowExecutor.SetRegistry(nodeRegistry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("failed to start worker service: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", slog.String("signal", sig.String()))
		cancel()
		if err := svc.Stop(); err != nil {
			logger.Error("failed to stop worker service", slog.String("error", err.Error()))
		}
	}()

	// Start HTTP Server for Health Checks
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

	logger.Info("worker pool started",
		slog.String("task_queue", *taskQueue),
		slog.String("matching_addr", *matchingAddr),
		slog.Int("num_workers", *numWorkers),
	)

	<-ctx.Done()
	logger.Info("worker service stopped")
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
