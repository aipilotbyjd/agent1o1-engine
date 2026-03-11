package frontend

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// HealthStatus represents a detailed health report for the engine.
type HealthStatus struct {
	Status          string                    `json:"status"`
	Version         string                    `json:"version"`
	Uptime          string                    `json:"uptime"`
	UptimeSeconds   float64                   `json:"uptime_seconds"`
	Hostname        string                    `json:"hostname"`
	GoVersion       string                    `json:"go_version"`
	NumGoroutines   int                       `json:"num_goroutines"`
	MemAllocMB      float64                   `json:"mem_alloc_mb"`
	MemSysMB        float64                   `json:"mem_sys_mb"`
	NumCPU          int                       `json:"num_cpu"`
	PartitionCount  int                       `json:"partition_count"`
	Services        map[string]*ServiceHealth `json:"services"`
	ActiveConsumers int                       `json:"active_consumers"`
	Timestamp       time.Time                 `json:"timestamp"`
}

// ServiceHealth tracks health of downstream services.
type ServiceHealth struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Latency string `json:"latency_ms,omitempty"`
}

// HealthChecker performs deep health checks on all engine dependencies.
type HealthChecker struct {
	redisClient    *redis.Client
	startTime      time.Time
	partitionCount int
	mu             sync.RWMutex
	lastResult     *HealthStatus
}

// NewHealthChecker creates a new HealthChecker.
func NewHealthChecker(redisClient *redis.Client, partitionCount int) *HealthChecker {
	return &HealthChecker{
		redisClient:    redisClient,
		startTime:      time.Now(),
		partitionCount: partitionCount,
	}
}

// Check performs a comprehensive health check.
func (h *HealthChecker) Check(ctx context.Context) *HealthStatus {
	hostname, _ := os.Hostname()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	services := make(map[string]*ServiceHealth)

	// Check Redis
	services["redis"] = h.checkRedis(ctx)

	// Compute overall status
	overallStatus := "healthy"
	for _, svc := range services {
		if svc.Status != "healthy" {
			overallStatus = "degraded"
			break
		}
	}

	status := &HealthStatus{
		Status:         overallStatus,
		Uptime:         time.Since(h.startTime).Truncate(time.Second).String(),
		UptimeSeconds:  time.Since(h.startTime).Seconds(),
		Hostname:       hostname,
		GoVersion:      runtime.Version(),
		NumGoroutines:  runtime.NumGoroutine(),
		MemAllocMB:     float64(mem.Alloc) / 1024 / 1024,
		MemSysMB:       float64(mem.Sys) / 1024 / 1024,
		NumCPU:         runtime.NumCPU(),
		PartitionCount: h.partitionCount,
		Services:       services,
		Timestamp:      time.Now().UTC(),
	}

	h.mu.Lock()
	h.lastResult = status
	h.mu.Unlock()

	return status
}

func (h *HealthChecker) checkRedis(ctx context.Context) *ServiceHealth {
	svc := &ServiceHealth{Name: "redis"}

	start := time.Now()
	err := h.redisClient.Ping(ctx).Err()
	latency := time.Since(start)

	if err != nil {
		svc.Status = "unhealthy"
		return svc
	}

	svc.Status = "healthy"
	svc.Latency = fmt.Sprintf("%.2f", float64(latency.Microseconds())/1000)

	return svc
}

// DLQStats returns info about the Dead Letter Queue.
func (h *HealthChecker) DLQStats(ctx context.Context, dlqKey string) map[string]interface{} {
	length, err := h.redisClient.XLen(ctx, dlqKey).Result()
	if err != nil {
		return map[string]interface{}{
			"stream": dlqKey,
			"length": -1,
			"error":  err.Error(),
		}
	}

	return map[string]interface{}{
		"stream": dlqKey,
		"length": length,
	}
}

// PartitionStats returns per-partition queue lengths.
func (h *HealthChecker) PartitionStats(ctx context.Context) []map[string]interface{} {
	stats := make([]map[string]interface{}, 0, h.partitionCount)

	for i := 0; i < h.partitionCount; i++ {
		streamKey := fmt.Sprintf("linkflow:jobs:partition:%d", i)
		length, err := h.redisClient.XLen(ctx, streamKey).Result()
		if err != nil {
			stats = append(stats, map[string]interface{}{
				"partition": i,
				"stream":    streamKey,
				"length":    -1,
				"error":     err.Error(),
			})
			continue
		}

		stats = append(stats, map[string]interface{}{
			"partition": i,
			"stream":    streamKey,
			"length":    length,
		})
	}

	return stats
}
