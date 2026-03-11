package metrics

import "time"

// ServiceMetrics provides common metrics for LinkFlow services.
type ServiceMetrics struct {
	registry *Registry
	service  string
}

// NewServiceMetrics creates a new service metrics collector.
func NewServiceMetrics(registry *Registry, service string) *ServiceMetrics {
	if registry == nil {
		registry = DefaultRegistry
	}
	return &ServiceMetrics{
		registry: registry,
		service:  service,
	}
}

// --- Request Metrics ---

// RequestStarted increments the request counter.
func (m *ServiceMetrics) RequestStarted(method, taskQueue string) {
	m.registry.Counter("linkflow_requests_total", Labels{
		"service":    m.service,
		"method":     method,
		"task_queue": taskQueue,
	}).Inc()
}

// RequestCompleted records a completed request.
func (m *ServiceMetrics) RequestCompleted(method, taskQueue, status string, duration time.Duration) {
	m.registry.Counter("linkflow_requests_completed_total", Labels{
		"service":    m.service,
		"method":     method,
		"task_queue": taskQueue,
		"status":     status,
	}).Inc()

	m.registry.Histogram("linkflow_request_duration_ms", Labels{
		"service":    m.service,
		"method":     method,
		"task_queue": taskQueue,
	}, nil).ObserveDuration(duration)
}

// RequestFailed records a failed request.
func (m *ServiceMetrics) RequestFailed(method, taskQueue, errorType string) {
	m.registry.Counter("linkflow_requests_failed_total", Labels{
		"service":    m.service,
		"method":     method,
		"task_queue": taskQueue,
		"error_type": errorType,
	}).Inc()
}

// --- Execution Metrics ---

// ExecutionStarted records a new execution.
func (m *ServiceMetrics) ExecutionStarted(namespace, workflowType string) {
	m.registry.Counter("linkflow_executions_started_total", Labels{
		"service":       m.service,
		"namespace":     namespace,
		"workflow_type": workflowType,
	}).Inc()

	m.registry.Gauge("linkflow_executions_active", Labels{
		"service":       m.service,
		"namespace":     namespace,
		"workflow_type": workflowType,
	}).Inc()
}

// ExecutionCompleted records a completed execution.
func (m *ServiceMetrics) ExecutionCompleted(namespace, workflowType, status string, duration time.Duration) {
	m.registry.Counter("linkflow_executions_completed_total", Labels{
		"service":       m.service,
		"namespace":     namespace,
		"workflow_type": workflowType,
		"status":        status,
	}).Inc()

	m.registry.Gauge("linkflow_executions_active", Labels{
		"service":       m.service,
		"namespace":     namespace,
		"workflow_type": workflowType,
	}).Dec()

	m.registry.Histogram("linkflow_execution_duration_ms", Labels{
		"service":       m.service,
		"namespace":     namespace,
		"workflow_type": workflowType,
	}, nil).ObserveDuration(duration)
}

// --- Task Metrics ---

// TaskScheduled records a scheduled task.
func (m *ServiceMetrics) TaskScheduled(taskQueue, taskType string) {
	m.registry.Counter("linkflow_tasks_scheduled_total", Labels{
		"service":    m.service,
		"task_queue": taskQueue,
		"task_type":  taskType,
	}).Inc()
}

// TaskStarted records a started task.
func (m *ServiceMetrics) TaskStarted(taskQueue, taskType string) {
	m.registry.Counter("linkflow_tasks_started_total", Labels{
		"service":    m.service,
		"task_queue": taskQueue,
		"task_type":  taskType,
	}).Inc()

	m.registry.Gauge("linkflow_tasks_active", Labels{
		"service":    m.service,
		"task_queue": taskQueue,
		"task_type":  taskType,
	}).Inc()
}

// TaskCompleted records a completed task.
func (m *ServiceMetrics) TaskCompleted(taskQueue, taskType, status string, duration time.Duration) {
	m.registry.Counter("linkflow_tasks_completed_total", Labels{
		"service":    m.service,
		"task_queue": taskQueue,
		"task_type":  taskType,
		"status":     status,
	}).Inc()

	m.registry.Gauge("linkflow_tasks_active", Labels{
		"service":    m.service,
		"task_queue": taskQueue,
		"task_type":  taskType,
	}).Dec()

	m.registry.Histogram("linkflow_task_duration_ms", Labels{
		"service":    m.service,
		"task_queue": taskQueue,
		"task_type":  taskType,
	}, nil).ObserveDuration(duration)
}

// TaskQueueDepth records the depth of a task queue.
func (m *ServiceMetrics) TaskQueueDepth(taskQueue string, depth int64) {
	m.registry.Gauge("linkflow_task_queue_depth", Labels{
		"service":    m.service,
		"task_queue": taskQueue,
	}).Set(float64(depth))
}

// --- Timer Metrics ---

// TimerScheduled records a scheduled timer.
func (m *ServiceMetrics) TimerScheduled(namespace string) {
	m.registry.Counter("linkflow_timers_scheduled_total", Labels{
		"service":   m.service,
		"namespace": namespace,
	}).Inc()
}

// TimerFired records a fired timer.
func (m *ServiceMetrics) TimerFired(namespace string, delay time.Duration) {
	m.registry.Counter("linkflow_timers_fired_total", Labels{
		"service":   m.service,
		"namespace": namespace,
	}).Inc()

	m.registry.Histogram("linkflow_timer_delay_ms", Labels{
		"service":   m.service,
		"namespace": namespace,
	}, nil).ObserveDuration(delay)
}

// TimerCanceled records a canceled timer.
func (m *ServiceMetrics) TimerCanceled(namespace string) {
	m.registry.Counter("linkflow_timers_canceled_total", Labels{
		"service":   m.service,
		"namespace": namespace,
	}).Inc()
}

// --- Node Metrics ---

// NodeExecuted records a node execution.
func (m *ServiceMetrics) NodeExecuted(nodeType, status string, duration time.Duration) {
	m.registry.Counter("linkflow_nodes_executed_total", Labels{
		"service":   m.service,
		"node_type": nodeType,
		"status":    status,
	}).Inc()

	m.registry.Histogram("linkflow_node_duration_ms", Labels{
		"service":   m.service,
		"node_type": nodeType,
	}, nil).ObserveDuration(duration)
}

// --- History Metrics ---

// HistoryEventRecorded records a history event.
func (m *ServiceMetrics) HistoryEventRecorded(eventType string) {
	m.registry.Counter("linkflow_history_events_total", Labels{
		"service":    m.service,
		"event_type": eventType,
	}).Inc()
}

// HistorySize records the size of a history.
func (m *ServiceMetrics) HistorySize(eventCount int64) {
	m.registry.Histogram("linkflow_history_size_events", Labels{
		"service": m.service,
	}, []float64{10, 50, 100, 500, 1000, 5000, 10000}).Observe(float64(eventCount))
}

// --- Cache Metrics ---

// CacheHit records a cache hit.
func (m *ServiceMetrics) CacheHit(cacheType string) {
	m.registry.Counter("linkflow_cache_hits_total", Labels{
		"service":    m.service,
		"cache_type": cacheType,
	}).Inc()
}

// CacheMiss records a cache miss.
func (m *ServiceMetrics) CacheMiss(cacheType string) {
	m.registry.Counter("linkflow_cache_misses_total", Labels{
		"service":    m.service,
		"cache_type": cacheType,
	}).Inc()
}

// --- Shard Metrics ---

// ShardAcquired records that a shard was acquired.
func (m *ServiceMetrics) ShardAcquired(shardID int32) {
	m.registry.Counter("linkflow_shards_acquired_total", Labels{
		"service":  m.service,
		"shard_id": intToStr(int64(shardID)),
	}).Inc()
}

// ShardReleased records that a shard was released.
func (m *ServiceMetrics) ShardReleased(shardID int32) {
	m.registry.Counter("linkflow_shards_released_total", Labels{
		"service":  m.service,
		"shard_id": intToStr(int64(shardID)),
	}).Inc()
}

// ShardsOwned sets the number of shards owned.
func (m *ServiceMetrics) ShardsOwned(count int) {
	m.registry.Gauge("linkflow_shards_owned", Labels{
		"service": m.service,
	}).Set(float64(count))
}

// --- gRPC Metrics ---

// GRPCRequestReceived records a received gRPC request.
func (m *ServiceMetrics) GRPCRequestReceived(method string) {
	m.registry.Counter("linkflow_grpc_requests_received_total", Labels{
		"service": m.service,
		"method":  method,
	}).Inc()
}

// GRPCRequestHandled records a handled gRPC request.
func (m *ServiceMetrics) GRPCRequestHandled(method, code string, duration time.Duration) {
	m.registry.Counter("linkflow_grpc_requests_handled_total", Labels{
		"service": m.service,
		"method":  method,
		"code":    code,
	}).Inc()

	m.registry.Histogram("linkflow_grpc_request_duration_ms", Labels{
		"service": m.service,
		"method":  method,
	}, nil).ObserveDuration(duration)
}

// --- Database Metrics ---

// DBQueryExecuted records a database query.
func (m *ServiceMetrics) DBQueryExecuted(query, status string, duration time.Duration) {
	m.registry.Counter("linkflow_db_queries_total", Labels{
		"service": m.service,
		"query":   query,
		"status":  status,
	}).Inc()

	m.registry.Histogram("linkflow_db_query_duration_ms", Labels{
		"service": m.service,
		"query":   query,
	}, nil).ObserveDuration(duration)
}

// DBConnectionPoolSize records the connection pool size.
func (m *ServiceMetrics) DBConnectionPoolSize(active, idle int) {
	m.registry.Gauge("linkflow_db_connections_active", Labels{
		"service": m.service,
	}).Set(float64(active))

	m.registry.Gauge("linkflow_db_connections_idle", Labels{
		"service": m.service,
	}).Set(float64(idle))
}
