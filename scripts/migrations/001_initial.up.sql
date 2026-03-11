-- LinkFlow Execution Engine Initial Migration (Consolidated)
-- PostgreSQL 14+

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =============================================================================
-- NAMESPACES
-- =============================================================================
CREATE TABLE IF NOT EXISTS namespaces (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    owner_email VARCHAR(255),
    retention_days INTEGER DEFAULT 30,
    history_size_limit_mb INTEGER DEFAULT 50,
    workflow_execution_ttl_seconds BIGINT,
    allowed_clusters TEXT[],
    default_cluster VARCHAR(255),
    search_attributes JSONB,
    archival_enabled BOOLEAN DEFAULT FALSE,
    archival_uri TEXT,
    data JSONB, -- Kept for backward compat if needed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_namespaces_name ON namespaces (name);

-- =============================================================================
-- EXECUTIONS (main workflow execution table)
-- =============================================================================
CREATE TABLE IF NOT EXISTS executions (
    shard_id            INTEGER NOT NULL,
    namespace_id        VARCHAR(255) NOT NULL REFERENCES namespaces(id),
    workflow_id         VARCHAR(255) NOT NULL,
    run_id              UUID NOT NULL,
    parent_workflow_id  VARCHAR(255),
    parent_run_id       UUID,
    workflow_type_name  VARCHAR(255) NOT NULL,
    status              SMALLINT NOT NULL,
    start_time          TIMESTAMPTZ NOT NULL,
    close_time          TIMESTAMPTZ,
    execution_timeout   BIGINT,
    run_timeout         BIGINT,
    task_queue          VARCHAR(255) NOT NULL,
    memo                BYTEA,
    search_attributes   JSONB,
    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

CREATE INDEX idx_executions_workflow_id ON executions (namespace_id, workflow_id);
CREATE INDEX idx_executions_run_id ON executions (run_id);
CREATE INDEX idx_executions_status ON executions (namespace_id, status);
CREATE INDEX idx_executions_start_time ON executions (namespace_id, start_time DESC);
CREATE INDEX idx_executions_task_queue ON executions (namespace_id, task_queue);
CREATE INDEX idx_executions_close_time ON executions (namespace_id, close_time DESC) WHERE close_time IS NOT NULL;

-- =============================================================================
-- HISTORY_EVENTS (event sourcing)
-- =============================================================================
CREATE TABLE IF NOT EXISTS history_events (
    shard_id        INTEGER NOT NULL,
    namespace_id    VARCHAR(255) NOT NULL,
    workflow_id     VARCHAR(255) NOT NULL,
    run_id          UUID NOT NULL,
    event_id        BIGINT NOT NULL,
    event_type      SMALLINT NOT NULL,
    version         BIGINT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    data            BYTEA NOT NULL,
    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, event_id)
);

CREATE INDEX idx_history_events_run ON history_events (namespace_id, workflow_id, run_id);
CREATE INDEX idx_history_events_version ON history_events (shard_id, namespace_id, workflow_id, run_id, version);

-- =============================================================================
-- MUTABLE_STATE (current execution state)
-- =============================================================================
CREATE TABLE IF NOT EXISTS mutable_state (
    shard_id        INTEGER NOT NULL,
    namespace_id    VARCHAR(255) NOT NULL,
    workflow_id     VARCHAR(255) NOT NULL,
    run_id          UUID NOT NULL,
    state           BYTEA NOT NULL,
    next_event_id   BIGINT NOT NULL,
    db_version      BIGINT NOT NULL,
    checksum        BYTEA,
    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

-- =============================================================================
-- ACTIVITY_TASKS
-- =============================================================================
CREATE TABLE IF NOT EXISTS activity_tasks (
    shard_id            INTEGER NOT NULL,
    namespace_id        VARCHAR(255) NOT NULL,
    task_queue          VARCHAR(255) NOT NULL,
    task_id             UUID PRIMARY KEY,
    workflow_id         VARCHAR(255) NOT NULL,
    run_id              UUID NOT NULL,
    scheduled_event_id  BIGINT NOT NULL,
    activity_id         VARCHAR(255) NOT NULL,
    activity_type       VARCHAR(255) NOT NULL,
    input               BYTEA,
    schedule_time       TIMESTAMPTZ NOT NULL,
    attempt             INTEGER DEFAULT 1,
    expiry_time         TIMESTAMPTZ
);

CREATE INDEX idx_activity_tasks_poll ON activity_tasks (namespace_id, task_queue, schedule_time);
CREATE INDEX idx_activity_tasks_workflow ON activity_tasks (namespace_id, workflow_id, run_id);
CREATE INDEX idx_activity_tasks_expiry ON activity_tasks (expiry_time) WHERE expiry_time IS NOT NULL;

-- =============================================================================
-- TIMERS
-- =============================================================================
CREATE TABLE IF NOT EXISTS timers (
    shard_id        INTEGER NOT NULL,
    namespace_id    VARCHAR(255) NOT NULL,
    workflow_id     VARCHAR(255) NOT NULL,
    run_id          UUID NOT NULL,
    timer_id        VARCHAR(255) NOT NULL,
    fire_time       TIMESTAMPTZ NOT NULL,
    status          SMALLINT DEFAULT 0,
    version         BIGINT NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fired_at        TIMESTAMPTZ,
    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, timer_id)
);

CREATE INDEX idx_timers_fire_time ON timers (shard_id, fire_time, status);
CREATE INDEX idx_timers_workflow ON timers (namespace_id, workflow_id, run_id);

-- =============================================================================
-- VISIBILITY (for search/list)
-- =============================================================================
CREATE TABLE IF NOT EXISTS visibility (
    namespace_id        VARCHAR(255) NOT NULL,
    workflow_id         VARCHAR(255) NOT NULL,
    run_id              UUID NOT NULL,
    workflow_type_name  VARCHAR(255),
    status              SMALLINT,
    start_time          TIMESTAMPTZ,
    close_time          TIMESTAMPTZ,
    execution_time      TIMESTAMPTZ,
    memo                BYTEA,
    search_attributes   JSONB,
    task_queue          VARCHAR(255),
    parent_workflow_id  VARCHAR(255),
    parent_run_id       VARCHAR(255),
    PRIMARY KEY (namespace_id, workflow_id, run_id)
);

CREATE INDEX idx_visibility_status ON visibility (namespace_id, status);
CREATE INDEX idx_visibility_start_time ON visibility (namespace_id, start_time DESC);
CREATE INDEX idx_visibility_close_time ON visibility (namespace_id, close_time DESC) WHERE close_time IS NOT NULL;
CREATE INDEX idx_visibility_workflow_type ON visibility (namespace_id, workflow_type_name);
CREATE INDEX idx_visibility_search_attrs ON visibility USING GIN (search_attributes);

-- =============================================================================
-- TASK_QUEUES
-- =============================================================================
CREATE TABLE IF NOT EXISTS task_queues (
    shard_id            INTEGER NOT NULL,
    namespace_id        VARCHAR(255) NOT NULL,
    name                VARCHAR(255) NOT NULL,
    kind                SMALLINT NOT NULL,
    last_update_time    TIMESTAMPTZ,
    PRIMARY KEY (shard_id, namespace_id, name, kind)
);

CREATE INDEX idx_task_queues_namespace ON task_queues (namespace_id, name);

-- =============================================================================
-- CLUSTERS
-- =============================================================================
CREATE TABLE IF NOT EXISTS clusters (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    region VARCHAR(100),
    endpoint VARCHAR(255),
    status SMALLINT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMPTZ,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- SERVICE_INSTANCES
-- =============================================================================
CREATE TABLE IF NOT EXISTS service_instances (
    id VARCHAR(255) PRIMARY KEY,
    service VARCHAR(100) NOT NULL,
    address VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    metadata JSONB,
    health SMALLINT NOT NULL DEFAULT 0,
    last_check TIMESTAMPTZ,
    version VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_service_instances_service ON service_instances (service, health);

-- =============================================================================
-- CREDENTIALS
-- =============================================================================
CREATE TABLE IF NOT EXISTS credentials (
    id VARCHAR(255) PRIMARY KEY,
    namespace_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    encrypted_value TEXT NOT NULL,
    credential_type VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (namespace_id, name)
);

CREATE INDEX idx_credentials_namespace ON credentials (namespace_id);

-- =============================================================================
-- TRIGGERS
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_namespaces_updated_at
    BEFORE UPDATE ON namespaces
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();
