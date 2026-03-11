-- LinkFlow Execution Engine Initial Migration Rollback

DROP TRIGGER IF EXISTS tr_namespaces_updated_at ON namespaces;
DROP FUNCTION IF EXISTS update_updated_at();

DROP TABLE IF EXISTS task_queues;
DROP TABLE IF EXISTS visibility;
DROP TABLE IF EXISTS timers;
DROP TABLE IF EXISTS activity_tasks;
DROP TABLE IF EXISTS mutable_state;
DROP TABLE IF EXISTS history_events;
DROP TABLE IF EXISTS executions;
DROP TABLE IF EXISTS namespaces;

DROP EXTENSION IF EXISTS "pg_trgm";
DROP EXTENSION IF EXISTS "uuid-ossp";
