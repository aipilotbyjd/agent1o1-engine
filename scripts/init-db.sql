-- LinkFlow database initialization script

-- Create database (run as superuser)
-- CREATE DATABASE linkflow;
-- CREATE USER linkflow WITH PASSWORD 'CHANGE_ME_IN_PRODUCTION';
-- GRANT ALL PRIVILEGES ON DATABASE linkflow TO linkflow;

\connect linkflow

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas for different services
CREATE SCHEMA IF NOT EXISTS workflow;
CREATE SCHEMA IF NOT EXISTS visibility;
CREATE SCHEMA IF NOT EXISTS matching;

-- Grant permissions
GRANT ALL ON SCHEMA workflow TO linkflow;
GRANT ALL ON SCHEMA visibility TO linkflow;
GRANT ALL ON SCHEMA matching TO linkflow;
GRANT ALL ON SCHEMA public TO linkflow;

-- Run schema
SET search_path TO workflow;
\i schema.sql
