-- LinkFlow Database Initialization
-- This runs automatically when PostgreSQL container starts (first time only)

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Grant permissions (in case needed)
GRANT ALL PRIVILEGES ON DATABASE linkflow TO linkflow;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS workflow;
CREATE SCHEMA IF NOT EXISTS visibility;
CREATE SCHEMA IF NOT EXISTS matching;

GRANT ALL ON SCHEMA workflow TO linkflow;
GRANT ALL ON SCHEMA visibility TO linkflow;
GRANT ALL ON SCHEMA matching TO linkflow;
GRANT ALL ON SCHEMA public TO linkflow;

-- Log
DO $$
BEGIN
    RAISE NOTICE 'LinkFlow database initialized successfully!';
END $$;
