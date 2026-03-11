# LinkFlow Engine (Go Microservices)

The high-performance workflow execution engine for LinkFlow, built with Go and gRPC.

## Architecture

A microservices-based distributed system:

| Service | Port (HTTP/gRPC) | Description |
|---|---|---|
| **frontend** | 8080 / 9090 | API Gateway & Job Claiming (routes to others) |
| **history** | 8081 / 7234 | Workflow Event Sourcing & State |
| **matching** | 8082 / 7235 | High-Performance Task Queue Routing |
| **worker** | 8083 / - | Executes Nodes (HTTP, AI, DB, etc.) |
| **timer** | 8084 / 7238 | Durable Timers & Scheduled Jobs |
| **visibility** | 8085 / 7237 | Search & Analytics (Elasticsearch optional) |
| **edge** | 8086 / - | Optional Edge Agent for remote execution |
| **control-plane** | 8087 / 7239 | Optional Cluster Management |

## Quick Start (Docker)

### 1. Configure Environment
Go Engine uses the root `.env` or direct environment variables. The Docker Compose file automatically reads from the root `.env`.

### 2. Run via Root Makefile (Recommended)
From the project root:

```bash
make engine-up
```

### 3. Run Manually
Start the infrastructure first, then:

```bash
cd ../../ && docker-compose -f apps/engine/docker-compose.yml up -d
```

## Scaling

Services are stateless (except History, which shards state) and can be scaled horizontally.

### Scale Workers
To increase execution throughput:

```bash
# Via Makefile
make scale-workers N=5

# Via Docker Compose
docker-compose up -d --scale worker=5
```

## Development

### Run Tests
```bash
# Unit tests
go test ./...

# Integration tests (requires running infra)
POSTGRES_DSN=... go test ./...
```

### Build & Lint
```bash
# Build all services
make go-build

# Lint
golangci-lint run
```

## Migrations
Database migrations are handled automatically in CI/CD or via the `migrate` service profile:

```bash
make migrate
```
