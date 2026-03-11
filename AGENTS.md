# LinkFlow Engine Assistant Guide

This file contains instructions for AI assistants working on the LinkFlow Execution Engine (Go).

## Context

The Engine is the **Execution Plane** of LinkFlow. It handles:
- High-performance workflow execution
- Task scheduling and dispatching
- Worker management
- State persistence (Event Sourcing)

## Tech Stack

- **Language**: Go 1.24
- **Communication**: gRPC + Protobuf
- **Database**: PostgreSQL 16
- **Queue**: Redis Streams

## Key Locations

| Service | Directory |
|---------|-----------|
| **Frontend** | `cmd/frontend/`, `internal/frontend/` |
| **History** | `cmd/history/`, `internal/history/` |
| **Matching** | `cmd/matching/`, `internal/matching/` |
| **Worker** | `cmd/worker/`, `internal/worker/` |
| **Proto Defs** | `api/proto/` |

## Documentation

- **Architecture**: `../../docs/02-architecture/03-execution-plane.md`
- **Deployment**: `../../docs/05-deployment/`

## Development Rules

1.  **Context**: Always pass `context.Context` as the first argument.
2.  **Errors**: Return errors, do not panic. Use `fmt.Errorf` with wrapping (`%w`).
3.  **Concurrency**: Use channels and wait groups carefully. Watch out for goroutine leaks.
4.  **Testing**: Write table-driven tests. Use `t.Parallel()` where appropriate.
5.  **Linting**: Run `golangci-lint run` before committing.
