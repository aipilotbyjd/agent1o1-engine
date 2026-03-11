# LinkFlow Execution Engine Makefile

.PHONY: all build test lint proto generate docker clean tools help
.PHONY: build-frontend build-history build-matching build-worker build-timer build-visibility build-edge build-control-plane
.PHONY: docker-frontend docker-history docker-matching docker-worker docker-timer docker-visibility docker-edge docker-control-plane
.PHONY: run-frontend run-history run-matching run-worker run-timer run-visibility run-edge run-control-plane
.PHONY: dev migrate-up migrate-down test-cover

SERVICES := frontend history matching worker timer visibility edge control-plane
DOCKER_REGISTRY ?= linkflow
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

all: build

# Build targets
build: $(addprefix build-,$(SERVICES))

build-%:
	@echo "Building $*..."
	go build $(LDFLAGS) -o bin/$* ./cmd/$*

# Test targets
test:
	go test -race -v ./...

test-cover:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Lint target
lint:
	golangci-lint run ./...

# Proto generation
proto:
	buf generate

# Go generate
generate:
	go generate ./...

# Docker targets
docker: $(addprefix docker-,$(SERVICES))

docker-%:
	@echo "Building Docker image for $*..."
	docker build --build-arg SERVICE=$* -t $(DOCKER_REGISTRY)/$*:$(VERSION) .

# Run targets
run-%:
	go run ./cmd/$*

# Development with hot reload
dev:
	air

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf coverage.out coverage.html
	rm -rf tmp/

# Install dev tools
tools:
	@echo "Installing development tools..."
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/air-verse/air@latest
	go install go.uber.org/mock/mockgen@latest
	@echo "Tools installed successfully"

# Database migrations
migrate-up:
	@echo "Running migrations up..."
	go run ./scripts/migrate/main.go up

migrate-down:
	@echo "Running migrations down..."
	go run ./scripts/migrate/main.go down

# Help
help:
	@echo "LinkFlow Execution Engine - Available Commands"
	@echo ""
	@echo "Build:"
	@echo "  make build              - Build all services"
	@echo "  make build-{service}    - Build individual service"
	@echo "                            Services: $(SERVICES)"
	@echo ""
	@echo "Test:"
	@echo "  make test               - Run all tests with race detector"
	@echo "  make test-cover         - Run tests with coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint               - Run golangci-lint"
	@echo "  make generate           - Run go generate"
	@echo ""
	@echo "Proto:"
	@echo "  make proto              - Generate protobuf code using buf"
	@echo ""
	@echo "Docker:"
	@echo "  make docker             - Build all Docker images"
	@echo "  make docker-{service}   - Build individual Docker image"
	@echo ""
	@echo "Run:"
	@echo "  make run-{service}      - Run individual service locally"
	@echo "  make dev                - Run with hot reload using air"
	@echo ""
	@echo "Database:"
	@echo "  make migrate-up         - Run database migrations up"
	@echo "  make migrate-down       - Run database migrations down"
	@echo ""
	@echo "Other:"
	@echo "  make tools              - Install required dev tools"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make help               - Show this help message"
