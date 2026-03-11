# LinkFlow Engine - Multi-stage Go Dockerfile
# Builds any service via SERVICE build arg

# ============================================
# Build Stage
# ============================================
FROM golang:1.24-alpine AS builder

ARG SERVICE=frontend
ARG VERSION=dev

RUN apk add --no-cache git ca-certificates tzdata curl

# Install buf CLI for protobuf generation
RUN curl -sSL https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-$(uname -m) -o /usr/local/bin/buf && \
    chmod +x /usr/local/bin/buf

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Generate protobuf code
RUN buf generate

# Build the service (static binary, stripped)
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -ldflags "-X main.version=${VERSION} -s -w" \
    -o /app/bin/service \
    ./cmd/${SERVICE}

# ============================================
# Runtime Stage
# ============================================
FROM alpine:3.19

ARG SERVICE=frontend

RUN apk add --no-cache ca-certificates tzdata wget

# Create non-root user
RUN addgroup -g 1000 linkflow && \
    adduser -u 1000 -G linkflow -s /bin/sh -D linkflow

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/service /app/service

# Copy configs if needed
COPY --from=builder /app/configs /app/configs

# Set ownership
RUN chown -R linkflow:linkflow /app

USER linkflow

EXPOSE 8080 9090

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/service"]
