# Multi-stage build for pico-apiserver
FROM golang:1.24.9-alpine AS builder

# Build arguments for multi-architecture support
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY pkg/ pkg/

# Build with dynamic architecture support
# Supports amd64, arm64, arm/v7, etc.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o pico-apiserver ./cmd/pico-apiserver

# Runtime image
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /workspace/pico-apiserver .

# Run as non-root user
RUN adduser -D -u 1000 apiserver
USER apiserver

EXPOSE 8080

ENTRYPOINT ["/app/pico-apiserver"]
CMD ["--port=8080", "--namespace=default", "--runtime-class-name=kuasar-vmm"]

