# Stage 1: Build static binary
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary without CGO and strip debug symbols
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/aqueduct-broker ./cmd/broker

# Stage 2: Minimal non-root runtime image
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /etc/aqueduct

# Copy compiled binary and default config
COPY --from=builder /bin/aqueduct-broker /bin/aqueduct-broker
COPY config.yaml /etc/aqueduct/config.yaml

# Run as non-root user
USER 65532:65532

# Expose QUIC UDP port and HTTP Prometheus metrics port
EXPOSE 4242/udp
EXPOSE 9090/tcp

ENTRYPOINT ["/bin/aqueduct-broker", "-config", "/etc/aqueduct/config.yaml"]
