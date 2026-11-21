# syntax=docker/dockerfile:1.7
#
# One multi-stage Dockerfile with a dedicated final image for each binary:
#   docker build --target api     -t durable-job-api .
#   docker build --target worker  -t durable-job-worker .
#   docker build --target migrate -t durable-job-migrate .
#   docker build --target apikey  -t durable-job-apikey .
#
# All binaries are built once in the builder stage; each runtime stage copies
# only the binary it needs. The final images are static, non-root and include
# CA certificates so HTTPS webhook delivery works without extra setup.

FROM golang:1.26-alpine AS builder

WORKDIR /src

# Download modules first so source-only changes reuse the dependency layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/apikey ./cmd/apikey

FROM alpine:3.21 AS runtime

RUN apk add --no-cache ca-certificates && \
    addgroup -S app && \
    adduser -S -G app app

WORKDIR /app

FROM runtime AS api

COPY --from=builder /out/api /usr/local/bin/api
USER app
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/api"]

FROM runtime AS worker

COPY --from=builder /out/worker /usr/local/bin/worker
USER app
EXPOSE 9091
ENTRYPOINT ["/usr/local/bin/worker"]

FROM runtime AS migrate

COPY --from=builder /out/migrate /usr/local/bin/migrate
USER app
ENTRYPOINT ["/usr/local/bin/migrate"]

FROM runtime AS apikey

COPY --from=builder /out/apikey /usr/local/bin/apikey
USER app
ENTRYPOINT ["/usr/local/bin/apikey"]
