# Build
FROM golang:1.24-alpine AS build
WORKDIR /src

# Dependencies first so a code change does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/edgedepth-gateway ./cmd/edgedepth-gateway

# Run
FROM alpine:3.20
# TLS roots are required: every Binance endpoint is https/wss.
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 gateway
USER gateway
COPY --from=build /out/edgedepth-gateway /usr/local/bin/edgedepth-gateway
EXPOSE 8080
ENTRYPOINT ["edgedepth-gateway"]
