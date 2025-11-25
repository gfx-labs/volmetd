FROM golang:1.23-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /volmetd ./cmd/volmetd

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /volmetd /volmetd

USER 65534:65534

ENTRYPOINT ["/volmetd"]
