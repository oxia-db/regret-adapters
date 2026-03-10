FROM golang:1.24 AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /okk-coordinator ./cmd/main.go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /okk-coordinator /okk-coordinator
ENTRYPOINT ["/okk-coordinator"]
