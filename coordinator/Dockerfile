FROM golang:1.24 AS builder

WORKDIR /build

# Copy the shared proto module
COPY go.mod go.sum ./
COPY internal/ internal/

# Copy the coordinator module
COPY coordinator/go.mod coordinator/go.sum ./coordinator/
RUN cd coordinator && go mod download

COPY coordinator/ ./coordinator/
RUN cd coordinator && CGO_ENABLED=0 go build -o /okk-coordinator ./cmd/main.go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /okk-coordinator /okk-coordinator
ENTRYPOINT ["/okk-coordinator"]
