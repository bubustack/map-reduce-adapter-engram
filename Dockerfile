# --- Build Stage ---
FROM golang:1.26-bookworm AS builder

WORKDIR /src

# Copy source code.
COPY . .

RUN go mod download

# Build the binary.
RUN CGO_ENABLED=0 GOOS=linux go build -o map-reduce-adapter-engram .

# --- Final Stage ---
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /src/map-reduce-adapter-engram /map-reduce-adapter-engram

ENTRYPOINT ["/map-reduce-adapter-engram"]
