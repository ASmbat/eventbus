# eventbus

[![CI](https://github.com/ASmbat/eventbus/actions/workflows/ci.yml/badge.svg)](https://github.com/ASmbat/eventbus/actions/workflows/ci.yml)

A small in-process, topic-based event bus in Go: bounded per-topic queues,
configurable worker pools, an explicit backpressure policy, and a
token-bucket rate limiter on the publish endpoint. Built to demonstrate
concurrency patterns for high-throughput, event-driven services, rather
than a REST CRUD API.

## Why this exists

High-traffic, event-driven systems live or die by what happens when a
consumer can't keep up with producers. This project makes that trade-off
explicit instead of hiding it: a full queue either rejects the publish
(`PolicyDrop`) or blocks the publisher (`PolicyBlock`) — a real choice a
caller has to make, not something a framework quietly decides for you.

## How it's structured

- **`internal/bus`** — the core: topics, bounded queues, worker pools,
  backpressure policy. No HTTP, no vendor logic — just the concurrency
  primitive.
- **`internal/ratelimit`** — a token-bucket limiter, used to protect the
  publish endpoint from bursty producers independently of the bus's own
  per-topic backpressure.
- **`internal/metrics`** — atomic counters (published/processed/dropped/
  failed) exposed as a JSON snapshot.
- **`internal/handler`** — the HTTP surface.
- **`cmd/server`** — wires up a demo "orders" topic with a small worker
  pool and simulated processing latency/failures, plus graceful shutdown.

## API

| Method | Path                | Description                                    |
|--------|---------------------|--------------------------------------------------|
| POST   | `/publish/{topic}`  | Publish the request body as an event's payload    |
| GET    | `/metrics`          | JSON snapshot of published/processed/dropped/failed |
| GET    | `/healthz`          | Liveness check                                    |

```bash
curl -X POST localhost:8080/publish/orders -d 'order-id-42'
curl localhost:8080/metrics
```

Publish responses:
- `202 Accepted` — queued successfully
- `404 Not Found` — no subscriber for that topic
- `503 Service Unavailable` — topic's queue is full (backpressure), or the
  server is shutting down
- `429 Too Many Requests` — publish rate limit exceeded

To actually see backpressure happen, fire a burst of requests faster than
the demo "orders" topic (3 workers, ~20ms per event) can drain them:

```bash
for i in $(seq 1 100); do curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/publish/orders -d "e$i" & done; wait
```

You'll see a mix of `202` and `503` once the queue (size 20) fills.

## Graceful shutdown

On SIGINT/SIGTERM the server stops accepting HTTP requests, then calls
`bus.Close`, which:

1. rejects new publishes with `ErrClosed` (and releases publishers blocked
   under `PolicyBlock`),
2. lets workers finish every event already in the queues,
3. waits for the workers, up to a timeout.

So events that were accepted with `202` are processed, not lost.

## Benchmarks

```bash
go test ./internal/bus -run x -bench . -benchmem
```

Example run (2 vCPU, Go 1.24):

| Benchmark | ns/op | allocs/op |
|---|---|---|
| Publish, 1 worker | ~460 | 0 |
| Publish, 4 workers | ~760 | 0 |
| Publish from parallel goroutines | ~700 | 0 |
| Publish rejected (queue full, `PolicyDrop`) | ~115 | 0 |

`Publish` does no heap allocations: the `Event` is copied into the
channel by value, so publishing adds no GC pressure.

## Running locally

```bash
make run
```

## Tests

```bash
make test
```

Covers: rate limiter burst/refill/capacity cap (with a fake clock, no
sleeps), duplicate subscriptions, drop-on-full, blocking backpressure,
failed and panicking handlers, graceful `Close` (drain, timeout, releasing
blocked publishers), and the HTTP status code for each outcome. Tests are
synchronized with channels instead of `time.Sleep`, and pass under `-race`.

## Design notes / trade-offs

- **Per-topic isolation**: each topic has its own queue and worker pool, so
  a slow or failing topic can't starve others sharing the process.
- **One subscription per topic**: subscribing twice to the same topic
  returns `ErrAlreadySubscribed` instead of silently replacing the queue.
- **No retry/DLQ**: a failed handler is counted and moved on, deliberately
  — retry semantics vary a lot by use case, so it's left as a decision for
  the handler itself (e.g. publish to a `topic.failed` topic) rather than
  baked into the bus.
- **In-process only**: no persistence, no replay, no cross-process
  delivery. It shows the concurrency pattern (bounded queues, worker
  pools, backpressure) on its own, without a message broker.

## License

MIT — see [LICENSE](LICENSE).
