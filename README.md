# nats-bridge-core

Shared plumbing for the Python NATS sidecar bridges — `knx-`, `dyson-`,
`miele-` and `midea-nats-bridge`.

## What is in here

Only what was identical across all four bridges, measured rather than guessed:

| Module | Contents |
| --- | --- |
| `logging_setup` | JSON formatter, stdout-failure-tracking handler, log-emit watchdog |
| `metrics` | the `/metrics` and `/healthz` HTTP server |
| `publisher` | JetStream publish with ack, retry, ordered queue, reconnect, drain on shutdown |
| `tracing` | OTLP tracer provider, W3C trace context over NATS headers, producer/consumer spans |
| `config` | `NatsSettings` — NATS connection, auth precedence, observability fields |

## What is deliberately not in here

No base class that drives the loop. `connect()` / `poll()` / `normalize()` differ
in every bridge — dyson runs off a paho thread, miele consumes HTTP SSE, midea
polls over LAN, knx is bidirectional — and that is exactly the part a shared
framework would take ownership of and get wrong.

Device counters, device settings and the `_amain` wiring stay in each bridge.

## Metrics labelling

Bridges disagree on labels: some count per device and kind, knx has a single
undifferentiated counter. So `Publisher` does not touch counters directly. It
takes a `PublisherMetrics` implementation and hands back the opaque `ctx` it
was given at `enqueue()` time:

```python
class Metrics:  # in the bridge
    def set_connected(self, connected: bool) -> None:
        self.nats_connected.set(1 if connected else 0)

    def count_published(self, ctx: object) -> None:
        device, kind = cast(tuple[str, str], ctx)
        self.messages_published.labels(device=device, kind=kind).inc()

    def count_error(self, ctx: object, reason: str) -> None:
        device, _ = cast(tuple[str, str], ctx)
        self.publish_errors.labels(device=device, reason=reason).inc()
```

A bridge that needs no labels can ignore `ctx` entirely.

## Tracing

`tracing.configure(settings, service_name="knx-nats-bridge")` installs an
OTLP/HTTP tracer provider when `TRACING_ENDPOINT` is set to the collector base
URL (for example `http://alloy-alloy-receiver.alloy.svc.cluster.local:4318`);
without it the OpenTelemetry API stays a no-op. `TRACING_SAMPLING_RATIO`
(default 0.1) is the root sampler, child spans follow their parent. Call
`tracing.shutdown()` on exit to flush.

`Publisher.enqueue()` captures the caller's trace context, `publish()` runs in a
producer span and sends `traceparent` as a NATS header, and `subscribe_core()`
handles every delivery in a consumer span joined to the trace in the headers. A
bridge with its own subscription wraps its handler in
`tracing.consumer_span(msg, subject)`.

Redpanda Connect samples by trace ID rather than by the parent's flag, so keep
the ratio equal on both sides to get whole traces.

## Install

Installed from a git tag, not PyPI — the bridges publish container images, not
packages, so there is no PyPI release path to reuse:

```toml
dependencies = [
    "nats-bridge-core @ git+https://github.com/alexander-zimmermann/nats-bridge-core@v0.1.0",
]
```

The Docker builder stage needs `git` for that to resolve.
