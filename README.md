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

## Install

Installed from a git tag, not PyPI — the bridges publish container images, not
packages, so there is no PyPI release path to reuse:

```toml
dependencies = [
    "nats-bridge-core @ git+https://github.com/alexander-zimmermann/nats-bridge-core@v0.1.0",
]
```

The Docker builder stage needs `git` for that to resolve.
