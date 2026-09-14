# nats-bridge-core

Shared plumbing for the Python NATS sidecar bridges — `knx-`, `dyson-`,
`miele-` and `midea-nats-bridge`.

## What is in here

Only what was identical across all four bridges, measured rather than guessed,
plus plumbing every bridge would otherwise copy verbatim (tracing, the KNX
descriptor):

| Module | Contents |
| --- | --- |
| `logging_setup` | JSON formatter, stdout-failure-tracking handler, log-emit watchdog |
| `metrics` | the `/metrics` and `/healthz` HTTP server |
| `publisher` | JetStream publish with ack, retry, ordered queue, reconnect, drain on shutdown |
| `tracing` | OTLP tracer provider, W3C trace context over NATS headers, producer/consumer spans |
| `config` | `NatsSettings` — NATS connection, auth precedence, observability fields |
| `knx_descriptor` | schema and loader for the `knx.yaml` a bridge ships: field → datapoint, DPT, writer behaviour |

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
`tracing.consumer_span(msg)`.

Redpanda Connect samples by trace ID rather than by the parent's flag, so keep
the ratio equal on both sides to get whole traces.

## KNX descriptor

A sidecar whose payloads end up on the KNX bus ships `knx.yaml` at its package
root. It describes the product family only — which published field is meant
for which datapoint, with which DPT, and how the writer should treat it. Group
addresses and device names are bound in lares, which generates the writer rules
from descriptor plus binding; a field an appliance lacks is simply left out
there.

```yaml
subjects:            # keyed by the subject suffix after the device segment
  state:
    fields:          # keyed by the payload field; the writer reads `$.<field>`
      phase:
        datapoint: Programm-Phase   # last segment(s) of the group-address name
        dpt: "5.010"                # main.sub, three-digit sub as in the catalog
        seed_on_start: true         # optional, default false
        min_delta: 0                # optional, ≥ 0
      remaining_minutes:
        datapoint: Restzeit
        dpt: "7.006"
        min_delta: 1
        min_delta_pct: 5            # optional, ≥ 0
  environment:
    fields:
      temperature_c:
        datapoint: Ist-Temperatur
        dpt: "9.001"
```

`knx_descriptor.load_package("miele_nats_bridge")` returns the typed
`Descriptor` (`subjects[suffix].fields[name]` with `datapoint`, `dpt`,
`payload_path` and the behaviour keys); `knx_descriptor.load(path)` reads a
file. Unknown keys, a missing `datapoint` or `dpt`, a DPT outside `main.sub`,
negative deltas, duplicate keys and unquoted YAML words (`on`, `yes`) raise
`DescriptorError` listing every offending field — a bridge's CI loads its own
descriptor so a typo fails there, not in the generator. Subject suffixes are
single NATS tokens, field names JSON identifiers, datapoints dot-separated
segments without whitespace; at least one subject with at least one field.
Whether a DPT exists is checked downstream against the catalog.

## Install

Installed from a git tag, not PyPI — the bridges publish container images, not
packages, so there is no PyPI release path to reuse:

```toml
dependencies = [
    "nats-bridge-core @ git+https://github.com/alexander-zimmermann/nats-bridge-core@v0.1.0",
]
```

The Docker builder stage needs `git` for that to resolve.
