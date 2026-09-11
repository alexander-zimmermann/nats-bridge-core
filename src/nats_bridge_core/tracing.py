"""OpenTelemetry wiring shared by the bridges: provider, W3C context over NATS headers, spans."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from nats.aio.msg import Msg
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.propagators.textmap import Getter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Span, SpanKind

from .config import NatsSettings

_tracer = trace.get_tracer("nats_bridge_core")


def traces_url(endpoint: str) -> str:
    """OTLP/HTTP traces path for a collector base URL; the exporter wants the full URL."""
    return endpoint.rstrip("/") + "/v1/traces"


def build_provider(endpoint: str, sampling_ratio: float, service_name: str) -> TracerProvider:
    """Provider exporting to the collector.

    The root sampler decides by trace ID, like Redpanda Connect does, so both
    sides keep the same traces when they run the same ratio.
    """
    provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name}),
        sampler=ParentBased(TraceIdRatioBased(sampling_ratio)),
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_url(endpoint))))
    return provider


def configure(settings: NatsSettings, service_name: str) -> None:
    """Install the global provider; without tracing_endpoint the API stays a no-op."""
    if settings.tracing_endpoint is None:
        return
    trace.set_tracer_provider(
        build_provider(settings.tracing_endpoint, settings.tracing_sampling_ratio, service_name)
    )


def shutdown() -> None:
    """Flush queued spans on exit; a no-op when tracing is off."""
    provider = trace.get_tracer_provider()
    if isinstance(provider, TracerProvider):
        provider.shutdown()


class _HeaderGetter(Getter[Mapping[str, str]]):
    """NATS header names are case-insensitive on the wire and nats-py keeps them as sent."""

    def get(self, carrier: Mapping[str, str], key: str) -> list[str] | None:
        values = [v for k, v in carrier.items() if k.lower() == key]
        return values or None

    def keys(self, carrier: Mapping[str, str]) -> list[str]:
        return [k.lower() for k in carrier]


_header_getter = _HeaderGetter()


def context_from_headers(headers: Mapping[str, str] | None) -> Context:
    """Trace context carried in NATS headers; the current context when there is none."""
    return extract(headers or {}, getter=_header_getter)


def outbound_headers() -> dict[str, str]:
    """W3C trace context of the active span as NATS headers; empty without one."""
    headers: dict[str, str] = {}
    inject(headers)
    return headers


def _attributes(operation: str, subject: str) -> dict[str, str]:
    return {
        "messaging.system": "nats",
        "messaging.operation.type": operation,
        "messaging.destination.name": subject,
    }


@contextmanager
def producer_span(subject: str) -> Iterator[Span]:
    """Span around one publish; the trace root when no span is active."""
    with _tracer.start_as_current_span(
        f"send {subject}", kind=SpanKind.PRODUCER, attributes=_attributes("send", subject)
    ) as span:
        yield span


@contextmanager
def consumer_span(msg: Msg, subscription: str | None = None) -> Iterator[Span]:
    """Span around handling one delivered message, joined to the trace in its headers.

    `subscription` is the subscribed subject when it differs from the message
    subject, i.e. a wildcard.
    """
    attributes = _attributes("process", msg.subject)
    if subscription is not None:
        attributes["messaging.destination.subscription.name"] = subscription
    with _tracer.start_as_current_span(
        f"process {msg.subject}",
        context=context_from_headers(msg.headers),
        kind=SpanKind.CONSUMER,
        attributes=attributes,
    ) as span:
        yield span
