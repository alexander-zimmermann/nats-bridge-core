"""Tracing helpers: provider shape, header extraction, span parenting."""

from __future__ import annotations

import pytest
from conftest import SPAN_ID, TRACE_ID, TRACEPARENT, make_msg
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import INVALID_SPAN, SpanKind
from pydantic import ValidationError

from nats_bridge_core import NatsSettings, tracing


def test_configure_without_endpoint_leaves_the_provider_alone() -> None:
    before = trace.get_tracer_provider()
    tracing.configure(NatsSettings(), service_name="test-bridge")
    assert trace.get_tracer_provider() is before


def test_build_provider_samples_by_trace_id_under_a_parent() -> None:
    provider = tracing.build_provider("http://collector:4318", 0.25, "test-bridge")
    assert provider.sampler.get_description().startswith("ParentBased{root:TraceIdRatioBased{0.25}")
    assert provider.resource.attributes["service.name"] == "test-bridge"
    provider.shutdown()


def test_traces_url_appends_the_otlp_path() -> None:
    assert tracing.traces_url("http://collector:4318") == "http://collector:4318/v1/traces"
    assert tracing.traces_url("http://collector:4318/") == "http://collector:4318/v1/traces"


@pytest.mark.parametrize("ratio", [-0.1, 1.5])
def test_sampling_ratio_is_bounded(ratio: float) -> None:
    with pytest.raises(ValidationError):
        NatsSettings(tracing_sampling_ratio=ratio)


def test_context_from_headers_ignores_header_case() -> None:
    ctx = tracing.context_from_headers({"Traceparent": TRACEPARENT})
    sc = trace.get_current_span(ctx).get_span_context()
    assert (sc.trace_id, sc.span_id, sc.is_remote) == (TRACE_ID, SPAN_ID, True)


def test_context_from_headers_without_traceparent_has_no_span() -> None:
    assert trace.get_current_span(tracing.context_from_headers(None)) is INVALID_SPAN


def test_outbound_headers_carry_the_active_span() -> None:
    with tracing.producer_span("s.one") as span:
        headers = tracing.outbound_headers()
    trace_id = format(span.get_span_context().trace_id, "032x")
    assert headers["traceparent"].split("-")[1] == trace_id


def test_outbound_headers_are_empty_without_a_valid_span() -> None:
    with trace.use_span(INVALID_SPAN):
        assert tracing.outbound_headers() == {}


def test_consumer_span_joins_the_trace_from_the_headers(spans: InMemorySpanExporter) -> None:
    msg = make_msg("dev.kitchen.command.power", {"traceparent": TRACEPARENT})
    with tracing.consumer_span(msg, "dev.*.command.*"):
        pass

    (span,) = spans.get_finished_spans()
    assert span.name == "process dev.kitchen.command.power"
    assert span.kind is SpanKind.CONSUMER
    assert span.context.trace_id == TRACE_ID
    assert span.parent is not None and span.parent.span_id == SPAN_ID
    assert span.attributes is not None
    assert span.attributes["messaging.destination.subscription.name"] == "dev.*.command.*"


def test_consumer_span_without_headers_starts_a_root_even_inside_a_span(
    spans: InMemorySpanExporter,
) -> None:
    with (
        trace.get_tracer("test").start_as_current_span("startup"),
        tracing.consumer_span(make_msg("dev.kitchen.command.power")),
    ):
        pass

    process = next(s for s in spans.get_finished_spans() if s.name.startswith("process "))
    assert process.parent is None


def test_consumer_span_records_the_subscription_only_for_wildcards(
    spans: InMemorySpanExporter,
) -> None:
    msg = make_msg("dev.kitchen.command.power")
    with tracing.consumer_span(msg, "dev.kitchen.command.power"):
        pass

    (span,) = spans.get_finished_spans()
    assert span.attributes is not None
    assert "messaging.destination.subscription.name" not in span.attributes
