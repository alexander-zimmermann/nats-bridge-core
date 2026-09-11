"""Publisher: queue bounds, metrics context passthrough, payload validation, trace context."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from nats.aio.msg import Msg
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js.errors import NoStreamResponseError
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from nats_bridge_core import NatsSettings, Publisher

TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


class FakeMetrics:
    def __init__(self) -> None:
        self.connected: bool | None = None
        self.published: list[object] = []
        self.errors: list[tuple[object, str]] = []

    def set_connected(self, connected: bool) -> None:
        self.connected = connected

    def count_published(self, ctx: object) -> None:
        self.published.append(ctx)

    def count_error(self, ctx: object, reason: str) -> None:
        self.errors.append((ctx, reason))


def _publisher(**kwargs: Any) -> tuple[Publisher, FakeMetrics]:
    metrics = FakeMetrics()
    return Publisher(NatsSettings(), metrics, **kwargs), metrics


async def test_enqueue_accepts_until_full_then_counts_queue_full() -> None:
    pub, metrics = _publisher(queue_max=2)
    assert pub.enqueue("a", "s.one", {"v": 1}) is True
    assert pub.enqueue("b", "s.two", {"v": 2}) is True

    assert pub.enqueue("c", "s.three", {"v": 3}) is False
    assert metrics.errors == [("c", "queue_full")]


async def test_enqueue_passes_context_through_untouched() -> None:
    pub, metrics = _publisher()
    ctx = ("kitchen", "state")
    pub.enqueue(ctx, "s.one", {"v": 1})
    assert pub.enqueue(ctx, "s.one", {"v": 1}) is True
    # The publisher never inspects ctx; the queue holds it as given.
    assert pub._queue.get_nowait()[0] is ctx


async def test_validation_failure_is_counted_and_not_published() -> None:
    def reject(_: dict[str, Any]) -> None:
        raise ValueError("bad payload")

    pub, metrics = _publisher(validate=reject)
    assert await pub.publish("ctx", "s.one", {"v": 1}) is False
    assert metrics.errors == [("ctx", "schema")]
    assert metrics.published == []


async def test_publish_without_connection_counts_other() -> None:
    pub, metrics = _publisher()
    assert await pub.publish("ctx", "s.one", {"v": 1}) is False
    assert metrics.errors == [("ctx", "other")]


def test_is_connected_false_before_connect() -> None:
    pub, _ = _publisher()
    assert pub.is_connected is False


class FakeJetStream:
    """Records publishes; raises the queued exceptions first, then succeeds."""

    def __init__(self, errors: list[Exception] | None = None) -> None:
        self.errors = list(errors or [])
        self.published: list[tuple[str, bytes]] = []
        self.headers: list[dict[str, str] | None] = []

    async def publish(self, subject: str, body: bytes, **kwargs: Any) -> None:
        if self.errors:
            raise self.errors.pop(0)
        self.published.append((subject, body))
        self.headers.append(kwargs.get("headers"))


async def test_retries_timeouts_then_succeeds() -> None:
    pub, metrics = _publisher()
    js = FakeJetStream(errors=[NATSTimeoutError(), NATSTimeoutError()])
    pub._js = js  # type: ignore[assignment]

    assert await pub.publish("ctx", "s.one", {"v": 1}) is True
    assert len(js.published) == 1
    assert metrics.errors == [("ctx", "timeout"), ("ctx", "timeout")]
    assert metrics.published == ["ctx"]


async def test_gives_up_after_three_timeouts() -> None:
    pub, metrics = _publisher()
    pub._js = FakeJetStream(errors=[NATSTimeoutError()] * 3)  # type: ignore[assignment]

    assert await pub.publish("ctx", "s.one", {"v": 1}) is False
    assert metrics.errors == [("ctx", "timeout")] * 3
    assert metrics.published == []


async def test_no_stream_fails_fast_without_retry() -> None:
    pub, metrics = _publisher()
    js = FakeJetStream(errors=[NoStreamResponseError()])
    pub._js = js  # type: ignore[assignment]

    assert await pub.publish("ctx", "s.one", {"v": 1}) is False
    assert js.published == []
    assert metrics.errors == [("ctx", "no_stream")]


async def test_worker_drains_queue_in_order() -> None:
    pub, _ = _publisher()
    js = FakeJetStream()
    pub._js = js  # type: ignore[assignment]

    for sub in ("s.one", "s.two", "s.three"):
        assert pub.enqueue(None, sub, {"v": 1}) is True

    worker = asyncio.create_task(pub._drain_queue())
    await asyncio.wait_for(pub._queue.join(), timeout=2.0)
    worker.cancel()

    assert [s for s, _ in js.published] == ["s.one", "s.two", "s.three"]


def _send_span(spans: InMemorySpanExporter, subject: str) -> Any:
    return next(s for s in spans.get_finished_spans() if s.name == f"send {subject}")


async def test_publish_sends_traceparent_of_the_enclosing_span(spans: InMemorySpanExporter) -> None:
    pub, _ = _publisher()
    js = FakeJetStream()
    pub._js = js  # type: ignore[assignment]

    with trace.get_tracer("test").start_as_current_span("caller") as caller:
        assert await pub.publish("ctx", "s.one", {"v": 1}) is True

    caller_sc = caller.get_span_context()
    assert js.headers[0] is not None
    assert js.headers[0]["traceparent"].split("-")[1] == format(caller_sc.trace_id, "032x")
    send = _send_span(spans, "s.one")
    assert send.kind is SpanKind.PRODUCER
    assert send.parent is not None and send.parent.span_id == caller_sc.span_id
    assert send.attributes["messaging.destination.name"] == "s.one"


async def test_publish_without_caller_span_starts_a_root_trace(spans: InMemorySpanExporter) -> None:
    pub, _ = _publisher()
    js = FakeJetStream()
    pub._js = js  # type: ignore[assignment]

    assert await pub.publish("ctx", "s.one", {"v": 1}) is True

    send = _send_span(spans, "s.one")
    assert send.parent is None
    assert js.headers[0] is not None
    assert js.headers[0]["traceparent"].split("-")[1] == format(send.context.trace_id, "032x")


async def test_enqueue_carries_the_trace_context_to_the_worker(spans: InMemorySpanExporter) -> None:
    pub, _ = _publisher()
    pub._js = FakeJetStream()  # type: ignore[assignment]

    with trace.get_tracer("test").start_as_current_span("caller") as caller:
        assert pub.enqueue(None, "s.one", {"v": 1}) is True

    worker = asyncio.create_task(pub._drain_queue())
    await asyncio.wait_for(pub._queue.join(), timeout=2.0)
    worker.cancel()

    caller_sc = caller.get_span_context()
    send = _send_span(spans, "s.one")
    assert send.context.trace_id == caller_sc.trace_id
    assert send.parent is not None and send.parent.span_id == caller_sc.span_id


async def test_publish_failure_marks_the_span_as_error(spans: InMemorySpanExporter) -> None:
    pub, _ = _publisher()
    pub._js = FakeJetStream(errors=[NoStreamResponseError()])  # type: ignore[assignment]

    assert await pub.publish("ctx", "s.one", {"v": 1}) is False

    send = _send_span(spans, "s.one")
    assert send.status.status_code is StatusCode.ERROR
    assert send.status.description == "no_stream"


class FakeCoreClient:
    """Records core subscriptions so a test can deliver messages by hand."""

    def __init__(self) -> None:
        self.callbacks: dict[str, Callable[[Msg], Awaitable[None]]] = {}

    async def subscribe(self, subject: str, cb: Callable[[Msg], Awaitable[None]]) -> None:
        self.callbacks[subject] = cb


async def test_subscribe_core_handles_each_message_in_a_consumer_span(
    spans: InMemorySpanExporter,
) -> None:
    pub, _ = _publisher()
    nc = FakeCoreClient()
    pub._nc = nc  # type: ignore[assignment]
    seen: list[int] = []

    async def handler(_: Msg) -> None:
        seen.append(trace.get_current_span().get_span_context().trace_id)

    await pub.subscribe_core("dev.*.command.*", handler)
    msg = Msg(
        _client=None,  # type: ignore[arg-type]
        subject="dev.kitchen.command.power",
        data=b"{}",
        headers={"Traceparent": TRACEPARENT},
    )
    await nc.callbacks["dev.*.command.*"](msg)

    assert seen == [0x0AF7651916CD43DD8448EB211C80319C]
    process = next(s for s in spans.get_finished_spans() if s.name.startswith("process "))
    assert process.name == "process dev.kitchen.command.power"
    assert process.kind is SpanKind.CONSUMER
    assert process.parent is not None and process.parent.span_id == 0xB7AD6B7169203331
    assert process.attributes["messaging.destination.subscription.name"] == "dev.*.command.*"
