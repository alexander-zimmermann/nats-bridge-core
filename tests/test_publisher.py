"""Publisher: queue bounds, metrics context passthrough, optional payload validation."""

from __future__ import annotations

import asyncio
from typing import Any

from nats.errors import TimeoutError as NATSTimeoutError
from nats.js.errors import NoStreamResponseError

from nats_bridge_core import NatsSettings, Publisher


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

    async def publish(self, subject: str, body: bytes, **_kwargs: Any) -> None:
        if self.errors:
            raise self.errors.pop(0)
        self.published.append((subject, body))


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
