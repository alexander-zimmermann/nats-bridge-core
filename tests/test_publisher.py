"""Publisher: queue bounds, metrics context passthrough, optional payload validation."""

from __future__ import annotations

from typing import Any

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
