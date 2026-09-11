"""NATS connection owner: JetStream publish with retry, plus core subscriptions."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js import JetStreamContext
from nats.js.errors import APIError, NoStreamResponseError
from opentelemetry import context as otel_context
from opentelemetry.trace import StatusCode

from . import tracing
from .config import NatsSettings

logger = logging.getLogger(__name__)

# Upper bound on queued-but-unpublished messages. Sized for a multi-hour NATS
# outage on a low-rate device; a busy bus should lower it.
_PUBLISH_QUEUE_MAX = 1000

# How long close() waits for the queue to drain before cancelling the worker.
_CLOSE_FLUSH_TIMEOUT_SECONDS = 5.0


class PublisherMetrics(Protocol):
    """What the publisher needs from a bridge's metrics.

    Label shape differs across bridges — some count per device and kind, others
    have a single undifferentiated counter — so the bridge owns the labelling
    and the publisher only reports what happened, with an opaque context object
    it was handed at enqueue time.
    """

    def set_connected(self, connected: bool) -> None: ...

    def count_published(self, ctx: object) -> None: ...

    def count_error(self, ctx: object, reason: str) -> None: ...


class Publisher:
    """Single NATS client owning the connection, the publish queue and its worker.

    Messages enter through the synchronous enqueue() (safe to call from a
    non-async callback via call_soon_threadsafe); a single worker task drains
    the queue, so messages are published in arrival order.
    """

    def __init__(
        self,
        settings: NatsSettings,
        metrics: PublisherMetrics,
        *,
        validate: Callable[[dict[str, Any]], None] | None = None,
        queue_max: int = _PUBLISH_QUEUE_MAX,
    ) -> None:
        self._settings = settings
        self._metrics = metrics
        self._validate = validate
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._queue: asyncio.Queue[tuple[object, str, dict[str, Any], otel_context.Context]] = (
            asyncio.Queue(maxsize=queue_max)
        )
        self._worker: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        if self._nc and self._nc.is_connected:
            return

        kwargs = self._settings.nats_auth_kwargs()
        kwargs.update(
            servers=self._settings.nats_servers_list,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            connect_timeout=10,
            disconnected_cb=self._on_disconnect,
            reconnected_cb=self._on_reconnect,
            closed_cb=self._on_closed,
            error_cb=self._on_error,
        )

        self._nc = NatsClient()
        await self._nc.connect(**kwargs)
        self._js = self._nc.jetstream()
        self._metrics.set_connected(True)
        logger.info("connected to NATS: %s", self._settings.nats_servers_list)

        if self._settings.nats_stream_check:
            await self._verify_stream()

        if self._worker is None:
            self._worker = asyncio.create_task(self._drain_queue())

    async def _verify_stream(self) -> None:
        assert self._js is not None
        try:
            info = await self._js.stream_info(self._settings.nats_stream_name)
            logger.info(
                "jetstream stream ok: %s (subjects=%s, messages=%d)",
                info.config.name,
                info.config.subjects,
                info.state.messages,
            )
        except Exception as exc:
            logger.warning(
                "jetstream stream %r not reachable at startup: %s",
                self._settings.nats_stream_name,
                exc,
            )

    async def last_message(self, subject: str) -> dict[str, Any] | None:
        """Last archived message on `subject`, or None when there is none.

        Restores state that only ever arrives over NATS, so a pod restart
        doesn't silently drop it.
        """
        if self._js is None:
            return None
        try:
            msg = await self._js.get_last_msg(self._settings.nats_stream_name, subject)
        except Exception as exc:
            logger.info("no archived message for %s: %s", subject, exc)
            return None

        try:
            payload = json.loads(msg.data or b"")
        except json.JSONDecodeError:
            logger.warning("archived message on %s is not valid JSON", subject)
            return None
        return payload if isinstance(payload, dict) else None

    async def subscribe_core(
        self, subject: str, callback: Callable[[Msg], Awaitable[None]]
    ) -> None:
        """Core (non-JetStream) subscription, used for command subjects.

        Each delivery is handled inside a consumer span joined to the trace
        carried in the message headers.
        """
        if self._nc is None:
            raise RuntimeError("subscribe_core() before connect()")

        async def handle(msg: Msg) -> None:
            with tracing.consumer_span(msg, subject):
                await callback(msg)

        await self._nc.subscribe(subject, cb=handle)
        logger.info("subscribed to %s", subject)

    async def close(self) -> None:
        if self._worker is not None:
            # Best-effort flush so a clean shutdown doesn't drop queued messages.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._queue.join(), timeout=_CLOSE_FLUSH_TIMEOUT_SECONDS)
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None
        if self._nc and self._nc.is_connected:
            await self._nc.drain()
        self._metrics.set_connected(False)

    @property
    def is_connected(self) -> bool:
        return bool(self._nc and self._nc.is_connected)

    def enqueue(self, ctx: object, subject: str, payload: dict[str, Any]) -> bool:
        """Queue one message for publishing; safe to call from sync callbacks.

        `ctx` is passed back to the metrics hooks untouched, so the bridge can
        label the counters however it likes. The caller's trace context travels
        with the message, so the publish span joins the caller's trace. Returns
        False (and counts a queue_full error) when the buffer is full, e.g.
        during a NATS outage.
        """
        try:
            self._queue.put_nowait((ctx, subject, payload, otel_context.get_current()))
        except asyncio.QueueFull:
            self._metrics.count_error(ctx, "queue_full")
            logger.warning(
                "publish queue full (%d), dropping message for %s", self._queue.maxsize, subject
            )
            return False
        return True

    async def _drain_queue(self) -> None:
        while True:
            ctx, subject, payload, trace_ctx = await self._queue.get()
            token = otel_context.attach(trace_ctx)
            try:
                await self.publish(ctx, subject, payload)
            except Exception:
                logger.exception("unexpected error publishing %s", subject)
            finally:
                otel_context.detach(token)
                self._queue.task_done()

    async def publish(self, ctx: object, subject: str, payload: dict[str, Any]) -> bool:
        """Publish one message inside a producer span, waiting for a JetStream ack.

        Returns True on success, False on a permanent failure after retries.
        """
        with tracing.producer_span(subject) as span:
            reason = await self._publish(ctx, subject, payload)
            if reason is not None:
                span.set_status(StatusCode.ERROR, reason)
            return reason is None

    async def _publish(self, ctx: object, subject: str, payload: dict[str, Any]) -> str | None:
        """The publish itself; returns the error reason counted in metrics, None on success."""
        if self._validate is not None:
            try:
                self._validate(payload)
            except Exception as exc:
                self._metrics.count_error(ctx, "schema")
                logger.error("payload failed validation: %s | payload=%s", exc, payload)
                return "schema"

        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        headers = tracing.outbound_headers() or None

        reason = "other"
        backoff = 0.1
        for attempt in range(1, 4):
            if not self._js:
                self._metrics.count_error(ctx, "other")
                return "other"
            try:
                await self._js.publish(subject, body, timeout=5.0, headers=headers)
                self._metrics.count_published(ctx)
                return None
            except NoStreamResponseError:
                # Stream/subject misconfiguration: retrying won't help, and any
                # sleep here would stall the ordered publish queue.
                self._metrics.count_error(ctx, "no_stream")
                logger.error("no stream matches subject %s (attempt %d)", subject, attempt)
                return "no_stream"
            except NATSTimeoutError:
                reason = "timeout"
                self._metrics.count_error(ctx, reason)
                logger.warning("publish timeout for %s (attempt %d)", subject, attempt)
            except NoRespondersError:
                reason = "nak"
                self._metrics.count_error(ctx, reason)
                logger.warning("no responders for %s (attempt %d)", subject, attempt)
            except APIError as exc:
                reason = "nak"
                self._metrics.count_error(ctx, reason)
                logger.warning("jetstream api error for %s (attempt %d): %s", subject, attempt, exc)
            except Exception:
                reason = "other"
                self._metrics.count_error(ctx, reason)
                logger.exception("unexpected publish error for %s (attempt %d)", subject, attempt)

            if attempt < 3:
                await asyncio.sleep(backoff)
                backoff *= 2

        return reason

    async def _on_disconnect(self) -> None:
        self._metrics.set_connected(False)
        logger.warning("nats disconnected")

    async def _on_reconnect(self) -> None:
        self._metrics.set_connected(True)
        logger.info("nats reconnected")

    async def _on_closed(self) -> None:
        self._metrics.set_connected(False)
        logger.warning("nats client closed")

    async def _on_error(self, err: Exception) -> None:
        logger.warning("nats error callback: %s", err)
