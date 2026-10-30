"""One NATS client per connection: reconnects for as long as the pod lives, one line per failure."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from nats.aio.client import Client as NatsClient

logger = logging.getLogger(__name__)

# nats.py gives up after 60 attempts two seconds apart; a pod waits for its
# server for as long as it lives, and its liveness probe decides the rest.
_RECONNECT_TIME_WAIT_SECONDS = 2
_CONNECT_TIMEOUT_SECONDS = 10


async def connect(
    servers: list[str],
    *,
    name: str | None = None,
    connected: Callable[[bool], object] | None = None,
    **auth: Any,
) -> NatsClient:
    """Connect to NATS and keep reconnecting; the client, once connected.

    `connected` hears every change of the connection's state, for a gauge.
    A failed attempt is one warning line: nats.py's own callback logs a
    traceback per attempt, thirty a minute while the server restarts.
    `auth` is what `NatsSettings.nats_auth_kwargs()` returns.
    """

    def state(up: bool) -> None:
        if connected is not None:
            connected(up)

    async def on_disconnect() -> None:
        state(False)
        logger.warning("nats disconnected")

    async def on_reconnect() -> None:
        state(True)
        logger.info("nats reconnected")

    async def on_closed() -> None:
        state(False)
        logger.warning("nats client closed")

    async def on_error(err: Exception) -> None:
        logger.warning("nats error callback: %s", err)

    client = NatsClient()
    await client.connect(
        servers=servers,
        name=name,
        max_reconnect_attempts=-1,
        reconnect_time_wait=_RECONNECT_TIME_WAIT_SECONDS,
        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        disconnected_cb=on_disconnect,
        reconnected_cb=on_reconnect,
        closed_cb=on_closed,
        error_cb=on_error,
        **auth,
    )
    state(True)
    logger.info("connected to NATS: %s", servers)
    return client
