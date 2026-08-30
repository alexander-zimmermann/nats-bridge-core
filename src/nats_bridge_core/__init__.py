"""Shared plumbing for the Python NATS sidecar bridges.

What lives here is what was byte-identical or mechanically identical across
every bridge: logging, the metrics server, the JetStream publish loop, and the
NATS half of settings. Device lifecycle — connecting to hardware, polling,
normalising payloads — stays in each bridge.
"""

from .config import LogFormat, NatsSettings
from .logging_setup import (
    LOG_EMIT_RECOVERY_WINDOW_SECONDS,
    JsonFormatter,
    TrackedStreamHandler,
    configure,
    watchdog_ok,
)
from .metrics import serve
from .publisher import Publisher, PublisherMetrics

__all__ = [
    "LOG_EMIT_RECOVERY_WINDOW_SECONDS",
    "JsonFormatter",
    "LogFormat",
    "NatsSettings",
    "Publisher",
    "PublisherMetrics",
    "TrackedStreamHandler",
    "configure",
    "serve",
    "watchdog_ok",
]
