"""One recording tracer provider for the whole session: the global can only be set once."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from nats.aio.msg import Msg
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    """Finished spans of the current test."""
    _exporter.clear()
    yield _exporter
    _exporter.clear()


TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
TRACE_ID = 0x0AF7651916CD43DD8448EB211C80319C
SPAN_ID = 0xB7AD6B7169203331


def make_msg(subject: str, headers: dict[str, str] | None = None) -> Msg:
    """A delivered message without a client behind it."""
    return Msg(_client=None, subject=subject, data=b"{}", headers=headers)  # type: ignore[arg-type]
