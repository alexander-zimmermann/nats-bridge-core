"""connect(): unlimited reconnect, warning-only callbacks, the gauge hook."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from nats_bridge_core import connection
from nats_bridge_core.connection import connect


class FakeClient:
    """Records what connect() asks of nats.py and lets a test fire its callbacks."""

    kwargs: dict[str, Any]

    async def connect(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        FakeClient.last = self

    last: FakeClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> type[FakeClient]:
    monkeypatch.setattr(connection, "NatsClient", FakeClient)
    return FakeClient


async def test_the_client_reconnects_for_as_long_as_it_lives(client: type[FakeClient]) -> None:
    await connect(["nats://a:4222"], name="bridge", nkeys_seed="/run/seed")

    kwargs = client.last.kwargs
    assert kwargs["servers"] == ["nats://a:4222"]
    assert kwargs["name"] == "bridge"
    assert kwargs["nkeys_seed"] == "/run/seed"
    assert kwargs["max_reconnect_attempts"] == -1
    assert kwargs["reconnect_time_wait"] == 2


async def test_the_gauge_follows_the_connection(client: type[FakeClient]) -> None:
    states: list[bool] = []
    await connect(["nats://a:4222"], connected=states.append)
    kwargs = client.last.kwargs

    await kwargs["disconnected_cb"]()
    await kwargs["reconnected_cb"]()
    await kwargs["closed_cb"]()

    assert states == [True, False, True, False]


async def test_a_failed_attempt_is_one_warning_line_without_a_traceback(
    client: type[FakeClient], caplog: pytest.LogCaptureFixture
) -> None:
    await connect(["nats://a:4222"])
    caplog.set_level(logging.INFO, logger="nats_bridge_core.connection")

    await client.last.kwargs["error_cb"](ConnectionRefusedError(111, "Connect call failed"))

    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert record.exc_info is None
    assert "Connect call failed" in record.getMessage()
