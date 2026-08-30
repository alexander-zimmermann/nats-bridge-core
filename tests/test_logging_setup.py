"""Log-emit watchdog: healthy until emits fail for longer than the recovery window."""

from __future__ import annotations

import pytest

from nats_bridge_core import LOG_EMIT_RECOVERY_WINDOW_SECONDS, TrackedStreamHandler, watchdog_ok


@pytest.fixture(autouse=True)
def _reset_handler_state() -> None:
    TrackedStreamHandler.emit_errors_total = 0
    TrackedStreamHandler.last_emit_ok_ts = 0.0


def test_ok_when_no_emit_errors() -> None:
    assert watchdog_ok(now=10_000.0) is True


def test_ok_while_within_recovery_window() -> None:
    TrackedStreamHandler.emit_errors_total = 3
    TrackedStreamHandler.last_emit_ok_ts = 1_000.0
    assert watchdog_ok(now=1_000.0 + LOG_EMIT_RECOVERY_WINDOW_SECONDS - 1) is True


def test_fails_once_window_elapsed() -> None:
    TrackedStreamHandler.emit_errors_total = 1
    TrackedStreamHandler.last_emit_ok_ts = 1_000.0
    assert watchdog_ok(now=1_000.0 + LOG_EMIT_RECOVERY_WINDOW_SECONDS + 1) is False
