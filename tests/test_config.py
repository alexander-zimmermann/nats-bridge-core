"""NatsSettings: auth precedence, subject-prefix validation, server list parsing."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from nats_bridge_core import NatsSettings


def test_servers_list_splits_and_strips() -> None:
    s = NatsSettings(nats_servers="nats://a:4222, nats://b:4222 ,")
    assert s.nats_servers_list == ["nats://a:4222", "nats://b:4222"]


@pytest.mark.parametrize("bad", ["with.dot", "with/slash", "with space", ""])
def test_subject_prefix_rejects_non_tokens(bad: str) -> None:
    with pytest.raises(ValidationError):
        NatsSettings(nats_subject_prefix=bad)


def test_creds_file_wins_over_nkey_and_user(tmp_path: Path) -> None:
    creds = tmp_path / "u.creds"
    creds.write_text("x")
    seed = tmp_path / "seed"
    seed.write_text("y")
    s = NatsSettings(nats_creds_file=creds, nats_nkey_seed_file=seed, nats_user="bob")
    assert s.nats_auth_kwargs() == {"user_credentials": str(creds)}


def test_nkey_wins_over_user(tmp_path: Path) -> None:
    seed = tmp_path / "seed"
    seed.write_text("y")
    s = NatsSettings(nats_nkey_seed_file=seed, nats_user="bob")
    assert s.nats_auth_kwargs() == {"nkeys_seed": str(seed)}


def test_user_password_read_from_file(tmp_path: Path) -> None:
    pw = tmp_path / "pw"
    pw.write_text("  hunter2\n")
    s = NatsSettings(nats_user="bob", nats_user_password_file=pw)
    assert s.nats_auth_kwargs() == {"user": "bob", "password": "hunter2"}


def test_user_without_password_file_raises() -> None:
    s = NatsSettings(nats_user="bob")
    with pytest.raises(RuntimeError, match="NATS_USER is set"):
        s.nats_auth_kwargs()


def test_no_auth_configured_is_empty() -> None:
    assert NatsSettings().nats_auth_kwargs() == {}
