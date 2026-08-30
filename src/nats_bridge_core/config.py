"""Settings base shared by every bridge: NATS connection, auth, and observability."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogFormat(StrEnum):
    JSON = "json"
    TEXT = "text"


class NatsSettings(BaseSettings):
    """Base for a bridge's Settings: NATS wiring plus observability.

    Subclasses add their device-specific fields and override nats_subject_prefix
    and nats_stream_name with their own defaults.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    nats_servers: str = "nats://localhost:4222"
    nats_subject_prefix: str = "bridge"
    nats_creds_file: Path | None = None
    nats_nkey_seed_file: Path | None = None
    nats_user: str | None = None
    nats_user_password_file: Path | None = None
    nats_stream_check: bool = True
    nats_stream_name: str = "BRIDGE"

    # Observability
    metrics_port: int = 9090
    log_level: str = "INFO"
    log_format: LogFormat = LogFormat.JSON

    @property
    def nats_servers_list(self) -> list[str]:
        return [s.strip() for s in self.nats_servers.split(",") if s.strip()]

    @field_validator("nats_subject_prefix")
    @classmethod
    def _single_token(cls, v: str) -> str:
        if "." in v or "/" in v or " " in v or not v:
            raise ValueError(
                "nats_subject_prefix must be a non-empty single token (no dots, slashes, spaces)"
            )
        return v

    def read_nats_password(self) -> str | None:
        if self.nats_user_password_file and self.nats_user_password_file.exists():
            return self.nats_user_password_file.read_text().strip()
        return None

    def nats_auth_kwargs(self) -> dict[str, Any]:
        """Build the auth subset of NatsClient.connect kwargs.

        Auth precedence: creds file > nkey seed file > user/password.
        Each form is mutually exclusive in nats-py; pick the first that's configured.
        """
        kwargs: dict[str, Any] = {}
        if self.nats_creds_file and self.nats_creds_file.exists():
            kwargs["user_credentials"] = str(self.nats_creds_file)
        elif self.nats_nkey_seed_file and self.nats_nkey_seed_file.exists():
            kwargs["nkeys_seed"] = str(self.nats_nkey_seed_file)
        elif self.nats_user:
            password = self.read_nats_password()
            if password is None:
                raise RuntimeError(
                    "NATS_USER is set but NATS_USER_PASSWORD_FILE is missing or empty"
                )
            kwargs["user"] = self.nats_user
            kwargs["password"] = password
        return kwargs
