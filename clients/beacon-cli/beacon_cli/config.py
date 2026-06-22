"""Connection configuration for beacon-cli.

Mirrors the Beacon server defaults (host ``0.0.0.0``, port ``5001``, admin
basic-auth ``beacon-admin`` / ``beacon-password``) and honours the same
``BEACON_*`` environment variables so the CLI lines up with a local server out
of the box.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_URL = "http://localhost:5001"
DEFAULT_TIMEOUT = 600.0


@dataclass
class ClientConfig:
    """Everything the HTTP client needs to talk to a Beacon server."""

    url: str = DEFAULT_URL
    username: str | None = None
    password: str | None = None
    timeout: float = DEFAULT_TIMEOUT

    def __post_init__(self) -> None:
        self.url = self.url.rstrip("/")

    @property
    def auth(self) -> tuple[str, str] | None:
        """Basic-auth pair, or ``None`` for an anonymous (read-only) session.

        Supplying credentials elevates requests to super-user, enabling DDL/DML.
        """
        if self.username is not None and self.password is not None:
            return (self.username, self.password)
        return None

    @classmethod
    def resolve(
        cls,
        url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float | None = None,
    ) -> ClientConfig:
        """Build a config from explicit values, falling back to env vars."""
        return cls(
            url=url or os.environ.get("BEACON_URL", DEFAULT_URL),
            username=username if username is not None else os.environ.get("BEACON_ADMIN_USERNAME"),
            password=password if password is not None else os.environ.get("BEACON_ADMIN_PASSWORD"),
            timeout=timeout if timeout is not None else DEFAULT_TIMEOUT,
        )
