"""Connection configuration for beacon-cli.

Connection details come only from explicit CLI arguments (``--url``,
``--username``, ``--password``, ``--timeout``); the CLI deliberately does *not*
read ``BEACON_*`` environment variables, since those configure the Beacon
*server* and picking them up here would silently connect with the server's admin
credentials. The URL defaults to a local server so the CLI works out of the box.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

DEFAULT_URL = "http://127.0.0.1:5001"
DEFAULT_TIMEOUT = 600.0


def _prefer_ipv4_localhost(url: str) -> str:
    """Rewrite a ``localhost`` host to ``127.0.0.1``.

    On Windows ``localhost`` resolves to ``::1`` (IPv6) first; when the server
    listens on IPv4 only, the connection attempt to ``::1`` stalls for ~2s per
    connection before falling back to IPv4. Targeting ``127.0.0.1`` skips that.
    """
    parts = urlsplit(url)
    if parts.hostname != "localhost":
        return url
    netloc = "127.0.0.1"
    if parts.port:
        netloc += f":{parts.port}"
    if parts.username:
        creds = parts.username + (f":{parts.password}" if parts.password else "")
        netloc = f"{creds}@{netloc}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


@dataclass
class ClientConfig:
    """Everything the HTTP client needs to talk to a Beacon server."""

    url: str = DEFAULT_URL
    username: str | None = None
    password: str | None = None
    timeout: float = DEFAULT_TIMEOUT

    def __post_init__(self) -> None:
        self.url = _prefer_ipv4_localhost(self.url.rstrip("/"))

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
        """Build a config from explicit CLI values, applying defaults.

        Only the values passed here are used — no environment variables are
        consulted — so a session's identity is always exactly what the caller
        asked for.
        """
        return cls(
            url=url or DEFAULT_URL,
            username=username,
            password=password,
            timeout=timeout if timeout is not None else DEFAULT_TIMEOUT,
        )
