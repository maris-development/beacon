"""Error types for beacon-cli, with messages safe to show end users."""

from __future__ import annotations


class BeaconCliError(RuntimeError):
    """Base class for errors that should be printed without a traceback."""


class QueryError(BeaconCliError):
    """A query (or metadata request) returned a non-2xx status.

    The server returns its error as a JSON string body
    (``Json(err.to_string())``); we surface it verbatim, trimmed to keep
    terminal output readable.
    """

    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body
        super().__init__(f"beacon request failed [{status_code}]: {_trim(body)}")


class ConnectionFailedError(BeaconCliError):
    """The server could not be reached at the configured URL."""

    def __init__(self, url: str, detail: str):
        self.url = url
        self.detail = detail
        super().__init__(f"could not connect to beacon at {url}: {detail}")


class AuthenticationError(BeaconCliError):
    """Credentials were supplied but the server rejected them.

    Raised when the CLI is configured with admin credentials that fail
    authentication, so the session could not be established as a super-user.
    """

    def __init__(self, url: str, username: str | None):
        self.url = url
        self.username = username
        who = f" (user {username!r})" if username else ""
        super().__init__(
            f"could not connect to beacon at {url} as super-user{who}: "
            "invalid admin credentials — check the --username / --password arguments"
        )


class StreamInterruptedError(BeaconCliError):
    """The server closed the response stream before it was complete.

    Typically means the query failed server-side *after* the response started
    (e.g. an execution error mid-stream), so no clean error body was returned.
    """

    def __init__(self, url: str, detail: str):
        self.url = url
        self.detail = detail
        super().__init__(
            f"the server at {url} closed the connection before the response "
            f"completed; the query likely failed during execution ({detail})"
        )


def _trim(body: str, limit: int = 500) -> str:
    body = body.strip()
    # The server wraps error strings in JSON quotes; unwrap a plain quoted string.
    if len(body) >= 2 and body[0] == '"' and body[-1] == '"':
        try:
            import json

            body = json.loads(body)
        except ValueError:
            pass
    return body if len(body) <= limit else body[: limit - 3] + "..."
