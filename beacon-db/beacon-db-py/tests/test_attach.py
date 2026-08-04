"""Attaching a remote Beacon instance as a catalog.

`con.attach(name, url)` mirrors a remote Beacon's schemas and tables under a local catalog name,
with joins/filters/aggregates pushed down to it over Flight SQL. The full round-trip (enumeration +
lazy resolution + pushdown) needs a live remote and is proven by the Rust integration test
`attached_remote_catalog_resolves_and_queries_tables` in beacon-server — the binding cannot start
a Flight SQL server (it links only beacon-core), so these tests pin the surface and the
attach-time failure contract, which do not need a server.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        yield connection


def test_nothing_attached_initially(con):
    assert con.attached() == []


def test_detach_of_an_unattached_name_returns_false(con):
    assert con.detach("nope") is False


def test_attach_contacts_the_remote_now_and_fails_clearly_when_unreachable(con):
    # Enumeration happens at attach time, so an unreachable endpoint raises here (not on first
    # query) and names the endpoint. Port 1 refuses immediately.
    with pytest.raises(beacondb.Error) as excinfo:
        con.attach("remote", "beacon://127.0.0.1:1")
    assert "127.0.0.1:1" in str(excinfo.value)
    # A failed attach records nothing, so detach is a no-op and the list stays empty.
    assert con.attached() == []
    assert con.detach("remote") is False


def test_attach_token_and_tls_are_keyword_only(con):
    # token/tls must be passed by keyword; positional extras are rejected by the signature.
    with pytest.raises(TypeError):
        con.attach("remote", "beacon://127.0.0.1:1", "sometoken")  # type: ignore[misc]


def test_sql_attach_is_reachable_and_fails_clearly_when_unreachable(con):
    # `ATTACH` also works as SQL (so it reaches the server/CLI/SQLAlchemy, not just this binding).
    # It executes immediately, enumerating the remote — an unreachable endpoint errors and names it.
    with pytest.raises(beacondb.Error) as excinfo:
        con.execute("ATTACH 'beacon://127.0.0.1:1' AS remote")
    assert "127.0.0.1:1" in str(excinfo.value)
    assert con.attached() == []


def test_sql_detach_of_an_unattached_name_errors(con):
    # SQL DETACH is strict: a name that is not an attached remote catalog is a clear error.
    with pytest.raises(beacondb.Error):
        con.execute("DETACH nope")


def test_attach_credentials_token_or_userpass_not_both(con):
    # A token and a username/password together is ambiguous — refused before any network call.
    with pytest.raises(beacondb.ProgrammingError, match="not both"):
        con.attach("remote", "beacon://127.0.0.1:1", token="t", password="p")


def test_attach_username_requires_password(con):
    with pytest.raises(beacondb.ProgrammingError, match="password"):
        con.attach("remote", "beacon://127.0.0.1:1", username="u")


def test_sql_attach_username_requires_password(con):
    # The same credential validation applies to the SQL WITH (...) form.
    with pytest.raises(beacondb.Error, match="password"):
        con.execute("ATTACH 'beacon://127.0.0.1:1' AS r WITH ('username' 'u')")
