"""RBAC integration tests over the HTTP API.

Covers the admin auth list endpoints (``/api/admin/auth/{users,roles}``), the
SQL-driven user/role/grant lifecycle, the model guards (read-only roles, reserved
super-user, protected anonymous user, non-super users can't manage or enumerate),
and — when the server runs with enforcement on — that grants allow and denies
block query results.

Connection: by default these use the Docker-managed container like the rest of
the suite. Set ``BEACON_BASE_URL`` (and optionally ``BEACON_ADMIN_USERNAME`` /
``BEACON_ADMIN_PASSWORD``) to run against an already-running Beacon instead — no
Docker required. The enforcement test only runs when ``BEACON_AUTH_ENFORCE=true``
is set in the test environment (matching the server's configuration).
"""

from __future__ import annotations

import os

import pytest
import requests

from beacon_client import BeaconHTTPClient

ADMIN_USER = os.environ.get("BEACON_ADMIN_USERNAME", "admin")
ADMIN_PW = os.environ.get("BEACON_ADMIN_PASSWORD", "securepassword")

ROLE = "rbac_reader"
USER = "rbac_alice"
USER2 = "rbac_bob"
TABLE = "rbac_obs"


@pytest.fixture(scope="module")
def base_url(request) -> str:
    """An external server (``BEACON_BASE_URL``) or the suite's Docker container.

    Resolving the Docker container lazily means setting ``BEACON_BASE_URL`` skips
    Docker entirely (no image build, network, or container).
    """
    url = os.environ.get("BEACON_BASE_URL")
    if url:
        return url.rstrip("/")
    return request.getfixturevalue("beacon_container")


@pytest.fixture(scope="module")
def admin(base_url) -> BeaconHTTPClient:
    return BeaconHTTPClient(base_url, ADMIN_USER, ADMIN_PW)


@pytest.fixture
def clean(admin):
    """Drop the test users/roles/table before and after, so reruns start clean."""

    def _drop():
        for sql in (
            f"REVOKE ROLE {ROLE} FROM USER {USER}",
            f"DROP USER {USER}",
            f"DROP USER {USER2}",
            f"DROP ROLE {ROLE}",
            f"DROP TABLE IF EXISTS {TABLE}",
        ):
            admin.raw({"sql": sql}, admin=True)  # ignore failures (may not exist)

    _drop()
    yield
    _drop()


def _users(admin: BeaconHTTPClient) -> list[dict]:
    resp = admin.admin_get("/api/admin/auth/users")
    assert resp.status_code == 200, resp.text
    return resp.json()


def _roles(admin: BeaconHTTPClient) -> list[dict]:
    resp = admin.admin_get("/api/admin/auth/roles")
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_list_flags_super_user_and_anonymous(admin):
    users = _users(admin)
    su = next(u for u in users if u["username"] == ADMIN_USER)
    assert su["is_super_user"] is True
    anon = next(u for u in users if u["username"] == "anonymous")
    assert anon["is_anonymous"] is True
    assert anon["is_super_user"] is False


def test_admin_auth_endpoints_require_super_user(admin, base_url, clean):
    # No credentials -> 401.
    assert admin.admin_get("/api/admin/auth/users", admin=False).status_code == 401
    assert requests.get(f"{base_url}/api/admin/auth/roles").status_code == 401

    # A valid but non-super user -> 403.
    admin.execute(f"CREATE USER {USER2} WITH PASSWORD 'pw'", admin=True)
    bob = BeaconHTTPClient(base_url, USER2, "pw")
    assert bob.admin_get("/api/admin/auth/users").status_code == 403


def test_rbac_lifecycle_reflected_in_endpoints(admin, clean):
    admin.execute(f"CREATE ROLE {ROLE}", admin=True)
    admin.execute(f"GRANT SELECT ON TABLE observations TO ROLE {ROLE}", admin=True)
    admin.execute(f"GRANT SELECT ON PATH 'argo/**/*.nc' TO ROLE {ROLE}", admin=True)
    admin.execute(f"DENY SELECT ON TABLE secret TO ROLE {ROLE}", admin=True)
    admin.execute(f"CREATE USER {USER} WITH PASSWORD 'pw'", admin=True)
    admin.execute(f"GRANT ROLE {ROLE} TO USER {USER}", admin=True)

    role = next(r for r in _roles(admin) if r["name"] == ROLE)
    grants = role["grants"]
    assert any(g["target_type"] == "table" and g["target_value"] == "observations" for g in grants)
    assert any(g["target_type"] == "path" and g["target_value"] == "argo/**/*.nc" for g in grants)
    assert any(d["target_type"] == "table" and d["target_value"] == "secret" for d in role["denies"])

    user = next(u for u in _users(admin) if u["username"] == USER)
    assert user["roles"] == [ROLE]

    # Revoke + drop, then confirm the state is gone.
    admin.execute(f"REVOKE SELECT ON TABLE observations FROM ROLE {ROLE}", admin=True)
    admin.execute(f"REVOKE DENY SELECT ON TABLE secret FROM ROLE {ROLE}", admin=True)
    admin.execute(f"REVOKE ROLE {ROLE} FROM USER {USER}", admin=True)
    admin.execute(f"DROP USER {USER}", admin=True)
    assert all(u["username"] != USER for u in _users(admin))

    role = next(r for r in _roles(admin) if r["name"] == ROLE)
    assert all(g["target_value"] != "observations" for g in role["grants"])
    assert role["denies"] == []

    admin.execute(f"DROP ROLE {ROLE}", admin=True)
    assert all(r["name"] != ROLE for r in _roles(admin))


def test_model_guards(admin, clean):
    admin.execute(f"CREATE ROLE {ROLE}", admin=True)

    # Roles are read-only: only SELECT may be granted.
    assert admin.raw({"sql": f"GRANT INSERT ON TABLE t TO ROLE {ROLE}"}, admin=True).status_code == 400

    # The super-user username is reserved and cannot be created or dropped.
    assert admin.raw({"sql": f"DROP USER {ADMIN_USER}"}, admin=True).status_code == 400
    assert admin.raw(
        {"sql": f"CREATE USER {ADMIN_USER} WITH PASSWORD 'x'"}, admin=True
    ).status_code == 400

    # The anonymous user can't be deleted while anonymous access is enabled.
    assert admin.raw({"sql": "DROP USER anonymous"}, admin=True).status_code == 400
    assert any(u["username"] == "anonymous" for u in _users(admin))


def test_non_super_user_cannot_manage_or_enumerate(admin, base_url, clean):
    admin.execute(f"CREATE USER {USER2} WITH PASSWORD 'pw'", admin=True)
    bob = BeaconHTTPClient(base_url, USER2, "pw")

    # Auth DDL requires the super-user -> 400.
    assert bob.raw({"sql": "CREATE ROLE hacker"}, admin=True).status_code == 400
    # The enumeration endpoints reject non-super principals -> 403.
    assert bob.admin_get("/api/admin/auth/roles").status_code == 403


@pytest.mark.skipif(
    os.environ.get("BEACON_AUTH_ENFORCE", "").lower() != "true",
    reason="requires the server to run with BEACON_AUTH_ENFORCE=true",
)
def test_enforcement_grant_allows_deny_blocks(admin, base_url, clean):
    admin.execute(f"CREATE TABLE {TABLE} (a BIGINT)", admin=True)
    admin.execute(f"INSERT INTO {TABLE} VALUES (1)", admin=True)
    admin.execute(f"CREATE ROLE {ROLE}", admin=True)
    admin.execute(f"GRANT SELECT TO ROLE {ROLE}", admin=True)
    admin.execute(f"CREATE USER {USER} WITH PASSWORD 'pw'", admin=True)
    admin.execute(f"GRANT ROLE {ROLE} TO USER {USER}", admin=True)
    alice = BeaconHTTPClient(base_url, USER, "pw")

    # The global grant lets alice read.
    assert alice.raw({"sql": f"SELECT * FROM {TABLE}"}, admin=True).status_code == 200

    # A deny wins over the grant.
    admin.execute(f"DENY SELECT ON TABLE {TABLE} TO ROLE {ROLE}", admin=True)
    assert alice.raw({"sql": f"SELECT * FROM {TABLE}"}, admin=True).status_code == 400

    # A user with no roles can't read at all.
    admin.execute(f"CREATE USER {USER2} WITH PASSWORD 'pw'", admin=True)
    mallory = BeaconHTTPClient(base_url, USER2, "pw")
    assert mallory.raw({"sql": f"SELECT * FROM {TABLE}"}, admin=True).status_code == 400
