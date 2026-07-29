"""Object-store secrets: CREATE / DROP / SHOW SECRET.

Secrets are registered in-process in the same `SecretStore` the object-store registry resolves
against when it builds an S3/GCS/Azure/HTTP store — so a `CREATE SECRET` here is what supplies
credentials to a later `read_parquet('s3://…')`, no environment variables needed. These tests run
entirely in-process (no cloud), pinning the SQL surface, the alias→object_store key mapping, and
the DROP/IF EXISTS semantics. The scope resolution itself is unit-tested in the Rust `secrets`
module.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        yield connection


def _secrets(con):
    return {row[0]: row for row in con.sql("SHOW SECRETS").fetchall()}


def test_create_secret_maps_aliased_params_to_object_store_keys(con):
    con.execute(
        "CREATE SECRET my_s3 (TYPE S3, KEY_ID 'AKIA', SECRET 'shh', "
        "REGION 'eu-west-1', SCOPE 's3://bucket')"
    )
    row = _secrets(con)["my_s3"]
    name, kind, scope, option_keys, persistent = row
    assert (name, kind, scope) == ("my_s3", "s3", "s3://bucket")
    # KEY_ID/SECRET/REGION were normalized to object_store config keys; values are never shown.
    assert option_keys == "access_key_id,region,secret_access_key"
    assert persistent is False


def test_scope_defaults_to_the_scheme_wide_prefix(con):
    con.execute("CREATE SECRET s3_default (TYPE S3, KEY_ID 'x', SECRET 'y')")
    assert _secrets(con)["s3_default"][2] == "s3://"


def test_native_object_store_keys_pass_through(con):
    # An object_store key given directly (not an alias) is kept as-is.
    con.execute(
        "CREATE SECRET direct (TYPE S3, 'access_key_id' 'x', 'endpoint' 'http://minio:9000')"
    )
    assert _secrets(con)["direct"][3] == "access_key_id,endpoint"


def test_create_secret_requires_a_type(con):
    with pytest.raises(beacondb.Error, match="TYPE"):
        con.execute("CREATE SECRET bad (KEY_ID 'x')")


def test_unknown_type_is_rejected(con):
    with pytest.raises(beacondb.Error, match="unknown secret TYPE"):
        con.execute("CREATE SECRET bad (TYPE SFTP)")


def test_drop_secret_and_if_exists(con):
    con.execute("CREATE SECRET s (TYPE S3, KEY_ID 'x')")
    assert "s" in _secrets(con)
    con.execute("DROP SECRET s")
    assert "s" not in _secrets(con)

    # strict drop of a missing secret errors; IF EXISTS makes it a no-op
    with pytest.raises(beacondb.Error):
        con.execute("DROP SECRET s")
    con.execute("DROP SECRET IF EXISTS s")


def test_gcs_and_azure_and_http_types(con):
    con.execute("CREATE SECRET g (TYPE GCS)")
    con.execute("CREATE SECRET a (TYPE AZURE)")
    con.execute("CREATE SECRET h (TYPE HTTP, SCOPE 'https://data.example.org')")
    secrets = _secrets(con)
    assert secrets["g"][1:3] == ("gcs", "gs://")
    assert secrets["a"][1:3] == ("azure", "az://")
    assert secrets["h"][1:3] == ("http", "https://data.example.org")


# ----------------------------------------------------------------------------------------
# Persistent secrets (encrypted in the database file)
# ----------------------------------------------------------------------------------------

import base64  # noqa: E402

# A test master key (32 bytes, base64). Real deployments set BEACON_SECRETS_KEY.
_KEY = base64.b64encode(b"0" * 32).decode()


def test_persistent_secret_requires_a_key(tmp_path):
    # Without a master key, persisting a credential to disk is refused (fail closed).
    con = beacondb.connect(str(tmp_path / "beacon.db"))
    with pytest.raises(beacondb.Error, match="no encryption key"):
        con.execute("CREATE PERSISTENT SECRET p (TYPE S3, KEY_ID 'x', SECRET 'y')")


def test_persistent_secret_survives_reopen_and_is_encrypted(tmp_path):
    path = str(tmp_path / "beacon.db")

    con = beacondb.connect(path, secrets_key=_KEY)
    con.execute(
        "CREATE PERSISTENT SECRET p (TYPE S3, KEY_ID 'AKIA', SECRET 'topsecret', SCOPE 's3://b')"
    )
    con.execute("CREATE SECRET session_only (TYPE S3, KEY_ID 'x')")  # not persistent
    flags = {r[0]: r[4] for r in con.sql("SHOW SECRETS").fetchall()}
    assert flags == {"p": True, "session_only": False}
    con.close()

    # Reopen: the persistent secret is reloaded (still marked persistent); the session one is gone.
    con = beacondb.connect(path, secrets_key=_KEY)
    rows = {r[0]: r for r in con.sql("SHOW SECRETS").fetchall()}
    assert set(rows) == {"p"}
    assert rows["p"][4] is True
    assert rows["p"][3] == "access_key_id,secret_access_key"  # option keys preserved
    con.close()

    # The credential value is encrypted at rest — not findable in the file bytes.
    assert b"topsecret" not in (tmp_path / "beacon.db").read_bytes()


def test_dropping_a_persistent_secret_removes_it_across_reopen(tmp_path):
    path = str(tmp_path / "beacon.db")
    con = beacondb.connect(path, secrets_key=_KEY)
    con.execute("CREATE PERSISTENT SECRET p (TYPE S3, KEY_ID 'x')")
    con.execute("DROP SECRET p")
    con.close()

    con = beacondb.connect(path, secrets_key=_KEY)
    assert con.sql("SHOW SECRETS").fetchall() == []


def test_persistence_refused_for_in_memory(tmp_path):
    con = beacondb.connect(":memory:", secrets_key=_KEY)
    with pytest.raises(beacondb.Error, match="file-backed"):
        con.execute("CREATE PERSISTENT SECRET p (TYPE S3, KEY_ID 'x')")


def test_bad_secrets_key_is_rejected(tmp_path):
    with pytest.raises(beacondb.ProgrammingError, match="32 bytes|base64"):
        beacondb.connect(str(tmp_path / "beacon.db"), secrets_key="not-a-valid-key")


# ----------------------------------------------------------------------------------------
# Remote-Beacon secrets (credentials for ATTACH)
# ----------------------------------------------------------------------------------------


def test_beacon_secret_stores_credentials_verbatim(con):
    # TYPE BEACON keeps token/username/password as-is (no S3 KEY_ID/SECRET aliasing).
    con.execute("CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD 'pw')")
    con.execute("CREATE SECRET lake_tok (TYPE BEACON, TOKEN 'abc')")
    secrets = _secrets(con)
    assert secrets["lake"][1] == "beacon"
    assert secrets["lake"][3] == "password,username"
    assert secrets["lake_tok"][3] == "token"


def test_attach_secret_and_inline_credentials_are_mutually_exclusive(con):
    con.execute("CREATE SECRET lake (TYPE BEACON, TOKEN 'abc')")
    with pytest.raises(beacondb.ProgrammingError, match="not both"):
        con.attach("r", "beacon://127.0.0.1:1", secret="lake", token="t")


def test_attach_with_unknown_secret_errors(con):
    with pytest.raises(beacondb.Error, match="no secret named"):
        con.attach("r", "beacon://127.0.0.1:1", secret="ghost")


def test_attach_with_a_non_beacon_secret_is_refused(con):
    con.execute("CREATE SECRET s3thing (TYPE S3, KEY_ID 'x')")
    with pytest.raises(beacondb.Error, match="not a beacon secret"):
        con.attach("r", "beacon://127.0.0.1:1", secret="s3thing")


def test_beacon_secret_can_be_removed(con):
    con.execute("CREATE SECRET lake (TYPE BEACON, TOKEN 'abc')")
    con.execute("DROP SECRET lake")
    assert "lake" not in _secrets(con)
