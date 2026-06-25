"""Error handling: malformed and unsatisfiable queries must fail cleanly (4xx),
not hang or 500.
"""

from __future__ import annotations


def test_unknown_table_is_400(client):
    assert client.status("SELECT * FROM no_such_table_xyz") == 400


def test_unknown_column_is_400(client):
    assert (
        client.status("SELECT nonexistent_col FROM read_parquet(['obs/*.parquet'])")
        == 400
    )


def test_type_mismatch_is_400(client):
    # number + string has no implicit coercion here.
    assert (
        client.status(
            "SELECT temperature + platform FROM read_parquet(['obs/*.parquet'])"
        )
        == 400
    )


def test_empty_glob_is_400(client):
    assert client.status("SELECT * FROM read_parquet(['does-not-exist/*.parquet'])") == 400


def test_unknown_output_format_is_400(client):
    resp = client.raw({"sql": "SELECT 1", "output": {"format": "bogus_format"}})
    assert resp.status_code == 400


def test_empty_body_is_400(client):
    # Neither a `sql` key nor any DSL field -> not a valid query.
    resp = client.raw({})
    assert resp.status_code == 400
