"""Runtime settings: `SET`, `RESET`, `ALTER SYSTEM` and `SHOW SETTINGS`.

These guard the SQL surface of issue #359 against a live server — that a setting
changes without a restart, that the `beacon.` prefix reaches DataFusion's own
options, and that a setting nobody can change at runtime says so.

`SET` is server-global and super-user-only, so the writes here go through the
admin credential and every test puts what it touched back.
"""

from __future__ import annotations

import pytest


def setting(client, name: str) -> str:
    """The value a setting currently holds, read the way a client would.

    `information_schema` is super-user-only, hence `admin=True`.
    """
    return client.scalar(
        f"SELECT value FROM information_schema.df_settings WHERE name = '{name}'",
        admin=True,
    )


@pytest.fixture
def restore_settings(client):
    """Puts the settings this module touches back the way it found them.

    A `SET` applies to the whole server, so a test that left one changed would
    leak into every later test in the session.
    """
    keys = [
        "beacon.default_table",
        "beacon.sql.stream_coalesce.target_rows",
        "beacon.netcdf.use_rust_reader",
        "beacon.batch_size",
    ]
    yield
    for key in keys:
        client.execute(f"RESET {key}")


def test_set_changes_a_setting_without_a_restart(client, restore_settings):
    assert setting(client, "beacon.default_table") == "default"

    client.execute("SET beacon.default_table = 'observations'")
    assert setting(client, "beacon.default_table") == "observations"


def test_set_and_show_agree(client, restore_settings):
    client.execute("SET beacon.sql.stream_coalesce.target_rows = 1024")

    rows = client.sql_rows("SHOW beacon.sql.stream_coalesce.target_rows", admin=True)
    # header + one row: the name and its value.
    assert len(rows) == 2
    assert rows[1][0] == "beacon.sql.stream_coalesce.target_rows"
    assert rows[1][1] == "1024"


def test_the_beacon_prefix_reaches_datafusion_options(client, restore_settings):
    """`beacon.` is a complete alias for `datafusion.`, and both keep working."""
    client.execute("SET beacon.execution.batch_size = 8192")
    assert setting(client, "datafusion.execution.batch_size") == "8192"

    # The documented BEACON_BATCH_SIZE spelling lands in the same option.
    client.execute("SET beacon.batch_size = 4096")
    assert setting(client, "datafusion.execution.batch_size") == "4096"

    client.execute("SET datafusion.execution.batch_size = 2048")
    assert setting(client, "datafusion.execution.batch_size") == "2048"


def test_reset_restores_the_startup_value(client, restore_settings):
    client.execute("SET beacon.netcdf.use_rust_reader = true")
    assert setting(client, "beacon.netcdf.use_rust_reader") == "true"

    client.execute("RESET beacon.netcdf.use_rust_reader")
    assert setting(client, "beacon.netcdf.use_rust_reader") == "false"


def test_a_query_still_runs_with_a_changed_setting(client, sample_data, restore_settings):
    """Changing a knob must not change an answer."""
    obs = "read_parquet(['obs/*.parquet'])"
    before = client.count(f"SELECT * FROM {obs}")

    client.execute("SET beacon.sql.stream_coalesce.target_rows = 1024")
    client.execute("SET beacon.execution.batch_size = 512")

    assert client.count(f"SELECT * FROM {obs}") == before


def test_a_startup_only_setting_is_refused(client):
    with pytest.raises(Exception) as excinfo:
        client.execute("SET beacon.port = 1234")
    assert "BEACON_PORT" in str(excinfo.value)


def test_an_unknown_setting_points_at_show_settings(client):
    with pytest.raises(Exception) as excinfo:
        client.execute("SET beacon.nonsense = 1")
    assert "SHOW SETTINGS" in str(excinfo.value)


def test_show_settings_is_readable_without_admin(client):
    """The issue's "a user cannot discover which settings exist": `SHOW SETTINGS`
    is the one settings surface a non-super-user can read."""
    rows = client.sql_rows("SHOW SETTINGS", admin=False)
    names = [row[0] for row in rows[1:]]

    assert "beacon.default_table" in names
    assert "beacon.netcdf.use_rust_reader" in names
    # Only the beacon namespace: the DataFusion half stays in df_settings.
    assert all(name.startswith("beacon.") for name in names)
    # Every row carries a description, so the listing documents itself.
    assert all(row[3] for row in rows[1:])


def test_changing_a_setting_needs_admin(client):
    with pytest.raises(Exception):
        client.execute("SET beacon.default_table = 'observations'", admin=False)
    assert setting(client, "beacon.default_table") == "default"


def test_alter_system_applies_immediately(client):
    """The persistent form still takes effect now. The restart half is covered by
    the Rust suite, which can restart a runtime in-process."""
    try:
        client.execute("ALTER SYSTEM SET beacon.default_table = 'observations'")
        assert setting(client, "beacon.default_table") == "observations"
    finally:
        client.execute("ALTER SYSTEM RESET beacon.default_table")
    assert setting(client, "beacon.default_table") == "default"


def test_beacon_system_settings_table(client, restore_settings):
    """The table form of `SHOW SETTINGS`, for a client that would rather filter."""
    client.execute("SET beacon.default_table = 'observations'")

    rows = client.sql_rows(
        'SELECT value, "default" FROM beacon.system.settings '
        "WHERE name = 'beacon.default_table'",
        admin=True,
    )
    assert rows[1][0] == "observations"
    assert rows[1][1] == "default"
