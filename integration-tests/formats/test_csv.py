"""CSV, end to end, in one file.

Writes its own CSV files, opens an embedded Beacon over them, queries them, creates an external
table, reopens the database, and checks the table survived.

    pytest formats/test_csv.py -v
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")

ROWS = 20
PLATFORMS = ["SHIP", "BUOY", "GLIDER", "FLOAT"]


def _body(delimiter: str = ",") -> str:
    return "".join(
        f"{i}{delimiter}{PLATFORMS[i % 4]}{delimiter}{i * 1.5}\n" for i in range(ROWS)
    )


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every CSV file this module queries. CSV is text, so it is written as text."""
    root = tmp_path_factory.mktemp("csv")

    (root / "header.csv").write_text("id,platform,value\n" + _body(), encoding="utf-8")
    (root / "no_header.csv").write_text(_body(), encoding="utf-8")
    (root / "semicolon.csv").write_text("id;platform;value\n" + _body(";"), encoding="utf-8")

    # Quoted fields: a delimiter, an escaped quote and a newline, each inside a value.
    (root / "quoted.csv").write_text(
        'id,label,value\n'
        '1,"Ship, RV Meteor",1.5\n'
        '2,"a ""quoted"" name",2.5\n'
        '3,"two\nlines",3.5\n',
        encoding="utf-8",
    )

    # Empty fields, in a text column and a numeric one.
    (root / "nulls.csv").write_text(
        "id,platform,value\n"
        + "".join(
            f"{i},{'' if i % 4 == 1 else PLATFORMS[i % 4]},{'' if i % 3 == 0 else i * 1.5}\n"
            for i in range(ROWS)
        ),
        encoding="utf-8",
    )

    # A column that looks integer for 18 rows and then is not.
    (root / "mixed.csv").write_text(
        "id,reading\n"
        + "".join(f"{i},{i * 2}\n" for i in range(ROWS - 2))
        + f"{ROWS - 2},n/a\n{ROWS - 1},{(ROWS - 1) * 2}\n",
        encoding="utf-8",
    )

    (root / "gzipped.csv.gz").write_bytes(
        gzip.compress(("id,platform,value\n" + _body()).encode("utf-8"))
    )

    parts = root / "parts"
    parts.mkdir()
    (parts / "a.csv").write_text("id,value\n1,10.0\n2,20.0\n", encoding="utf-8")
    (parts / "b.csv").write_text("id,value\n3,30.0\n4,40.0\n", encoding="utf-8")
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_file_reads(con):
    rows = con.sql("SELECT id, platform, value FROM read_csv('header.csv') ORDER BY id LIMIT 2").fetchall()
    assert rows == [(0, "SHIP", 0.0), (1, "BUOY", 1.5)]


def test_the_types_are_inferred(con):
    """CSV states no types, so the inferred ones are the answer."""
    relation = con.sql("SELECT * FROM read_csv('header.csv')")
    assert relation.columns == ["id", "platform", "value"]
    assert relation.types == ["Int64", "Utf8", "Float64"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_csv('header.csv')").fetchall() == [(ROWS,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        "SELECT count(*) n, max(value) hi FROM read_csv('header.csv') WHERE value >= 15.0"
    ).fetchall()[0]
    assert got == (ROWS - 10, (ROWS - 1) * 1.5)


def test_the_delimiter_argument_is_honoured(con):
    """A semicolon file read with `';'` gives the same rows as the comma file."""
    order = "ORDER BY id"
    comma = con.sql(f"SELECT id, value FROM read_csv('header.csv') {order}").fetchall()
    semi = con.sql(f"SELECT id, value FROM read_csv('semicolon.csv', ';') {order}").fetchall()
    assert comma == semi


def test_the_wrong_delimiter_gives_one_column(con):
    """Forgetting the delimiter returns a table, not an error: one column, right row count."""
    relation = con.sql("SELECT * FROM read_csv('semicolon.csv')")
    assert relation.columns == ["id;platform;value"]


def test_a_quoted_field_keeps_its_delimiter_quote_and_newline(con):
    rows = con.sql("SELECT id, label FROM read_csv('quoted.csv') ORDER BY id").fetchall()
    assert len(rows) == 3, "a newline inside quotes must not split the record"
    assert rows[0][1] == "Ship, RV Meteor"
    assert rows[1][1] == 'a "quoted" name'
    assert rows[2][1] == "two\nlines"


def test_an_empty_field_is_a_null(con):
    total, values, platforms = con.sql(
        "SELECT count(*), count(value), count(platform) FROM read_csv('nulls.csv')"
    ).fetchall()[0]
    assert total == ROWS
    assert values < total, "an empty numeric field must not read as a number"
    assert platforms < total, 'an empty text field must not read as ""'

    zeros = con.sql("SELECT count(*) AS n FROM read_csv('nulls.csv') WHERE value = 0.0").fetchall()[0][0]
    assert zeros == 0, "a null must not read as 0.0"


def test_a_late_string_widens_the_whole_column(con):
    """`n/a` at row 18 of 20 makes the column text, whatever the inference sample."""
    for sample in (5, 1000):
        relation = con.sql(f"SELECT reading FROM read_csv('mixed.csv', ',', {sample})")
        assert relation.types == ["Utf8"], f"with infer_records={sample}"
        assert len(relation.fetchall()) == ROWS


# --- many files ---------------------------------------------------------------


def test_a_glob_reads_every_file(con):
    assert con.sql("SELECT count(*) AS n FROM read_csv('parts/*.csv')").fetchall() == [(4,)]


def test_a_glob_column_order_is_stable(con):
    orders = {tuple(con.sql("SELECT * FROM read_csv('parts/*.csv')").columns) for _ in range(5)}
    assert len(orders) == 1, f"the column order changed between runs: {orders}"


# --- what is not supported ----------------------------------------------------


def test_a_headerless_file_loses_its_first_row(con):
    """`read_csv` takes no header flag, so the first data row becomes the column names.

    Recorded rather than wished away: nothing in a CSV file says whether it has a header, and
    there is no argument to say so. 19 rows instead of 20, and the names come from the data.
    """
    relation = con.sql("SELECT * FROM read_csv('no_header.csv')")
    assert len(relation.fetchall()) == ROWS - 1
    assert relation.columns == ["0", "SHIP", "0.0"]


def test_a_compressed_file_is_refused_rather_than_misread(con):
    """`read_csv` does not decompress, and fails rather than returning rows of mojibake."""
    with pytest.raises(Exception) as refusal:
        con.sql("SELECT * FROM read_csv('gzipped.csv.gz')").fetchall()
    assert "utf-8" in str(refusal.value).lower() or "csv" in str(refusal.value).lower()


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'header.csv'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
    assert "obs" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'header.csv'")
        con.execute("CREATE EXTERNAL TABLE parts STORED AS CSV LOCATION 'parts/*.csv'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
        assert con.sql("SELECT count(*) AS n FROM parts").fetchall() == [(4,)]
        assert {"obs", "parts"} <= set(con.list_tables())
        assert con.sql("SELECT id, value FROM obs ORDER BY id LIMIT 1").fetchall() == [(0, 0.0)]
