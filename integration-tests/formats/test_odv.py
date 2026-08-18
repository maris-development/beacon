"""ODV ASCII, end to end, in one file.

ODV is the exchange format of Beacon's users. It has no Python writer, so the files are written
as text, which is what they are. Opens an embedded Beacon over them and queries them.

    pytest formats/test_odv.py -v

There is no `STORED AS ODV`, so this format has no external table and no restart case. The
tests that would need one are absent rather than skipped, and the reason is here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")

LEVELS = 4
STATIONS = 3

#: The columns declared in the header, and the type each one asks for.
DECLARED = [
    ("Longitude [degrees_east]", "FLOAT"),
    ("Latitude [degrees_north]", "FLOAT"),
    ("Depth [m]", "FLOAT"),
    ("Temperature [degC]", "FLOAT"),
]
#: The reader always adds these four, and strips the unit suffix off every declared label.
COLUMNS = ["Cruise", "Station", "Type", "yyyy-mm-ddThh:mm:ss.sss",
           *[label.rsplit(" [", 1)[0] for label, _ in DECLARED]]


def _document(stations: int, *, repeat_metadata: bool = False, blank_at: int | None = None) -> str:
    """An ODV spreadsheet.

    `repeat_metadata` writes a station's metadata on every row instead of only its first, which
    is the other legal spelling of the same dataset.
    """
    lines = [
        "//<Encoding>UTF-8</Encoding>",
        "//<Version>ODV Spreadsheet V4.8</Version>",
        "//<DataType>Profiles</DataType>",
        "//",
        '//<MetaVariable>label="Cruise" var_type="METACRUISE" value_type="INDEXED_TEXT" '
        'qf_schema="SEADATANET" comment=""</MetaVariable>',
        '//<MetaVariable>label="Station" var_type="METASTATION" value_type="INDEXED_TEXT" '
        'qf_schema="SEADATANET" comment=""</MetaVariable>',
        '//<MetaVariable>label="Type" var_type="METATYPE" value_type="TEXT:2" '
        'qf_schema="SEADATANET" comment=""</MetaVariable>',
    ]
    for label, value_type in DECLARED:
        lines.append(
            f'//<DataVariable>label="{label}" var_type="METABASIC" value_type="{value_type}" '
            f'qf_schema="SEADATANET" comment=""</DataVariable>'
        )
    lines.append("\t".join(COLUMNS))

    for station in range(stations):
        for level in range(LEVELS):
            meta = level == 0 or repeat_metadata
            temperature = "" if level == blank_at else f"{18.0 - level * 0.5:.1f}"
            lines.append("\t".join([
                "CRUISE-1" if meta else "",
                f"ST-{station}" if meta else "",
                "B" if meta else "",
                f"2020-01-0{station + 1}T00:00:00.000" if meta else "",
                f"{4.0 + station:.3f}" if meta else "",
                f"{52.0 + station:.3f}" if meta else "",
                f"{level * 10.0:.1f}",
                temperature,
            ]))
    return "\n".join(lines) + "\n"


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every ODV file this module queries."""
    root = tmp_path_factory.mktemp("odv")
    (root / "one_station.txt").write_text(_document(1), encoding="utf-8")
    (root / "many_stations.txt").write_text(_document(STATIONS), encoding="utf-8")
    # The same three stations, metadata repeated on every row.
    (root / "repeated.txt").write_text(_document(STATIONS, repeat_metadata=True), encoding="utf-8")
    # An empty temperature cell, which is ODV's absent value.
    (root / "absent.txt").write_text(_document(1, blank_at=1), encoding="utf-8")
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_file_reads(con):
    rows = con.sql(
        'SELECT "Depth", "Temperature" FROM read_odv_ascii(\'one_station.txt\') ORDER BY "Depth"'
    ).fetchall()
    assert rows == [(0.0, 18.0), (10.0, 17.5), (20.0, 17.0), (30.0, 16.5)]


def test_the_declared_types_are_used(con):
    """A `value_type` in the header picks the Arrow type, and the unit suffix is stripped."""
    relation = con.sql("SELECT * FROM read_odv_ascii('one_station.txt')")
    for column in COLUMNS:
        assert column in relation.columns, column
    types = dict(zip(relation.columns, relation.types))
    assert types["Cruise"] == "Utf8"
    assert types["Depth"] == "Float32"
    assert types["yyyy-mm-ddThh:mm:ss.sss"] == "Timestamp(ms)"


def test_the_row_count(con):
    """`count(<column>)` rather than `count(*)`: see the last test in this file."""
    n = con.sql('SELECT count("Depth") AS n FROM read_odv_ascii(\'many_stations.txt\')').fetchall()
    assert n == [(STATIONS * LEVELS,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        'SELECT count("Depth") n, max("Depth") deepest FROM read_odv_ascii(\'many_stations.txt\') '
        'WHERE "Depth" >= 20.0'
    ).fetchall()[0]
    assert got == (STATIONS * 2, 30.0)


def test_the_first_row_of_a_station_carries_its_metadata(con):
    rows = con.sql(
        'SELECT "Cruise", "Station", "Longitude" FROM read_odv_ascii(\'one_station.txt\') '
        'ORDER BY "Depth" LIMIT 1'
    ).fetchall()
    assert rows == [("CRUISE-1", "ST-0", 4.0)]


def test_an_empty_data_cell_is_a_null(con):
    """An absent measurement reads null; the rows around it are unaffected."""
    rows = con.sql(
        'SELECT "Temperature" FROM read_odv_ascii(\'absent.txt\') ORDER BY "Depth"'
    ).fetchall()
    assert [r[0] for r in rows] == [18.0, None, 17.0, 16.5]


# --- what is not supported ----------------------------------------------------


def test_a_station_does_not_carry_its_metadata_down_its_rows(con):
    """ODV writes a station's metadata once and it belongs to every row of that station.

    Beacon reads null on a continuation row, so a 4-level station carries its cruise, station
    and position on 1 row of 4. Recorded as it behaves: the two files below hold the same
    dataset, one with the metadata written once and one with it repeated, and they differ.
    """
    once = con.sql(
        'SELECT "Cruise" FROM read_odv_ascii(\'many_stations.txt\') ORDER BY "Station", "Depth"'
    ).fetchall()
    repeated = con.sql(
        'SELECT "Cruise" FROM read_odv_ascii(\'repeated.txt\') ORDER BY "Station", "Depth"'
    ).fetchall()

    assert repeated == [("CRUISE-1",)] * (STATIONS * LEVELS), "the repeated file is fully populated"
    assert once != repeated, "the same dataset, two spellings, two answers"
    assert once.count(("CRUISE-1",)) == STATIONS, "only each station's first row keeps it"


def test_count_star_fails(con):
    """`count(*)` fails on every ODV file, including the one committed in the repository.

    An empty projection leaves the decoder with no column to decode. `count(<column>)` works and
    `SELECT *` works, so the row count is reachable and only its commonest spelling is not.
    """
    with pytest.raises(Exception) as refusal:
        con.sql("SELECT count(*) AS n FROM read_odv_ascii('one_station.txt')").fetchall()
    assert "ODV" in str(refusal.value)


def test_the_attribute_column_order_is_not_stable(con):
    """The `.units` and `.qf_schema` columns come back in a different order on almost every run.

    The metadata is collected in a `HashMap`, and Rust randomizes its iteration order per
    process, so the order reaches the schema. This is the defect #377 fixed for five formats. A
    client that reads by column position gets different data between runs.

    Asserted as it behaves, and the data columns are checked to be stable, which is what makes
    it survivable: a caller naming its columns gets the right answer.
    """
    orders = {tuple(con.sql("SELECT * FROM read_odv_ascii('one_station.txt')").columns) for _ in range(8)}
    assert len(orders) > 1, (
        "the column order was stable over 8 runs. If this now holds, the HashMap ordering was "
        "fixed and this test should become an equality assertion."
    )
    for order in orders:
        assert list(order[: len(COLUMNS)]) == COLUMNS, "the data columns must stay put"


def test_there_is_no_external_table_format_for_odv(con):
    """`STORED AS ODV` is not a format, so an ODV file cannot become an external table.

    That is why this file has no restart case. Recorded here so the absence is deliberate
    rather than an oversight.
    """
    with pytest.raises(Exception) as refusal:
        con.execute("CREATE EXTERNAL TABLE stations STORED AS ODV LOCATION 'one_station.txt'")
    assert "FileFormat" in str(refusal.value) or "format" in str(refusal.value).lower()
