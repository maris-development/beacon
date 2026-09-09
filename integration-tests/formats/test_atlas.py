"""Atlas, end to end, in one file.

Writes its own Atlas collections, opens an embedded Beacon over them, queries them, creates an
external table, reopens the database, and checks the table survived.

    pytest formats/test_atlas.py -v

Needs `atlas-python`, `netCDF4` and the `beacondb` extension. All are skipped cleanly if absent.

An Atlas collection is one write-once file, `data.atlas`, holding many datasets. `atlas.create`
builds one from a directory of netCDF files: one dataset per file, named after the file. A
`LOCATION` therefore names the container, not the directory around it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
atlas = pytest.importorskip("atlas", reason="pip install atlas-python")
pytest.importorskip("netCDF4", reason="pip install netCDF4")

import numpy as np  # noqa: E402
from netCDF4 import Dataset  # noqa: E402

#: netCDF4 1.7.4 sets `.shape` on a numpy array, which numpy 2.5 deprecated. It comes from
#: inside the library on every write and there is no writer call that avoids it.
pytestmark = pytest.mark.filterwarnings(
    "ignore:Setting the shape on a NumPy array has been deprecated"
)

ROWS = 8
#: Datasets per collection, one per source file.
FILES = 5
TOTAL_ROWS = ROWS * FILES


def _source_file(path: Path, index: int) -> None:
    """One netCDF file: `temperature` and `depth` over `obs`, plus attributes.

    File `i` covers temperatures `[10i, 10i + 7]`, so each dataset lands in a range of its own
    and a threshold has an answer that can be written down.
    """
    with Dataset(path, "w", format="NETCDF4") as ds:
        ds.createDimension("obs", ROWS)
        temperature = ds.createVariable("temperature", "f4", ("obs",))
        temperature[:] = np.arange(ROWS, dtype="float32") + index * 10
        temperature.units = "celsius"
        depth = ds.createVariable("depth", "f4", ("obs",))
        depth[:] = np.arange(ROWS, dtype="float32") * 10.0
        ds.platform = f"p{index}"


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Build every collection this module queries."""
    root = tmp_path_factory.mktemp("atlas")

    # The netCDF files `atlas.create` ingests. They are not queried themselves.
    source = tmp_path_factory.mktemp("atlas-source")
    for index in range(FILES):
        _source_file(source / f"d{index}.nc", index)

    atlas.create(source, root / "obs")

    # A second collection under a nested prefix, so a glob has something to cover.
    nested = tmp_path_factory.mktemp("atlas-source-nested")
    _source_file(nested / "extra.nc", FILES)
    atlas.create(nested, root / "more" / "obs")

    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_collection_reads_every_dataset(con):
    """One dataset per source file, and every row of each."""
    assert con.sql(
        "SELECT count(*) AS n FROM read_atlas('obs/data.atlas')"
    ).fetchall() == [(TOTAL_ROWS,)]


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT temperature, depth FROM read_atlas('obs/data.atlas')")
    assert relation.columns == ["temperature", "depth"]


def test_values_read_back(con):
    rows = con.sql(
        "SELECT temperature, depth FROM read_atlas('obs/data.atlas') "
        "ORDER BY temperature LIMIT 2"
    ).fetchall()
    assert rows == [(0.0, 0.0), (1.0, 10.0)]


def test_a_filter_and_an_aggregate(con):
    """File `i` covers `[10i, 10i + 7]`, so `>= 20` keeps the last three files whole."""
    got = con.sql(
        "SELECT count(*) n, min(temperature) lo, max(temperature) hi "
        "FROM read_atlas('obs/data.atlas') WHERE temperature >= 20"
    ).fetchall()[0]
    assert got == (ROWS * 3, 20.0, 47.0)


def test_a_predicate_nothing_meets_returns_nothing(con):
    """Every dataset is ruled out by its own statistics, and none is opened."""
    assert con.sql(
        "SELECT count(*) AS n FROM read_atlas('obs/data.atlas') WHERE temperature > 100000"
    ).fetchall() == [(0,)]


def test_two_reads_return_the_same_rows(con):
    """Datasets are read in parallel and land in completion order, so compare with ORDER BY."""
    query = "SELECT temperature FROM read_atlas('obs/data.atlas') ORDER BY temperature"
    assert con.sql(query).fetchall() == con.sql(query).fetchall()


def test_a_glob_covers_several_collections(con):
    assert con.sql(
        "SELECT count(*) AS n FROM read_atlas('**/data.atlas')"
    ).fetchall() == [(TOTAL_ROWS + ROWS,)]


# --- attributes ---------------------------------------------------------------


def test_a_variable_attribute_is_a_column(con):
    """A per-array attribute is `{array}.{attr}`, as it is for netCDF and Zarr."""
    units = con.sql(
        'SELECT DISTINCT "temperature.units" AS u FROM read_atlas(\'obs/data.atlas\')'
    ).fetchall()
    assert units == [("celsius",)]


def test_a_dataset_attribute_is_a_column_under_a_dot(con):
    """A collection-level attribute of the source file becomes `.{attr}`.

    The leading dot is what keeps an attribute from colliding with an array of the same name.
    """
    platforms = con.sql(
        'SELECT DISTINCT ".platform" AS p FROM read_atlas(\'obs/data.atlas\') ORDER BY p'
    ).fetchall()
    assert platforms == [(f"p{i}",) for i in range(FILES)]


def test_an_attribute_predicate_selects_one_dataset(con):
    """An attribute is exact in the footer, so a predicate on one reaches its dataset alone."""
    got = con.sql(
        "SELECT count(*) n, min(temperature) lo, max(temperature) hi "
        "FROM read_atlas('obs/data.atlas') WHERE \".platform\" = 'p3'"
    ).fetchall()[0]
    assert got == (ROWS, 30.0, 37.0)


# --- the schema function ------------------------------------------------------


def test_the_schema_function_reports_the_columns(con):
    columns = {
        row[0]
        for row in con.sql(
            "SELECT column_name FROM read_atlas_schema('obs/data.atlas')"
        ).fetchall()
    }
    assert {"temperature", "depth", "temperature.units", ".platform"} <= columns


# --- the external table -------------------------------------------------------


def test_an_external_table_names_the_container(con):
    """A `LOCATION` points at `data.atlas` itself, not at the directory holding it."""
    con.execute("CREATE EXTERNAL TABLE obs STORED AS ATLAS LOCATION 'obs/data.atlas'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(TOTAL_ROWS,)]
    assert "obs" in con.list_tables()


def test_an_external_table_takes_a_glob(con):
    con.execute("CREATE EXTERNAL TABLE every STORED AS ATLAS LOCATION '**/data.atlas'")
    assert con.sql("SELECT count(*) AS n FROM every").fetchall() == [(TOTAL_ROWS + ROWS,)]


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS ATLAS LOCATION 'obs/data.atlas'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(TOTAL_ROWS,)]
        assert "obs" in con.list_tables()
        rows = con.sql("SELECT temperature FROM obs ORDER BY temperature LIMIT 1").fetchall()
        assert rows == [(0.0,)]


def test_pruning_can_be_turned_off_per_table(datasets, tmp_path):
    """The switch changes what is read, never what is returned."""
    with beacondb.connect(str(tmp_path / "options.db"), datasets=str(datasets)) as con:
        con.execute(
            "CREATE EXTERNAL TABLE pruned STORED AS ATLAS LOCATION 'obs/data.atlas' "
            "OPTIONS ('use_pruning' 'true')"
        )
        con.execute(
            "CREATE EXTERNAL TABLE whole STORED AS ATLAS LOCATION 'obs/data.atlas' "
            "OPTIONS ('use_pruning' 'false')"
        )
        query = "SELECT temperature FROM {} WHERE temperature >= 20 ORDER BY temperature"
        assert con.sql(query.format("pruned")).fetchall() == con.sql(
            query.format("whole")
        ).fetchall()


# --- what is not supported ----------------------------------------------------


def test_a_stale_collection_is_passed_over(con, datasets):
    """Atlas before 0.16 was a directory behind an `atlas.json` registry.

    This build reads container version 8 alone, which Atlas 0.17 writes. Such a directory
    holds no container at all, so nothing recognises it and no row of it reaches a query.
    Whether that surfaces as an error or as an empty result is not the point and not asserted —
    what matters is that a stale collection is never half-read as if it were a current one.
    """
    legacy = datasets / "legacy"
    legacy.mkdir(exist_ok=True)
    (legacy / "atlas.json").write_text("{}")

    try:
        rows = con.sql("SELECT * FROM read_atlas('legacy/atlas.json')").fetchall()
    except Exception:
        return
    assert rows == [], "a legacy directory holds no container, so it contributes no rows"
