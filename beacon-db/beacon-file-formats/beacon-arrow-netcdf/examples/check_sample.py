"""Check the files of the write_sample example with netCDF4 for Python.

Usage: python check_sample.py <folder>

The script prints one line per check. It exits 0 when all checks pass, and 1 when a check fails.
"""

import os
import sys

import netCDF4
import numpy

passed = 0
failed = 0


def check(label, condition):
    """Print the result of one check and count it."""
    global passed, failed
    try:
        ok, detail = condition()
    except Exception as error:
        ok, detail = False, f"{type(error).__name__}: {error}"
    if ok:
        passed += 1
    else:
        failed += 1
    print(f"{'PASS' if ok else 'FAIL'} {label}: {detail}")


def equal(actual, expected):
    """Compare two values and give the actual value as the detail."""
    return actual == expected, repr(actual)


def check_flat(path, compressed):
    name = os.path.basename(path)
    with netCDF4.Dataset(path) as ds:
        print(f"INFO {name} data_model={ds.data_model} _NCProperties={_nc_properties(ds)!r}")
        obs = ds.dimensions.get("obs")
        check(f"{name} obs is unlimited", lambda: equal(obs.isunlimited(), True))
        check(f"{name} obs length", lambda: equal(len(obs), 3))

        types = {"i32": numpy.dtype("i4"), "f64": numpy.dtype("f8"), "name": str, "time": numpy.dtype("i8")}
        for variable, dtype in types.items():
            check(f"{name} {variable} type", lambda: equal(ds.variables[variable].dtype, dtype))
            check(f"{name} {variable} dimensions", lambda: equal(ds.variables[variable].dimensions, ("obs",)))

        values = {"i32": [1, 2, 3], "f64": [1.5, 2.5, 3.5], "name": ["a", "", "c"], "time": [0, 60, 120]}
        for variable, expected in values.items():
            check(f"{name} {variable} values", lambda: equal(ds.variables[variable][:].tolist(), expected))

        check(f"{name} title", lambda: equal(ds.getncattr("title"), "sample"))
        check(f"{name} comment exists", lambda: equal("comment" in ds.ncattrs(), True))
        check(f"{name} comment", lambda: equal(ds.getncattr("comment"), ""))
        check(f"{name} f64.units", lambda: equal(ds.variables["f64"].getncattr("units"), "m"))
        check(
            f"{name} time.units",
            lambda: equal(ds.variables["time"].getncattr("units"), "seconds since 1970-01-01T00:00:00Z"),
        )
        check(f"{name} name has no _FillValue", lambda: equal("_FillValue" in ds.variables["name"].ncattrs(), False))

        if compressed:
            for variable in ("i32", "f64", "time"):
                check(f"{name} {variable} filters", lambda: _deflate(ds.variables[variable].filters(), 4))
            # The writer does not compress a string variable.
            check(f"{name} name filters", lambda: equal(ds.variables["name"].filters().get("zlib"), False))
            for variable in ("i32", "f64", "name", "time"):
                check(f"{name} {variable} chunking", lambda: equal(ds.variables[variable].chunking(), [2]))


def check_nd(path):
    name = os.path.basename(path)
    with netCDF4.Dataset(path) as ds:
        print(f"INFO {name} data_model={ds.data_model} _NCProperties={_nc_properties(ds)!r}")
        ds.set_auto_mask(False)
        check(f"{name} lat length", lambda: equal(len(ds.dimensions["lat"]), 2))
        check(f"{name} lon length", lambda: equal(len(ds.dimensions["lon"]), 3))
        check(f"{name} lat values", lambda: equal(ds.variables["lat"][:].tolist(), [10, 20]))
        check(f"{name} lon values", lambda: equal(ds.variables["lon"][:].tolist(), [100, 200, 300]))

        temperature = ds.variables["temperature"]
        check(f"{name} temperature dimensions", lambda: equal(temperature.dimensions, ("lat", "lon")))
        check(f"{name} temperature type", lambda: equal(temperature.dtype, numpy.dtype("f8")))
        check(f"{name} temperature shape", lambda: equal(temperature.shape, (2, 3)))
        data = temperature[:]
        cells = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)]
        check(f"{name} temperature values", lambda: equal([float(data[cell]) for cell in cells], [1.0, 2.0, 3.0, 4.0, 5.0]))
        check(f"{name} missing cell", lambda: equal(float(data[1, 2]), float(numpy.finfo("f8").min)))


def _deflate(filters, level):
    """Check that `filters` is deflate at `level` with shuffle."""
    ok = filters.get("zlib") is True and filters.get("complevel") == level and filters.get("shuffle") is True
    return ok, repr(filters)


def _nc_properties(ds):
    """The _NCProperties attribute, or None when the file has none."""
    try:
        return ds.getncattr("_NCProperties")
    except AttributeError:
        return None


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    folder = sys.argv[1]
    print(f"INFO netCDF4 {netCDF4.__version__}, netCDF-C {netCDF4.__netcdf4libversion__}")
    check_flat(os.path.join(folder, "flat.nc"), compressed=False)
    check_flat(os.path.join(folder, "flat_compressed.nc"), compressed=True)
    check_nd(os.path.join(folder, "nd.nc"))
    print(f"INFO {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
