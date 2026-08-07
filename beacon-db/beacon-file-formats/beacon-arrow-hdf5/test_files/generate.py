#!/usr/bin/env python3
"""Generate the plain-HDF5 fixtures this crate tests against.

The files hold no netCDF convention: no dimension scales, no `_NCProperties`,
no reserved attributes. That is the point. They cover the two layouts the
netCDF data model cannot express, which is why `beacon-arrow-hdf5` carries its
own reader.

The fixtures are committed, so a build needs neither Python nor h5py. Run this
only to change them:

    pip install h5py
    python3 generate.py
"""

import os

import h5py
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))


def nested_groups() -> None:
    """A file whose datasets live two group levels deep.

    Every first axis is 3 long, so the phony dimension names a reader invents
    for an HDF5 file with no dimension scales stay consistent across datasets.
    """
    with h5py.File(os.path.join(HERE, "nested-groups.h5"), "w") as f:
        f.attrs["title"] = "nested group example"
        # 8 bytes wide on purpose. A version-1 attribute message pads its value
        # block to 8 bytes, and `oxcdf` takes the padding as data, so a narrower
        # scalar comes back as two values and is dropped. See the note in
        # `reader::open`.
        f.attrs["version"] = np.int64(2)

        f.create_dataset("station_id", data=np.array([11, 22, 33], dtype=np.int32))

        obs = f.create_group("observations")
        obs.attrs["units"] = "degC"
        obs.create_dataset(
            "temperature",
            data=np.arange(12, dtype=np.float32).reshape(3, 4),
        )
        obs.create_dataset(
            "salinity",
            data=np.linspace(30.0, 35.5, 12, dtype=np.float64).reshape(3, 4),
        )

        qc = obs.create_group("qc")
        qc.create_dataset(
            "flag",
            data=np.arange(12, dtype=np.int8).reshape(3, 4),
        )


def compound() -> None:
    """A file holding one compound dataset.

    The last member is a variable-length string. A reader that models the
    fixed-width members must skip that one rather than fail the dataset.
    """
    dtype = np.dtype(
        [
            ("station", np.int32),
            ("depth", np.float32),
            ("temp", np.float64),
            ("label", "S8"),
            ("note", h5py.string_dtype()),
        ]
    )
    rows = np.array(
        [
            (1, 0.0, 12.5, b"alpha", "first"),
            (2, 10.0, 11.25, b"beta", "second"),
            (3, 20.0, 10.0, b"gamma", "third"),
            (4, 30.0, 9.75, b"delta", "fourth"),
        ],
        dtype=dtype,
    )

    with h5py.File(os.path.join(HERE, "compound.h5"), "w") as f:
        measurements = f.create_dataset("measurements", data=rows)
        measurements.attrs["description"] = "compound example"
        f.create_dataset("index", data=np.arange(4, dtype=np.int32))


if __name__ == "__main__":
    nested_groups()
    compound()
    print("wrote nested-groups.h5 and compound.h5")
