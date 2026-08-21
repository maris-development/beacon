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


def instrument() -> None:
    """A file shaped like the one an instrument writes.

    This is a miniature of a DAS acquisition file: one payload of two axes in
    the root group, a description of each channel in a second group, a sweep
    description in a third, and a pile of small metadata in a fourth.

    It carries the four cases the dimension rules turn on:

    * `header/channels` and `sweep/coeffs` are 4 long in two different groups.
      netCDF invents a dimension per group, so they broadcast against the
      payload only once beacon unifies them by length.
    * `sweep/coeffs` is 4 long and means something else. It joins the channel
      axis all the same, which is the trade-off that unification takes.
    * `header/sensitivities` is 1 x 1, so its second axis cannot take the name
      its first one holds.
    * `header/missing` is empty, so netCDF gives it a dimension of its own.

    The metadata of `instrument` outnumbers the payload on purpose: a grid
    chosen by variable count picks the 3-long axis and drops the payload, and a
    grid chosen by volume picks the payload.
    """
    with h5py.File(os.path.join(HERE, "instrument.h5"), "w") as f:
        # The payload: 6 samples of 4 channels, counting up so a test can see
        # which axis a column follows.
        f.create_dataset(
            "data", data=np.arange(24, dtype=np.int16).reshape(6, 4)
        )
        f.create_dataset("fileVersion", data=np.int32(3))

        header = f.create_group("header")
        # One value per channel, in a group of its own.
        header.create_dataset(
            "channels", data=np.array([0, 4, 8, 12], dtype=np.int32)
        )
        header.create_dataset(
            "distances", data=np.array([0.0, 1.5, 3.0, 4.5], dtype=np.float64)
        )
        header.create_dataset("dt", data=np.float64(0.008))
        header.create_dataset("unit", data="rad/(s*m)")
        header.create_dataset(
            "sensitivities", data=np.array([[8.25]], dtype=np.float64)
        )
        header.create_dataset(
            "missing", shape=(0,), maxshape=(None,), dtype=np.int32
        )

        sweep = f.create_group("sweep")
        # 6 long, which is the length of the payload's first axis.
        sweep.create_dataset(
            "window", data=np.linspace(0.0, 1.0, 6, dtype=np.float64)
        )
        # 4 long, which is the length of the channel axis. It counts something
        # else entirely.
        sweep.create_dataset(
            "coeffs", data=np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float64)
        )

        # Six variables on one 3-long axis. They outnumber every other grid.
        instrument = f.create_group("instrument")
        for index, name in enumerate(
            ["gains", "offsets", "delays", "temperatures", "states", "codes"]
        ):
            instrument.create_dataset(
                name, data=np.arange(3, dtype=np.float64) + index
            )


def optodas() -> None:
    """A miniature of an ASN OptoDAS acquisition file.

    This is `instrument.h5` plus the metadata that layout records about itself:
    the names and sizes of the payload axes, the range of each one, the start of
    the acquisition and the scale of its counts.

    The numbers are the ones a real file carries, at a scale a test can check by
    hand: 6 samples at 125 Hz from 2026-03-28T12:00:00Z, and 4 channels 4 raw
    channels apart, each 1.25 m wide.
    """
    with h5py.File(os.path.join(HERE, "optodas.h5"), "w") as f:
        # int16 counts, 6 samples of 4 channels.
        f.create_dataset("data", data=np.arange(24, dtype=np.int16).reshape(6, 4))
        f.create_dataset("fileVersion", data=np.int32(3))

        header = f.create_group("header")
        header.create_dataset("dimensionNames", data=["time", "distance"])
        header.create_dataset(
            "dimensionSizes", data=np.array([6, 4], dtype=np.int64)
        )
        header.create_dataset("dimensionUnits", data=["s", "m"])
        # 2026-03-28T12:00:00Z, and 125 Hz.
        header.create_dataset("time", data=np.float64(1774699200.0))
        header.create_dataset("dt", data=np.float64(0.008))
        header.create_dataset("dataScale", data=np.float64(0.5))
        header.create_dataset("unit", data="rad/(s*m)")
        header.create_dataset("gaugeLength", data=np.float64(4.0))
        header.create_dataset(
            "channels", data=np.array([0, 4, 8, 12], dtype=np.int32)
        )
        # An acquisition with no gap. A file that reports one gets no clock.
        header.create_dataset(
            "missingSamples", shape=(0,), maxshape=(None,), dtype=np.int32
        )

        ranges = header.create_group("dimensionRanges")
        # The sample axis: 6 samples, one unit each.
        first = ranges.create_group("dimension0")
        first.create_dataset("name", data="time")
        first.create_dataset("min", data=np.int64(0))
        first.create_dataset("max", data=np.int64(5))
        first.create_dataset("size", data=np.int64(6))
        first.create_dataset("unitScale", data=np.float64(0.008))
        # The distance axis: 4 positions, 4 raw channels apart, 1.25 m each.
        second = ranges.create_group("dimension1")
        second.create_dataset("name", data="distance")
        second.create_dataset("min", data=np.int64(0))
        second.create_dataset("max", data=np.int64(12))
        second.create_dataset("size", data=np.int64(4))
        second.create_dataset("unitScale", data=np.float64(1.25))

        options = f.create_group("instrumentOptions")
        options.create_dataset("names", data=["productCode", "variant"])
        options.create_dataset("values", data=["OptoDAS C01", "S"])

        # The clock corrections this layer does not apply. They stay readable.
        timing = f.create_group("timing")
        timing.create_dataset(
            "ppses", data=np.array([0.548, 1.548], dtype=np.float64)
        )
        timing.create_dataset("sampleDelayPPS", data=np.float64(0.0001))


if __name__ == "__main__":
    nested_groups()
    compound()
    instrument()
    optodas()
    print("wrote nested-groups.h5, compound.h5, instrument.h5 and optodas.h5")
