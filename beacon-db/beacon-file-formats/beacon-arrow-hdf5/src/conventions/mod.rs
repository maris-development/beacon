//! Layout conventions read on top of the HDF5 container.
//!
//! # What a convention is
//!
//! HDF5 is a container. It says how bytes are stored and nothing about what
//! they mean. netCDF adds a data model on top, and CF adds meaning on top of
//! that, which is why [`beacon_arrow_netcdf::oxcdf_reader::compat`] can decode a
//! packed variable or a CF time without being told.
//!
//! An instrument writes HDF5 directly and follows neither. Its own layout is
//! regular all the same: one vendor's files always put the payload in one
//! place, the description of each axis in another, and the scale of the values
//! in a third. A convention here reads that layout.
//!
//! # Every convention is opt-in
//!
//! [`Hdf5Convention::None`](crate::Hdf5Convention::None) is the default, so a
//! read inspects no file for a convention it may not follow. A table asks for
//! one by name:
//!
//! ```sql
//! CREATE EXTERNAL TABLE das STORED AS HDF5 LOCATION 'acquisition/*.hdf5'
//! OPTIONS ('convention' = 'optodas');
//! ```
//!
//! Off, this module costs one comparison per file. On, it costs the few hundred
//! metadata bytes the check reads.
//!
//! # What a convention may do
//!
//! Two things, and no more:
//!
//! 1. **Name the axes.** It returns a rename for each axis it recognises, which
//!    joins the map the reader already applies. Nothing is rebuilt for it.
//! 2. **Add what the file implies.** A coordinate the file describes but does
//!    not store, and a payload decoded through the scale the file records.
//!
//! A convention never renames a column the file itself names, never drops one,
//! and never fails a read: a file that does not match is read plainly, with one
//! warning that names the check that failed.

/// An ASN OptoDAS acquisition file.
pub mod optodas;
