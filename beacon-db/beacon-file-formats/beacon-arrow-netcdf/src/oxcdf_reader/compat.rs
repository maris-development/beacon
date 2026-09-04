//! Conversion from [`oxcdf`] variables and attributes to ND arrays.
//!
//! This is the [`oxcdf`] twin of [`crate::compat`]. It applies the same CF
//! rules, in the same order, so both readers give a variable the same logical
//! type:
//!
//! 1. `scale_factor` / `add_offset` packing decodes to `f64`.
//! 2. A CF `units` string decodes to a nanosecond timestamp.
//! 3. Everything else keeps the type the file stores.
//!
//! Rule 1 wins over rule 2. A variable carries one or the other, never both.

use std::sync::Arc;

use beacon_nd_array::{datatypes::NdArrayType, NdArray, NdArrayD};
use num_traits::AsPrimitive;
use oxcdf::{AsyncNetcdfFile, AsyncVariable, AttributeValue, DType};

use crate::{
    backend::AttributeBackend,
    decoders::cf_time::parse_time_units,
    dimensions::PhonyDimensions,
    oxcdf_reader::backend::{
        NumericBackend, ScaleOffsetBackend, StringBackend, StringSource, TimestampBackend,
        VariableRef,
    },
};

/// Take the `_FillValue` attribute only when it is stored as the variable's own
/// type.
///
/// A netCDF writer gives `_FillValue` the type of its variable. Both readers
/// keep to that rule, so both null out the same cells.
macro_rules! fill_value_of {
    ($variable:expr, $variant:ident) => {
        match $variable.attribute("_FillValue").map(|a| &a.value) {
            Some(AttributeValue::$variant(value)) => Some(*value),
            _ => None,
        }
    };
}

/// Interpret a scalar attribute (`scale_factor`, `add_offset`) as an `f64`.
///
/// Returns `None` for a textual or multi-valued attribute.
fn attribute_as_f64(attribute: &AttributeValue) -> Option<f64> {
    if attribute.len() != 1 {
        return None;
    }
    // `as_f64` rejects the textual and raw variants for us.
    attribute.as_f64()
}

/// The `_FillValue` attribute as a string, for a string-typed variable.
fn string_fill_value(variable: &AsyncVariable<'_>) -> Option<String> {
    match variable.attribute("_FillValue").map(|a| &a.value) {
        Some(AttributeValue::Str(fill)) => Some(fill.clone()),
        _ => None,
    }
}

/// Build the ND array of a numeric variable, with the CF transforms applied.
fn numeric_variable_to_nd_array<T>(
    variable: VariableRef,
    fill_value: Option<T>,
    cf_time_epoch_unit: Option<(hifitime::Epoch, hifitime::Unit)>,
    scale_offset: Option<(f64, f64)>,
) -> anyhow::Result<Arc<dyn NdArrayD>>
where
    T: NdArrayType + oxcdf::Element + AsPrimitive<f64>,
{
    if let Some((scale, offset)) = scale_offset {
        let raw_fill = fill_value.map(|f| f.as_());
        let backend = ScaleOffsetBackend::new(variable, scale, offset, raw_fill);
        return Ok(Arc::new(NdArray::new_with_backend(backend)?));
    }

    // The `_FillValue` of a time variable decodes with the same arithmetic as
    // the data, so a fill cell nulls out instead of reaching a query as a real
    // date.
    if let Some((epoch, unit)) = cf_time_epoch_unit {
        let raw_fill = fill_value.map(|f| f.as_());
        let backend = TimestampBackend::new(variable, epoch, unit, raw_fill);
        return Ok(Arc::new(NdArray::new_with_backend(backend)?));
    }

    let backend = NumericBackend::new(variable, fill_value);
    Ok(Arc::new(NdArray::new_with_backend(backend)?))
}

/// Convert an [`oxcdf`] attribute value into a rank-0 ND array.
///
/// Returns an error when the attribute type has no ND equivalent.
pub fn attribute_to_nd_array(
    attribute_name: &str,
    attribute_value: &AttributeValue,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    /// One scalar, or a one-element list of the same type.
    macro_rules! scalar {
        ($single:ident, $plural:ident) => {
            match attribute_value {
                AttributeValue::$single(value) => Some(value.clone()),
                AttributeValue::$plural(values) if values.len() == 1 => Some(values[0].clone()),
                _ => None,
            }
        };
    }

    /// Wrap a scalar in an ND array as soon as one of the arms matches.
    macro_rules! try_scalar {
        ($($single:ident / $plural:ident),* $(,)?) => {
            $(
                if let Some(value) = scalar!($single, $plural) {
                    return Ok(Arc::new(NdArray::new_with_backend(AttributeBackend::new(value))?));
                }
            )*
        };
    }

    try_scalar!(
        Uchar / Uchars,
        Schar / Schars,
        Ushort / Ushorts,
        Short / Shorts,
        Uint / Uints,
        Int / Ints,
        Ulonglong / Ulonglongs,
        Longlong / Longlongs,
        Float / Floats,
        Double / Doubles,
        Str / Strs,
    );

    Err(anyhow::anyhow!(
        "Unsupported attribute type for attribute '{}'",
        attribute_name
    ))
}

/// Convert an [`oxcdf`] variable into a lazy ND array.
///
/// The conversion handles:
/// - CF-time numeric variables (`units` attribute).
/// - CF `scale_factor` / `add_offset` packing.
/// - netCDF string variables and char arrays with a trailing length dimension.
/// - `_FillValue` for the numeric and string types.
///
/// `phony` renames the dimensions netCDF invented for a file that names none,
/// so two groups of one file broadcast against each other. Pass
/// [`PhonyDimensions::none`] to keep the names the reader gave. A NetCDF-4 file
/// names every dimension, so the argument does nothing there.
pub fn variable_to_nd_array(
    file: Arc<AsyncNetcdfFile>,
    variable: &AsyncVariable<'_>,
    phony: &PhonyDimensions,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    variable_to_nd_array_packed(file, variable, phony, None)
}

/// [`variable_to_nd_array`] with the packing supplied rather than read.
///
/// CF puts `scale_factor` and `add_offset` on the variable itself, and the
/// function above reads them there. A vendor layout can put the same numbers
/// somewhere else — an OptoDAS file keeps one `dataScale` for its payload in
/// another group — and `packing` carries them in from wherever the convention
/// found them. It wins over the attributes of the variable.
pub fn variable_to_nd_array_packed(
    file: Arc<AsyncNetcdfFile>,
    variable: &AsyncVariable<'_>,
    phony: &PhonyDimensions,
    packing: Option<(f64, f64)>,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    // The path, not the leaf name. `AsyncNetcdfFile::variable` takes either for
    // a root variable, because it trims the leading slash, but only the path
    // reaches a variable inside a group. This reader stays in the root group;
    // `beacon-arrow-hdf5` walks every group with the same conversion.
    let name = variable.path.clone();
    let shape: Vec<usize> = variable.shape.iter().map(|&len| len as usize).collect();
    let dimensions = phony.apply(&variable.dimensions);

    // The optional CF `calendar` attribute selects how the reference date is
    // read. It defaults to Gregorian when absent.
    let calendar = variable
        .attribute("calendar")
        .and_then(|a| a.value.as_text())
        .map(|c| c.to_string());
    let cf_time_epoch_unit = variable
        .attribute("units")
        .and_then(|a| a.value.as_text())
        .and_then(|units| parse_time_units(units, calendar.as_deref()));

    // CF `scale_factor` / `add_offset` packing. A missing factor defaults to
    // the identity, as it does on the netcdf-c path.
    let scale = variable
        .attribute("scale_factor")
        .and_then(|a| attribute_as_f64(&a.value));
    let offset = variable
        .attribute("add_offset")
        .and_then(|a| attribute_as_f64(&a.value));
    let scale_offset = packing.or_else(|| {
        (scale.is_some() || offset.is_some()).then(|| (scale.unwrap_or(1.0), offset.unwrap_or(0.0)))
    });

    let variable_ref = || {
        VariableRef::new(
            file.clone(),
            name.clone(),
            shape.clone(),
            dimensions.clone(),
        )
    };

    match variable.vartype() {
        DType::Int(1) => numeric_variable_to_nd_array::<i8>(
            variable_ref(),
            fill_value_of!(variable, Schar),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Int(2) => numeric_variable_to_nd_array::<i16>(
            variable_ref(),
            fill_value_of!(variable, Short),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Int(4) => numeric_variable_to_nd_array::<i32>(
            variable_ref(),
            fill_value_of!(variable, Int),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Int(8) => numeric_variable_to_nd_array::<i64>(
            variable_ref(),
            fill_value_of!(variable, Longlong),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Uint(1) => numeric_variable_to_nd_array::<u8>(
            variable_ref(),
            fill_value_of!(variable, Uchar),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Uint(2) => numeric_variable_to_nd_array::<u16>(
            variable_ref(),
            fill_value_of!(variable, Ushort),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Uint(4) => numeric_variable_to_nd_array::<u32>(
            variable_ref(),
            fill_value_of!(variable, Uint),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Uint(8) => numeric_variable_to_nd_array::<u64>(
            variable_ref(),
            fill_value_of!(variable, Ulonglong),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Float(4) => numeric_variable_to_nd_array::<f32>(
            variable_ref(),
            fill_value_of!(variable, Float),
            cf_time_epoch_unit,
            scale_offset,
        ),
        DType::Float(8) => numeric_variable_to_nd_array::<f64>(
            variable_ref(),
            fill_value_of!(variable, Double),
            cf_time_epoch_unit,
            scale_offset,
        ),
        // A netCDF `string` variable, and a fixed-length string wider than one
        // byte, both hold one whole string in each element.
        DType::String | DType::FixedString(_) => {
            let backend = StringBackend::new(
                variable_ref(),
                StringSource::Native,
                string_fill_value(variable),
            );
            Ok(Arc::new(NdArray::new_with_backend(backend)?))
        }
        DType::Char => {
            // A char variable whose last dimension is named for a string length
            // holds one string in each row of that axis. The axis is the string
            // itself, so it stays out of the logical shape.
            let is_fixed_string_array = dimensions
                .last()
                .map(|dim| {
                    let dim = dim.to_lowercase();
                    dim.starts_with("string")
                        || dim.starts_with("strlen")
                        || dim.starts_with("strnlen")
                })
                .unwrap_or(false);

            let backend = if is_fixed_string_array {
                let width = *shape
                    .last()
                    .expect("a trailing dimension name implies a trailing axis");
                let logical = VariableRef::new(
                    file.clone(),
                    name.clone(),
                    shape[..shape.len() - 1].to_vec(),
                    dimensions[..dimensions.len() - 1].to_vec(),
                )
                .with_string_width(width);
                StringBackend::new(
                    logical,
                    StringSource::FixedChar,
                    string_fill_value(variable),
                )
            } else {
                StringBackend::new(
                    variable_ref(),
                    StringSource::Char,
                    string_fill_value(variable),
                )
            };

            Ok(Arc::new(NdArray::new_with_backend(backend)?))
        }
        other => Err(anyhow::anyhow!(
            "Unsupported variable type '{:?}' for variable '{}'",
            other,
            name
        )),
    }
}
