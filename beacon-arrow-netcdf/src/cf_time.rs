use std::str::FromStr;

use hifitime::{Epoch, Unit};
use ndarray::{ArrayBase, Dim, IxDynImpl, OwnedRepr};
use netcdf::{
    types::{FloatType, IntType},
    AttributeValue, Extents, Variable,
};
use regex::Regex;

use crate::nc_array::{Dimension, NetCDFNdArray, NetCDFNdArrayBase, NetCDFNdArrayInner};

pub fn calendar(variable: &Variable) -> Option<String> {
    variable
        .attributes()
        .find(|attr| attr.name() == "calendar")
        .map(|attr| {
            if let AttributeValue::Str(calendar) = attr.value()? {
                Ok(calendar.to_string())
            } else {
                anyhow::bail!("calendar attribute is not a string")
            }
        })?
        .ok()
}

pub fn units(variable: &Variable) -> Option<String> {
    variable
        .attributes()
        .find(|attr| attr.name() == "units")
        .map(|attr| {
            if let AttributeValue::Str(units) = attr.value()? {
                Ok(units.to_string())
            } else {
                anyhow::bail!("units attribute is not a string")
            }
        })?
        .ok()
}

fn is_cf_time_variable_impl(variable: &Variable) -> Option<(Unit, Epoch)> {
    let units = units(variable);
    // let calendar = calendar(variable);

    match units {
        Some(units) => {
            let units_l = units.to_lowercase();
            let units = extract_units(units_l.as_str());
            let epoch = extract_epoch(units_l.as_str());

            match (units, epoch) {
                (Some(units), Some(epoch)) => return Some((units, epoch)),
                _ => return None,
            }
        }
        _ => None,
    }
}

pub fn is_cf_time_variable(variable: &Variable) -> bool {
    is_cf_time_variable_impl(variable).is_some()
}

pub fn decode_cf_time_variable(variable: &Variable) -> anyhow::Result<Option<NetCDFNdArray>> {
    if let Some((units, epoch)) = is_cf_time_variable_impl(variable) {
        let (array, fill) = match variable.vartype() {
            netcdf::types::NcVariableType::Int(IntType::I8) => (
                Some(convert_nd_array(
                    variable.get::<i8, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<i8>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Int(IntType::I16) => (
                Some(convert_nd_array(
                    variable.get::<i16, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<i16>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Int(IntType::I32) => (
                Some(convert_nd_array(
                    variable.get::<i32, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<i32>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Int(IntType::I64) => (
                Some(convert_nd_array(
                    variable.get::<i64, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<i64>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Int(IntType::U8) => (
                Some(convert_nd_array(
                    variable.get::<u8, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<u8>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Int(IntType::U16) => (
                Some(convert_nd_array(
                    variable.get::<u16, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<u16>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Int(IntType::U32) => (
                Some(convert_nd_array(
                    variable.get::<u32, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<u32>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Int(IntType::U64) => (
                Some(convert_nd_array(
                    variable.get::<u64, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<u64>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Float(FloatType::F32) => (
                Some(convert_nd_array(
                    variable.get::<f32, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<f32>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            netcdf::types::NcVariableType::Float(FloatType::F64) => (
                Some(convert_nd_array(
                    variable.get::<f64, _>(Extents::All)?,
                    units,
                    epoch,
                )),
                variable
                    .fill_value::<f64>()?
                    .map(|v| convert_fill_value(v, units, epoch)),
            ),
            _ => (None, None),
        };

        if let Some(array) = array {
            let dims = variable
                .dimensions()
                .iter()
                .map(|d| Dimension {
                    name: d.name(),
                    size: d.len(),
                })
                .collect::<Vec<_>>();

            let inner_array = NetCDFNdArrayInner::TimestampSecond(NetCDFNdArrayBase {
                fill_value: fill,
                inner: array,
            });

            return Ok(Some(NetCDFNdArray::new(dims, inner_array)));
        }
    }
    Ok(None)
}

fn convert_fill_value<T: num_traits::cast::AsPrimitive<f64>>(
    fill_value: T,
    unit: Unit,
    epoch: Epoch,
) -> i64 {
    (epoch + (fill_value.as_() * unit)).to_unix_seconds() as i64
}

fn convert_nd_array<T: num_traits::cast::AsPrimitive<f64>>(
    mut base: ArrayBase<OwnedRepr<T>, Dim<IxDynImpl>>,
    unit: Unit,
    epoch: Epoch,
) -> ArrayBase<OwnedRepr<i64>, Dim<IxDynImpl>> {
    let shape = base.shape().to_vec();
    let data = base
        .iter_mut()
        .map(|v| epoch + (v.as_() * unit))
        .map(|v| v.to_unix_seconds() as i64)
        .collect::<Vec<_>>();

    ArrayBase::from_shape_vec(shape, data).unwrap()
}

fn extract_units(input: &str) -> Option<hifitime::Unit> {
    let re = Regex::new(r"^(?P<units>\w+) since").unwrap();
    re.captures(input)
        .map(|caps| match caps["units"].to_string().as_str() {
            "seconds" => Some(hifitime::Unit::Second),
            "milliseconds" => Some(hifitime::Unit::Millisecond),
            "microseconds" => Some(hifitime::Unit::Microsecond),
            "nanoseconds" => Some(hifitime::Unit::Nanosecond),
            "days" => Some(hifitime::Unit::Day),
            "weeks" => Some(hifitime::Unit::Week),
            _ => None,
        })
        .flatten()
}

/// Extracts the epoch date from a string like "days since -4713-11-24"
fn extract_epoch(input: &str) -> Option<Epoch> {
    let re = Regex::new(r"since (?P<epoch>-?\d{1,4}-\d{1,2}-\d{1,2})").unwrap();
    let result = re
        .captures(input)
        .map(|caps| {
            let epoch_str = caps["epoch"].to_string();
            let mut epoch = Epoch::from_str(&epoch_str).ok();

            if epoch.is_none() {
                if epoch_str == "-4713-01-01" {
                    epoch = Some(Epoch::from_jde_utc(0.0));
                }
            }

            epoch
        })
        .flatten();

    result
}
