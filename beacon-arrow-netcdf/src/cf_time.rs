use std::str::FromStr;

use hifitime::Epoch;
use ndarray::{ArrayBase, Dim, IxDynImpl, OwnedRepr};
use netcdf::{types::IntType, AttributeValue, Extents, Variable};
use regex::Regex;

use crate::nc_array::NetCDFNdArray;

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

pub fn decode_cf_time_variable(variable: &Variable) -> Option<NetCDFNdArray> {
    let units = units(variable);
    let calendar = calendar(variable);

    match (units, calendar) {
        (Some(units), Some(calendar)) => {
            let calendar_l = calendar.to_lowercase();
            if calendar_l == "gregorian" || calendar_l == "standard" {
                let units_l = units.to_lowercase();
                let units = extract_units(units_l.as_str());
                let epoch = extract_epoch(units_l.as_str());

                match (units, epoch) {
                    (Some(units), Some(epoch)) => match variable.vartype() {
                        netcdf::types::NcVariableType::Int(IntType::I8) => {
                            variable
                                .get::<i8, _>(Extents::All)
                                .map(convert_nd_array::<i8>);
                        }
                        netcdf::types::NcVariableType::Float(float_type) => todo!(),
                        _ => {}
                    },
                    _ => {}
                }
            }
            None
        }
        _ => None,
    }
}

fn convert_nd_array<T>(
    base: ArrayBase<OwnedRepr<i8>, Dim<IxDynImpl>>,
) -> ArrayBase<OwnedRepr<i64>, Dim<IxDynImpl>> {
    todo!()
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
    re.captures(input)
        .map(|caps| Epoch::from_str(caps["epoch"].to_string().as_str()).ok())
        .flatten()
}
