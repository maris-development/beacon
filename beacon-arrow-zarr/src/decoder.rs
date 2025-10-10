use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{AsArray, PrimitiveArray, TimestampMillisecondArray},
    datatypes::{Float64Type, Int64Type, SchemaRef, TimestampMillisecondType},
};
use hifitime::Epoch;
use indexmap::IndexMap;
use nd_arrow_array::NdArrowArray;
use regex::Regex;

pub trait Decoder {
    fn create(
        group_reader: &crate::reader::ArrowGroupReader,
        input_schema: SchemaRef,
    ) -> Result<Self, String>
    where
        Self: Sized;
    fn decoded_schema(&self) -> SchemaRef;
    fn decoding_pipeline(&self, array_name: &str) -> Option<Vec<Arc<dyn DecodingPipelineStep>>>;
}

pub trait DecodingPipelineStep {
    fn output_data_type(&self) -> arrow::datatypes::DataType;
    fn decode(&self, array: NdArrowArray) -> Result<NdArrowArray, String>;
}

pub struct CFDecoder {
    // Array decoders to apply per array name
    array_decoders: std::collections::HashMap<String, Vec<Arc<dyn DecodingPipelineStep>>>,
    decoded_schema: SchemaRef,
}

impl Decoder for CFDecoder {
    fn create(
        group_reader: &crate::reader::ArrowGroupReader,
        input_schema: SchemaRef,
    ) -> Result<Self, String> {
        let mut array_decoders: std::collections::HashMap<String, Arc<dyn DecodingPipelineStep>> =
            std::collections::HashMap::new();
        let mut fields = IndexMap::new();
        for field in input_schema.fields() {
            fields.insert(field.name().clone(), field.as_ref().clone());
        }

        for (array_name, _) in group_reader.arrays() {
            // Get all the attributes that start with array_name + "."
            let prefix = format!("{}.", array_name);
            let array_attribute: Vec<_> = group_reader
                .arrow_schema()
                .fields()
                .iter()
                .filter(|field| field.name().starts_with(&prefix))
                .cloned()
                .collect();

            // Check for scale_factor and add_offset attributes (SCALE_FACTOR and ADD_OFFSET in CF conventions)

            let mut offset = None;
            let mut scale_factor = None;

            for field in &array_attribute {
                if *field.name() == format!("{}.scale_factor", array_name) {
                    match group_reader.attributes()[field.name()] {
                        crate::attributes::AttributeValue::Float64(value) => {
                            scale_factor = Some(value)
                        }
                        _ => {
                            return Err(format!(
                                "scale_factor attribute for array {} is not Float64",
                                array_name
                            ));
                        }
                    }
                } else if *field.name() == format!("{}.add_offset", array_name) {
                    match group_reader.attributes()[field.name()] {
                        crate::attributes::AttributeValue::Float64(value) => offset = Some(value),
                        _ => {
                            return Err(format!(
                                "add_offset attribute for array {} is not Float64",
                                array_name
                            ));
                        }
                    }
                }
            }

            let decoding_pipeline = match (scale_factor, offset) {
                (Some(scale), Some(offset)) => {
                    Some(Arc::new(ScaleFactorOffsetDecoder::new(scale, offset))
                        as Arc<dyn DecodingPipelineStep>)
                }
                (Some(scale), None) => Some(Arc::new(ScaleFactorOffsetDecoder::new(scale, 0.0))
                    as Arc<dyn DecodingPipelineStep>),
                (None, Some(offset)) => Some(Arc::new(ScaleFactorOffsetDecoder::new(1.0, offset))
                    as Arc<dyn DecodingPipelineStep>),
                _ => None,
            };

            if let Some(decoding_pipeline) = decoding_pipeline {
                array_decoders.insert(array_name.to_string(), decoding_pipeline.clone());
                // Update the field data type to Float64
                let array_field = fields.get_mut(array_name).unwrap();
                *array_field = array_field
                    .clone()
                    .with_data_type(decoding_pipeline.output_data_type());
            }

            // Check for cf time attributes (units)
            for field in &array_attribute {
                if *field.name() == format!("{}.units", array_name)
                    && let crate::attributes::AttributeValue::String(ref value) =
                        group_reader.attributes()[field.name()]
                {
                    // Check if units is a valid CF time unit
                    if value.starts_with("seconds since")
                        || value.starts_with("minutes since")
                        || value.starts_with("hours since")
                        || value.starts_with("days since")
                        || value.starts_with("weeks since")
                        || value.starts_with("months since")
                        || value.starts_with("years since")
                    {
                        match TimeUnitsDecoder::new(value.clone()) {
                            Ok(decoder) => {
                                let decoder = Arc::new(decoder) as Arc<dyn DecodingPipelineStep>;
                                array_decoders.insert(array_name.to_string(), decoder.clone());
                                // Update the field data type to Int64 (Arrow timestamp in milliseconds)
                                let array_field = fields.get_mut(array_name).unwrap();
                                *array_field = array_field
                                    .clone()
                                    .with_data_type(decoder.output_data_type());
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Could not create TimeUnitsDecoder for array {}: {}",
                                    array_name,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            array_decoders: array_decoders
                .into_iter()
                .map(|(k, v)| (k, vec![v]))
                .collect(),
            decoded_schema: Arc::new(arrow::datatypes::Schema::new(
                fields.into_values().collect::<Vec<_>>(),
            )),
        })
    }

    fn decoded_schema(&self) -> SchemaRef {
        self.decoded_schema.clone()
    }

    fn decoding_pipeline(&self, array_name: &str) -> Option<Vec<Arc<dyn DecodingPipelineStep>>> {
        self.array_decoders.get(array_name).cloned()
    }
}

pub struct ScaleFactorOffsetDecoder {
    scale: f64,
    offset: f64,
}

impl ScaleFactorOffsetDecoder {
    pub fn new(scale: f64, offset: f64) -> Self {
        ScaleFactorOffsetDecoder { scale, offset }
    }
}

impl DecodingPipelineStep for ScaleFactorOffsetDecoder {
    fn decode(&self, array: NdArrowArray) -> Result<NdArrowArray, String> {
        let arrow_array = array.as_arrow_array();

        // Cast to float64 for processing
        let float_array =
            arrow::compute::cast(&arrow_array, &arrow::datatypes::DataType::Float64).unwrap();

        let float_array = float_array.as_primitive::<Float64Type>();

        let applied_array: PrimitiveArray<Float64Type> =
            float_array.unary(|value| value * self.scale + self.offset);

        Ok(NdArrowArray::new(
            Arc::new(applied_array) as arrow::array::ArrayRef,
            array.dimensions().clone(),
        )
        .unwrap())
    }

    fn output_data_type(&self) -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Float64
    }
}

pub struct TimeUnitsDecoder {
    unit: hifitime::Unit,
    epoch: Epoch,
}

impl TimeUnitsDecoder {
    pub fn new(units_str: String) -> Result<Self, String> {
        let unit = Self::extract_units(&units_str)
            .ok_or(format!("Unsupported CF time units: {}", units_str))?;
        let epoch = Self::extract_epoch(&units_str).ok_or(format!(
            "Could not extract epoch from CF time units: {}",
            units_str
        ))?;
        Ok(TimeUnitsDecoder { unit, epoch })
    }

    fn convert_array(
        &self,
        array: &PrimitiveArray<Int64Type>,
        unit: hifitime::Unit,
        epoch: Epoch,
    ) -> TimestampMillisecondArray {
        let data: PrimitiveArray<Int64Type> =
            array.unary(|v| (epoch + (v * unit)).to_unix_milliseconds() as i64);

        // Reinterpret the Int64 array as TimestampMillisecond
        data.reinterpret_cast::<TimestampMillisecondType>()
    }

    fn extract_units(input: &str) -> Option<hifitime::Unit> {
        let re = Regex::new(r"^(?P<units>\w+) since").unwrap();
        re.captures(input)
            .and_then(|caps| match caps["units"].to_string().as_str() {
                "seconds" => Some(hifitime::Unit::Second),
                "milliseconds" => Some(hifitime::Unit::Millisecond),
                "microseconds" => Some(hifitime::Unit::Microsecond),
                "nanoseconds" => Some(hifitime::Unit::Nanosecond),
                "days" => Some(hifitime::Unit::Day),
                "weeks" => Some(hifitime::Unit::Week),
                _ => None,
            })
    }

    /// Extracts the epoch date from a string like "days since -4713-11-24"
    fn extract_epoch(input: &str) -> Option<Epoch> {
        let re = Regex::new(r"since (?P<epoch>-?\d{1,4}-\d{1,2}-\d{1,2})").unwrap();
        let result = re.captures(input).and_then(|caps| {
            let epoch_str = caps["epoch"].to_string();
            let mut epoch = Epoch::from_str(&epoch_str).ok();

            if epoch.is_none() && epoch_str == "-4713-01-01" {
                epoch = Some(Epoch::from_jde_utc(0.0));
            }

            epoch
        });

        result
    }
}

impl DecodingPipelineStep for TimeUnitsDecoder {
    fn output_data_type(&self) -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
    }

    fn decode(&self, array: NdArrowArray) -> Result<NdArrowArray, String> {
        let arrow_array = array.as_arrow_array();

        // Cast to Int64 for processing
        let int_array =
            arrow::compute::cast(&arrow_array, &arrow::datatypes::DataType::Int64).unwrap();

        let int_array = int_array.as_primitive::<Int64Type>();

        let applied_array = self.convert_array(int_array, self.unit, self.epoch);

        Ok(NdArrowArray::new(
            Arc::new(applied_array) as arrow::array::ArrayRef,
            array.dimensions().clone(),
        )
        .unwrap())
    }
}
