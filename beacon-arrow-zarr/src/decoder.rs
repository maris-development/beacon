use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{
        ArrayRef, AsArray, BinaryArray, BooleanArray, PrimitiveArray, StringArray,
        TimestampMillisecondArray,
    },
    datatypes::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef,
        TimestampMillisecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use hifitime::Epoch;
use indexmap::IndexMap;
use nd_arrow_array::{NdArrowArray, batch::NdRecordBatch};
use regex::Regex;

use crate::attributes::AttributeValue;

pub trait Decoder: Send + Sync {
    fn create(
        group_reader: &crate::reader::AsyncArrowZarrGroupReader,
        input_schema: SchemaRef,
    ) -> Result<Self, String>
    where
        Self: Sized;
    fn decoded_schema(&self) -> SchemaRef;
    fn decoding_array_pipeline(
        &self,
        array_name: &str,
    ) -> Option<Vec<Arc<dyn DecodingPipelineStep>>>;
    fn decode_array(&self, array_name: &str, array: NdArrowArray) -> Result<NdArrowArray, String> {
        if let Some(pipeline) = self.decoding_array_pipeline(array_name) {
            pipeline.iter().fold(Ok(array), |maybe_array, step| {
                maybe_array.and_then(|array| step.decode(array))
            })
        } else {
            Ok(array)
        }
    }
}

pub trait DecodingPipelineStep: Send + Sync {
    fn output_data_type(
        &self,
        input_data_type: arrow::datatypes::DataType,
    ) -> arrow::datatypes::DataType {
        input_data_type
    }
    fn decode(&self, array: NdArrowArray) -> Result<NdArrowArray, String>;
}

pub struct FillValueDecoder {
    // Array decoders to apply per array name
    array_fill_decoders: std::collections::HashMap<String, Arc<dyn DecodingPipelineStep>>,
    decoded_schema: SchemaRef,
}

impl Decoder for FillValueDecoder {
    fn create(
        group_reader: &crate::reader::AsyncArrowZarrGroupReader,
        input_schema: SchemaRef,
    ) -> Result<Self, String>
    where
        Self: Sized,
    {
        let mut array_fill_decoders = std::collections::HashMap::new();
        for (array_name, array_reader) in group_reader.arrays() {
            // Add a FillValueDecodingStep if the array has a fill value
            let fill_value_attr = array_reader.attributes().get("_FillValue");
            if let Some(arrow_fill_value) = ArrowFillValue::from_zarrs_fill_value(
                array_reader.data_type(),
                fill_value_attr
                    .and_then(AttributeValue::from_json_value)
                    .as_ref(),
            ) {
                let fill_value_decoder = FillValueDecodingStep {
                    fill_value: arrow_fill_value,
                };
                array_fill_decoders.insert(
                    array_name.to_string(),
                    Arc::new(fill_value_decoder) as Arc<dyn DecodingPipelineStep>,
                );
            }
        }
        Ok(Self {
            array_fill_decoders,
            decoded_schema: input_schema,
        })
    }

    fn decoded_schema(&self) -> SchemaRef {
        self.decoded_schema.clone()
    }

    fn decoding_array_pipeline(
        &self,
        array_name: &str,
    ) -> Option<Vec<Arc<dyn DecodingPipelineStep>>> {
        self.array_fill_decoders
            .get(array_name)
            .map(|decoder| vec![decoder.clone()])
    }
}

pub struct FillValueDecodingStep {
    fill_value: ArrowFillValue,
}

pub enum ArrowFillValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl ArrowFillValue {
    pub fn from_zarrs_fill_value(
        array_data_type: &zarrs::array::DataType,
        fill_value_attr: Option<&AttributeValue>,
    ) -> Option<Self> {
        match array_data_type {
            zarrs::array::DataType::Bool => fill_value_attr
                .and_then(|attr| attr.as_bool())
                .map(ArrowFillValue::Bool),
            zarrs::array::DataType::Int8 => fill_value_attr
                .and_then(|attr| attr.as_f64().and_then(|v| i8::try_from(v as i64).ok()))
                .map(ArrowFillValue::Int8),
            zarrs::array::DataType::Int16 => fill_value_attr
                .and_then(|attr| attr.as_f64().and_then(|v| i16::try_from(v as i64).ok()))
                .map(ArrowFillValue::Int16),
            zarrs::array::DataType::Int32 => fill_value_attr
                .and_then(|attr| attr.as_f64().and_then(|v| i32::try_from(v as i64).ok()))
                .map(ArrowFillValue::Int32),
            zarrs::array::DataType::Int64 => fill_value_attr
                .and_then(|attr| attr.as_f64().map(|v| v as i64).or(None))
                .map(ArrowFillValue::Int64),
            zarrs::array::DataType::UInt8 => fill_value_attr
                .and_then(|attr| attr.as_f64().and_then(|v| u8::try_from(v as u64).ok()))
                .map(ArrowFillValue::UInt8),
            zarrs::array::DataType::UInt16 => fill_value_attr
                .and_then(|attr| attr.as_f64().and_then(|v| u16::try_from(v as u64).ok()))
                .map(ArrowFillValue::UInt16),
            zarrs::array::DataType::UInt32 => fill_value_attr
                .and_then(|attr| attr.as_f64().and_then(|v| u32::try_from(v as u64).ok()))
                .map(ArrowFillValue::UInt32),

            zarrs::array::DataType::UInt64 => fill_value_attr
                .and_then(|attr| attr.as_f64().map(|v| v as u64).or(None))
                .map(ArrowFillValue::UInt64),

            zarrs::array::DataType::Float32 => fill_value_attr
                .and_then(|attr| attr.as_f64().map(|v| v as f32).or(None))
                .map(ArrowFillValue::Float32),

            zarrs::array::DataType::Float64 => fill_value_attr
                .and_then(|attr| attr.as_f64())
                .map(ArrowFillValue::Float64),

            zarrs::array::DataType::String => {
                // Interpret fill_bytes as UTF-8 string
                fill_value_attr
                    .and_then(|attr| attr.as_str().map(|s| s.to_string()))
                    .map(ArrowFillValue::String)
            }

            _ => None,
        }
    }
}

impl DecodingPipelineStep for FillValueDecodingStep {
    fn output_data_type(
        &self,
        input_data_type: arrow::datatypes::DataType,
    ) -> arrow::datatypes::DataType {
        input_data_type
    }

    fn decode(&self, array: NdArrowArray) -> Result<NdArrowArray, String> {
        let arrow_array = array.as_arrow_array();
        let nullable_array: ArrayRef = match (arrow_array.data_type(), &self.fill_value) {
            (arrow::datatypes::DataType::Null, _) => return Ok(array.clone()),
            (arrow::datatypes::DataType::Boolean, ArrowFillValue::Bool(fill_value)) => {
                let bool_array = array.as_boolean();
                let filled_array: BooleanArray = bool_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::Int8, ArrowFillValue::Int8(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::Int8Type>();
                let filled_array: PrimitiveArray<Int8Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::Int16, ArrowFillValue::Int16(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::Int16Type>();
                let filled_array: PrimitiveArray<Int16Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::Int32, ArrowFillValue::Int32(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::Int32Type>();
                let filled_array: PrimitiveArray<Int32Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::Int64, ArrowFillValue::Int64(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::Int64Type>();
                let filled_array: PrimitiveArray<Int64Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::UInt8, ArrowFillValue::UInt8(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::UInt8Type>();
                let filled_array: PrimitiveArray<UInt8Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::UInt16, ArrowFillValue::UInt16(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::UInt16Type>();
                let filled_array: PrimitiveArray<UInt16Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }

            (arrow::datatypes::DataType::UInt32, ArrowFillValue::UInt32(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::UInt32Type>();
                let filled_array: PrimitiveArray<UInt32Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }

            (arrow::datatypes::DataType::UInt64, ArrowFillValue::UInt64(fill_value)) => {
                let int_array = array.as_primitive::<arrow::datatypes::UInt64Type>();
                let filled_array: PrimitiveArray<UInt64Type> = int_array
                    .iter()
                    .map(|v| v.filter(|&value| value != *fill_value))
                    .collect();
                Arc::new(filled_array)
            }

            (arrow::datatypes::DataType::Float32, ArrowFillValue::Float32(value)) => {
                let float_array = array.as_primitive::<arrow::datatypes::Float32Type>();
                let filled_array: PrimitiveArray<Float32Type> = float_array
                    .iter()
                    .map(|v| v.filter(|&v| v != *value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::Float64, ArrowFillValue::Float64(value)) => {
                let float_array = array.as_primitive::<arrow::datatypes::Float64Type>();
                let filled_array: PrimitiveArray<Float64Type> = float_array
                    .iter()
                    .map(|v| v.filter(|&v| v != *value))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::Binary, ArrowFillValue::Bytes(fill_value)) => {
                let binary_array = array.as_binary::<i32>();
                let filled_array: BinaryArray = binary_array
                    .iter()
                    .map(|v| v.filter(|&value| value != fill_value.as_slice()))
                    .collect();
                Arc::new(filled_array)
            }
            (arrow::datatypes::DataType::Utf8, ArrowFillValue::String(fill_value)) => {
                let string_array = array.as_string::<i32>();
                let filled_array: StringArray = string_array
                    .iter()
                    .map(|v| v.filter(|&value| value != fill_value))
                    .collect();

                Arc::new(filled_array)
            }
            _ => {
                return Err(format!(
                    "FillValueDecodingStep does not support data type: {:?}",
                    array.data_type()
                ));
            }
        };

        Ok(NdArrowArray::new(nullable_array, array.dimensions().clone()).unwrap())
    }
}

pub struct CFDecoder {
    // Array decoders to apply per array name
    array_decoders: std::collections::HashMap<String, Vec<Arc<dyn DecodingPipelineStep>>>,
    decoded_schema: SchemaRef,
}

impl Decoder for CFDecoder {
    fn create(
        group_reader: &crate::reader::AsyncArrowZarrGroupReader,
        input_schema: SchemaRef,
    ) -> Result<Self, String> {
        let mut array_decoders: std::collections::HashMap<
            String,
            Vec<Arc<dyn DecodingPipelineStep>>,
        > = std::collections::HashMap::new();
        let mut fields = IndexMap::new();
        for field in input_schema.fields() {
            fields.insert(field.name().clone(), field.as_ref().clone());
        }

        for (array_name, array_reader) in group_reader.arrays() {
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
                    Some(Arc::new(ScaleFactorOffsetDecodingStep::new(scale, offset))
                        as Arc<dyn DecodingPipelineStep>)
                }
                (Some(scale), None) => {
                    Some(Arc::new(ScaleFactorOffsetDecodingStep::new(scale, 0.0))
                        as Arc<dyn DecodingPipelineStep>)
                }
                (None, Some(offset)) => {
                    Some(Arc::new(ScaleFactorOffsetDecodingStep::new(1.0, offset))
                        as Arc<dyn DecodingPipelineStep>)
                }
                _ => None,
            };

            if let Some(decoding_pipeline) = decoding_pipeline {
                array_decoders
                    .entry(array_name.to_string())
                    .or_default()
                    .push(decoding_pipeline.clone());
                // Update the field data type to Float64
                let array_field = fields.get_mut(array_name).unwrap();
                *array_field = array_field.clone().with_data_type(
                    decoding_pipeline.output_data_type(array_field.data_type().clone()),
                );
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
                        match TimeUnitsDecodingStep::new(value.clone()) {
                            Ok(decoder) => {
                                let decoder = Arc::new(decoder) as Arc<dyn DecodingPipelineStep>;
                                array_decoders
                                    .entry(array_name.to_string())
                                    .or_default()
                                    .push(decoder.clone());
                                // Update the field data type to Int64 (Arrow timestamp in milliseconds)
                                let array_field = fields.get_mut(array_name).unwrap();
                                *array_field = array_field.clone().with_data_type(
                                    decoder.output_data_type(array_field.data_type().clone()),
                                );
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
            array_decoders,
            decoded_schema: Arc::new(arrow::datatypes::Schema::new(
                fields.into_values().collect::<Vec<_>>(),
            )),
        })
    }

    fn decoded_schema(&self) -> SchemaRef {
        self.decoded_schema.clone()
    }

    fn decoding_array_pipeline(
        &self,
        array_name: &str,
    ) -> Option<Vec<Arc<dyn DecodingPipelineStep>>> {
        self.array_decoders.get(array_name).cloned()
    }
}

pub struct ScaleFactorOffsetDecodingStep {
    scale: f64,
    offset: f64,
}

impl ScaleFactorOffsetDecodingStep {
    pub fn new(scale: f64, offset: f64) -> Self {
        ScaleFactorOffsetDecodingStep { scale, offset }
    }
}

impl DecodingPipelineStep for ScaleFactorOffsetDecodingStep {
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

    fn output_data_type(
        &self,
        _input_data_type: arrow::datatypes::DataType,
    ) -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Float64
    }
}

pub struct TimeUnitsDecodingStep {
    unit: hifitime::Unit,
    epoch: Epoch,
}

impl TimeUnitsDecodingStep {
    pub fn new(units_str: String) -> Result<Self, String> {
        let unit = Self::extract_units(&units_str)
            .ok_or(format!("Unsupported CF time units: {}", units_str))?;
        let epoch = Self::extract_epoch(&units_str).ok_or(format!(
            "Could not extract epoch from CF time units: {}",
            units_str
        ))?;
        Ok(TimeUnitsDecodingStep { unit, epoch })
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

impl DecodingPipelineStep for TimeUnitsDecodingStep {
    fn output_data_type(
        &self,
        _input_data_type: arrow::datatypes::DataType,
    ) -> arrow::datatypes::DataType {
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
