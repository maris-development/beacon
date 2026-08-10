use std::{collections::HashMap, sync::Arc};

use arrow_schema::{ArrowError, FieldRef, SchemaRef};

pub struct ArrowTypeWidening {
    pub strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl ArrowTypeWidening {
    pub fn new(strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        Self { strategy }
    }

    /// The strategy a session gets when nobody registers one.
    ///
    /// `FastObjectTable` reads this extension to merge the schemas of the files
    /// behind one table, and it requires it. `RuntimeBuilder` registers it for a
    /// server. A session built by hand, in a test or an embedded use, has to
    /// register it too, and this is what it registers.
    pub fn default_extension() -> Arc<Self> {
        Arc::new(Self::new(Arc::new(DefaultArrowTypeWidening)))
    }

    pub fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
        self.strategy.merge_schemas(schema_refs)
    }
}

pub trait ArrowTypeWideningStrategy: Send + Sync {
    fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError>;
}

pub struct DefaultArrowTypeWidening;

impl ArrowTypeWideningStrategy for DefaultArrowTypeWidening {
    fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
        if schema_refs.is_empty() {
            return Err(ArrowError::SchemaError(
                "No schemas provided for merging".to_string(),
            ));
        }

        let mut merged_fields = Vec::new();
        let mut field_map: HashMap<String, FieldRef> = HashMap::new();

        for schema_ref in schema_refs {
            for field in schema_ref.fields() {
                let field_name = field.name().clone();
                if let Some(existing_field) = field_map.get(&field_name) {
                    // If the field already exists, we need to check if the types are compatible
                    if existing_field.data_type() != field.data_type() {
                        return Err(ArrowError::SchemaError(format!(
                            "Incompatible types for field '{}': {:?} vs {:?}",
                            field_name,
                            existing_field.data_type(),
                            field.data_type()
                        )));
                    }
                } else {
                    // If the field does not exist, add it to the map and merged fields
                    field_map.insert(field_name.clone(), field.clone());
                    merged_fields.push(field.clone());
                }
            }
        }

        Ok(Arc::new(arrow_schema::Schema::new(merged_fields)))
    }
}

/// Beacon's super typing as a widening strategy.
///
/// Fields union in first-seen order, and a field two files disagree on widens
/// to the smallest type that holds both — `Int32` + `Int64` is `Int64`,
/// `Int64` + `Float32` is `Float64`, anything + a string is the string. The
/// rules follow Polars and Numpy and live in
/// [`beacon_common::super_typing`]; this type only adapts them to the
/// [`ArrowTypeWideningStrategy`] trait. A pair with no common representation
/// (`Date32` + `Int32`) is an error rather than a guess.
///
/// This is the strategy `FastObjectTable` merges its schemas with unless a
/// deployment registers another through `RuntimeBuilder::with_type_widening`,
/// so a `read_*` over files that disagree on a column's width behaves exactly
/// as it always has.
pub struct SuperTypeWidening;

impl ArrowTypeWideningStrategy for SuperTypeWidening {
    fn merge_schemas(&self, schema_refs: &[SchemaRef]) -> Result<SchemaRef, ArrowError> {
        beacon_common::super_typing::super_type_schema(schema_refs)
            .map(Arc::new)
            .map_err(|e| ArrowError::SchemaError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn merging_no_schemas_is_an_error() {
        let widening = ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening));
        let err = widening.merge_schemas(&[]).unwrap_err();
        assert!(
            matches!(err, ArrowError::SchemaError(_)),
            "expected a schema error, got {err:?}"
        );
    }

    #[test]
    fn merge_unions_fields_in_first_seen_order() {
        let widening = ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening));
        let merged = widening
            .merge_schemas(&[
                schema(&[("a", DataType::Int32), ("b", DataType::Utf8)]),
                // `b` repeats with an identical type (deduped), `c` is new.
                schema(&[("b", DataType::Utf8), ("c", DataType::Float64)]),
            ])
            .unwrap();

        let names: Vec<&str> = merged.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn merge_rejects_a_field_with_two_types() {
        // The default strategy does not widen: it only unions identical fields,
        // so a type conflict is an error rather than a promotion.
        let widening = ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening));
        let err = widening
            .merge_schemas(&[
                schema(&[("a", DataType::Int32)]),
                schema(&[("a", DataType::Int64)]),
            ])
            .unwrap_err();

        match err {
            ArrowError::SchemaError(message) => {
                assert!(message.contains('a'), "message should name the field: {message}");
            }
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }

    #[test]
    fn super_typing_widens_a_field_with_two_types() {
        // The super-typing strategy promotes instead: the same conflict the
        // default rejects widens to the type that holds both.
        let widening = ArrowTypeWidening::new(Arc::new(SuperTypeWidening));
        let merged = widening
            .merge_schemas(&[
                schema(&[("a", DataType::Int32)]),
                schema(&[("a", DataType::Int64)]),
            ])
            .unwrap();
        assert_eq!(
            merged.field_with_name("a").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn super_typing_still_rejects_a_field_with_no_common_type() {
        // Widening never guesses: a pair with no common representation is an
        // error rather than a silent cast.
        let widening = ArrowTypeWidening::new(Arc::new(SuperTypeWidening));
        let err = widening
            .merge_schemas(&[
                schema(&[("a", DataType::Date32)]),
                schema(&[("a", DataType::Int32)]),
            ])
            .unwrap_err();

        match err {
            ArrowError::SchemaError(message) => {
                assert!(message.contains('a'), "message should name the field: {message}");
            }
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }
}
