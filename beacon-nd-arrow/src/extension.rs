use arrow::datatypes::Field;
use arrow_schema::DataType;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::NdArrayError;

/// The extension name used to mark *ND columns*.
///
/// Important: per-row dimensions are NOT stored here. They are stored in the column's
/// nested arrays (`dim_names` and `dim_sizes`).
pub const ND_EXTENSION_NAME: &str = "beacon.nd";

/// Arrow spec keys for extension types stored on `Field` metadata.
pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub const EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";

/// Child field name for the flattened values list.
pub const VALUES_FIELD_NAME: &str = "values";
/// Child field name for the (dictionary-encoded) dimension names list.
pub const DIM_NAMES_FIELD_NAME: &str = "dim_names";
/// Child field name for the dimension sizes list.
pub const DIM_SIZES_FIELD_NAME: &str = "dim_sizes";

/// Returns the Arrow `DataType` used to store an ND column with `storage_type`.
///
/// Representation per row:
/// - `values`: `List<T>` containing the flat values
/// - `dim_names`: `List<Dictionary<Int32, Utf8>>` (dictionary-encoded names)
/// - `dim_sizes`: `List<UInt32>`
pub fn nd_column_data_type(storage_type: DataType) -> DataType {
    let values_item = Field::new("item", storage_type, true);
    let names_item = Field::new(
        "item",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    );
    let sizes_item = Field::new("item", DataType::UInt32, true);

    DataType::Struct(
        vec![
            Field::new(
                VALUES_FIELD_NAME,
                DataType::List(Arc::new(values_item)),
                true,
            ),
            Field::new(
                DIM_NAMES_FIELD_NAME,
                DataType::List(Arc::new(names_item)),
                true,
            ),
            Field::new(
                DIM_SIZES_FIELD_NAME,
                DataType::List(Arc::new(sizes_item)),
                true,
            ),
        ]
        .into(),
    )
}

/// Build a `Field` that declares a Beacon ND column.
///
/// This tags the field with Arrow extension metadata so that the intent ("this is an ND column")
/// survives Arrow IPC roundtrips.
///
/// Note: per-row dimensions are stored in the nested arrays, not in the field metadata.
///
/// ```
/// use arrow_schema::DataType;
/// use beacon_nd_arrow::extension;
///
/// let field = extension::nd_column_field("x", DataType::Int32, false)?;
/// assert!(extension::is_nd_column_field(&field));
/// # Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
/// ```
pub fn nd_column_field(
    name: impl Into<String>,
    storage_type: DataType,
    nullable: bool,
) -> Result<Field, NdArrayError> {
    let mut metadata = HashMap::new();
    metadata.insert(
        EXTENSION_NAME_KEY.to_string(),
        ND_EXTENSION_NAME.to_string(),
    );
    // Keep metadata intentionally dimension-free.
    metadata.insert(EXTENSION_METADATA_KEY.to_string(), "{}".to_string());

    Ok(
        Field::new(name.into(), nd_column_data_type(storage_type), nullable)
            .with_metadata(metadata),
    )
}

/// Returns `true` when `field` is tagged as a Beacon ND column.
///
/// This checks the Arrow extension metadata keys.
pub fn is_nd_column_field(field: &Field) -> bool {
    field
        .metadata()
        .get(EXTENSION_NAME_KEY)
        .map(|name| name == ND_EXTENSION_NAME)
        .unwrap_or(false)
}

/// Extract the underlying storage `DataType` from an ND column field.
///
/// This inspects the nested struct layout and returns the `T` from the `values: List<T>` child.
pub fn nd_storage_type_from_field(field: &Field) -> Result<DataType, NdArrayError> {
    match field.data_type() {
        DataType::Struct(fields) => {
            let values_field = fields
                .iter()
                .find(|f| f.name() == VALUES_FIELD_NAME)
                .ok_or_else(|| NdArrayError::InvalidNdColumn("missing values field".into()))?;
            match values_field.data_type() {
                DataType::List(item_field) => Ok(item_field.data_type().clone()),
                other => Err(NdArrayError::InvalidNdColumn(format!(
                    "values field must be List<T>, got {other:?}"
                ))),
            }
        }
        other => Err(NdArrayError::InvalidNdColumn(format!(
            "ND column must be Struct, got {other:?}"
        ))),
    }
}
