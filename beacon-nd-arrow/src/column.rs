use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, ListArray, StringArray, StructArray, UInt32Array,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Int32Type;
use arrow_schema::DataType;

use crate::{
    NdArrowArray,
    dimensions::{Dimension, Dimensions},
    error::NdArrayError,
    extension,
};

/// An Arrow column storing one ND array per row.
///
/// Physical representation:
/// `Struct{ values: List<T>, dim_names: List<Dictionary<Int32, Utf8>>, dim_sizes: List<UInt32> }`
///
/// # Example
///
/// ```
/// use std::sync::Arc;
///
/// use arrow::array::Int32Array;
/// use beacon_nd_arrow::{
///     NdArrowArray,
///     column::NdArrowArrayColumn,
///     dimensions::{Dimension, Dimensions},
/// };
///
/// let nd1 = NdArrowArray::new(
///     Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
///     Dimensions::new(vec![
///         Dimension::try_new("y", 2)?,
///         Dimension::try_new("x", 3)?,
///     ]),
/// )?;
/// let nd2 = NdArrowArray::new(
///     Arc::new(Int32Array::from(vec![7, 8, 9])),
///     Dimensions::new(vec![
///         Dimension::try_new("y", 1)?,
///         Dimension::try_new("x", 3)?,
///     ]),
/// )?;
///
/// let col = NdArrowArrayColumn::from_rows(vec![nd1.clone(), nd2.clone()])?;
/// assert_eq!(col.len(), 2);
///
/// let r0 = col.row(0)?;
/// assert_eq!(r0.dimensions().shape(), vec![2, 3]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct NdArrowArrayColumn {
    array: Arc<StructArray>,
    storage_type: DataType,
}

impl NdArrowArrayColumn {
    /// Build an ND column from a list of row values.
    ///
    /// All rows must have the same underlying storage `DataType`.
    ///
    /// # Errors
    /// - If `rows` is empty (storage type cannot be inferred).
    /// - If rows have mismatched storage types.
    /// - If internal offsets overflow Arrow's `i32` list offset representation.
    pub fn from_rows(rows: Vec<NdArrowArray>) -> Result<Self, NdArrayError> {
        if rows.is_empty() {
            return Err(NdArrayError::InvalidNdColumn(
                "cannot infer storage type from empty rows".into(),
            ));
        }

        let storage_type = rows[0].values().data_type().clone();
        for (idx, r) in rows.iter().enumerate() {
            if r.values().data_type() != &storage_type {
                return Err(NdArrayError::InvalidNdColumn(format!(
                    "row {idx} storage dtype mismatch: {:?} != {:?}",
                    r.values().data_type(),
                    storage_type
                )));
            }
        }

        let values_list = build_list_array_from_arrays(
            rows.iter().map(|r| r.values().clone()).collect::<Vec<_>>(),
            storage_type.clone(),
        )?;

        let (dim_names_list, dim_sizes_list) = build_dimensions_lists(&rows)?;

        let fields = match extension::nd_column_data_type(storage_type.clone()) {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        };

        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(values_list) as ArrayRef,
                Arc::new(dim_names_list) as ArrayRef,
                Arc::new(dim_sizes_list) as ArrayRef,
            ],
            None,
        );

        Ok(Self {
            array: Arc::new(struct_array),
            storage_type,
        })
    }

    /// Interpret an existing Arrow array as an ND column.
    ///
    /// This validates that the input is a `StructArray` and infers the storage type from the
    /// `values: List<T>` child.
    pub fn try_from_array(array: ArrayRef) -> Result<Self, NdArrayError> {
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| NdArrayError::InvalidNdColumn("expected StructArray".into()))?;

        // Infer storage type from the values field
        let values_col = struct_array
            .column_by_name(extension::VALUES_FIELD_NAME)
            .ok_or_else(|| NdArrayError::InvalidNdColumn("missing values column".into()))?;

        let storage_type = match values_col.data_type() {
            DataType::List(item_field) => item_field.data_type().clone(),
            other => {
                return Err(NdArrayError::InvalidNdColumn(format!(
                    "values must be List<T>, got {other:?}"
                )));
            }
        };

        Ok(Self {
            array: Arc::new(struct_array.clone()),
            storage_type,
        })
    }

    /// Convert this column into an `ArrayRef` (the physical Arrow representation).
    pub fn into_array_ref(self) -> ArrayRef {
        self.array
    }

    /// Borrow the underlying `StructArray`.
    pub fn as_struct_array(&self) -> &StructArray {
        self.array.as_ref()
    }

    /// Return the underlying Arrow storage type `T` from the `values: List<T>` field.
    pub fn storage_type(&self) -> &DataType {
        &self.storage_type
    }

    /// Number of rows in the column.
    pub fn len(&self) -> usize {
        self.array.len()
    }

    /// Returns `true` if the column has zero rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read a single row as an `NdArrowArray` value.
    pub fn row(&self, index: usize) -> Result<NdArrowArray, NdArrayError> {
        if index >= self.len() {
            return Err(NdArrayError::InvalidNdColumn(
                "row index out of bounds".into(),
            ));
        }

        let values_list = self
            .array
            .column_by_name(extension::VALUES_FIELD_NAME)
            .ok_or_else(|| NdArrayError::InvalidNdColumn("missing values column".into()))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| NdArrayError::InvalidNdColumn("values must be ListArray".into()))?;

        let names_list = self
            .array
            .column_by_name(extension::DIM_NAMES_FIELD_NAME)
            .ok_or_else(|| NdArrayError::InvalidNdColumn("missing dim_names column".into()))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| NdArrayError::InvalidNdColumn("dim_names must be ListArray".into()))?;

        let sizes_list = self
            .array
            .column_by_name(extension::DIM_SIZES_FIELD_NAME)
            .ok_or_else(|| NdArrayError::InvalidNdColumn("missing dim_sizes column".into()))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| NdArrayError::InvalidNdColumn("dim_sizes must be ListArray".into()))?;

        let values = values_list.value(index);
        let names = names_list.value(index);
        let sizes = sizes_list.value(index);

        let names = names
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .ok_or_else(|| {
                NdArrayError::InvalidNdColumn(
                    "dim_names inner must be DictionaryArray<Int32, Utf8>".into(),
                )
            })?;
        let sizes = sizes
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                NdArrayError::InvalidNdColumn("dim_sizes inner must be UInt32Array".into())
            })?;

        if names.len() != sizes.len() {
            return Err(NdArrayError::InvalidNdColumn(
                "dim_names and dim_sizes length mismatch".into(),
            ));
        }

        let dict_values = names
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                NdArrayError::InvalidNdColumn("dim_names dictionary must be StringArray".into())
            })?;
        let keys = names.keys();

        let dims = (0..names.len())
            .map(|i| {
                let key = keys.value(i) as usize;
                let name = dict_values.value(key).to_string();
                let size = sizes.value(i) as usize;
                Dimension::try_new(name, size)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let dimensions = Dimensions::new(dims);
        NdArrowArray::new(Arc::new(values), dimensions)
    }
}

/// Build a `ListArray` from already-flat child arrays.
///
/// This is used to build the `values: List<T>` field for an ND column.
fn build_list_array_from_arrays(
    arrays: Vec<ArrayRef>,
    storage_type: DataType,
) -> Result<ListArray, NdArrayError> {
    let mut offsets: Vec<i32> = Vec::with_capacity(arrays.len() + 1);
    offsets.push(0);

    let mut total: i32 = 0;
    for a in &arrays {
        total = total
            .checked_add(a.len() as i32)
            .ok_or_else(|| NdArrayError::InvalidNdColumn("list offsets overflow i32".into()))?;
        offsets.push(total);
    }

    let values = if arrays.is_empty() {
        arrow::array::new_empty_array(&storage_type)
    } else {
        let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
        arrow::compute::concat(&refs)?
    };

    let item_field = Arc::new(arrow::datatypes::Field::new("item", storage_type, true));
    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::new(
        Buffer::from_slice_ref(offsets.as_slice()),
        0,
        offsets.len(),
    ));

    Ok(ListArray::new(item_field, offsets_buffer, values, None))
}

/// Build per-row `dim_names` and `dim_sizes` list arrays.
///
/// Dimension names are dictionary-encoded across the entire column to improve compression.
fn build_dimensions_lists(rows: &[NdArrowArray]) -> Result<(ListArray, ListArray), NdArrayError> {
    let mut name_offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    let mut size_offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    name_offsets.push(0);
    size_offsets.push(0);

    let mut dict: Vec<String> = vec![];
    let mut dict_index: HashMap<String, i32> = HashMap::new();
    let mut flat_name_keys: Vec<i32> = vec![];
    let mut flat_sizes: Vec<u32> = vec![];

    let mut name_total: i32 = 0;
    let mut size_total: i32 = 0;

    for row in rows {
        let dims = row
            .dimensions()
            .as_multi_dimensional()
            .cloned()
            .unwrap_or_default();

        for d in &dims {
            let key = if let Some(existing) = dict_index.get(d.name()) {
                *existing
            } else {
                let next = dict.len() as i32;
                dict.push(d.name().to_string());
                dict_index.insert(d.name().to_string(), next);
                next
            };
            flat_name_keys.push(key);
            let size_u32 = u32::try_from(d.size()).map_err(|_| {
                NdArrayError::InvalidNdColumn(format!("dimension size overflows u32: {}", d.size()))
            })?;
            flat_sizes.push(size_u32);
        }

        name_total = name_total.checked_add(dims.len() as i32).ok_or_else(|| {
            NdArrayError::InvalidNdColumn("dim_names offsets overflow i32".into())
        })?;
        size_total = size_total.checked_add(dims.len() as i32).ok_or_else(|| {
            NdArrayError::InvalidNdColumn("dim_sizes offsets overflow i32".into())
        })?;
        name_offsets.push(name_total);
        size_offsets.push(size_total);
    }

    let dict_values = Arc::new(StringArray::from_iter_values(
        dict.iter().map(|s| s.as_str()),
    ));
    let keys = Int32Array::from(flat_name_keys);
    let names_values: DictionaryArray<Int32Type> = DictionaryArray::try_new(keys, dict_values)?;
    let sizes_values = UInt32Array::from_iter_values(flat_sizes.iter().copied());

    let names_item = Arc::new(arrow::datatypes::Field::new(
        "item",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    ));
    let sizes_item = Arc::new(arrow::datatypes::Field::new("item", DataType::UInt32, true));

    let name_offsets_buffer = OffsetBuffer::new(ScalarBuffer::new(
        Buffer::from_slice_ref(name_offsets.as_slice()),
        0,
        name_offsets.len(),
    ));
    let size_offsets_buffer = OffsetBuffer::new(ScalarBuffer::new(
        Buffer::from_slice_ref(size_offsets.as_slice()),
        0,
        size_offsets.len(),
    ));

    let names_list = ListArray::new(
        names_item,
        name_offsets_buffer,
        Arc::new(names_values),
        None,
    );
    let sizes_list = ListArray::new(
        sizes_item,
        size_offsets_buffer,
        Arc::new(sizes_values),
        None,
    );
    Ok((names_list, sizes_list))
}
