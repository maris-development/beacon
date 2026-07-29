use crate::dataset::AnyDataset;

pub fn any_dataset_to_arrow_schema(
    dataset: &AnyDataset,
) -> anyhow::Result<arrow::datatypes::Schema> {
    let mut fields = Vec::new();
    for (name, dtype) in dataset.fields() {
        let field = arrow::datatypes::Field::new(name, dtype.into(), true);
        fields.push(field);
    }

    Ok(arrow::datatypes::Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NdArray, NdArrayD, dataset::Dataset};
    use indexmap::IndexMap;
    use std::sync::Arc;

    async fn dataset(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> AnyDataset {
        let map: IndexMap<String, Arc<dyn NdArrayD>> = arrays
            .into_iter()
            .map(|(name, array)| (name.to_string(), array))
            .collect();
        AnyDataset::try_from_dataset(Dataset::new("ds".to_string(), map).await)
            .await
            .unwrap()
    }

    fn f64_array(dims: &[&str]) -> Arc<dyn NdArrayD> {
        let dim_names: Vec<String> = dims.iter().map(|d| d.to_string()).collect();
        let shape = vec![1; dims.len()];
        Arc::new(
            NdArray::<f64>::try_new_from_vec_in_mem(vec![0.0], shape, dim_names, None).unwrap(),
        )
    }

    fn i32_array(dims: &[&str]) -> Arc<dyn NdArrayD> {
        let dim_names: Vec<String> = dims.iter().map(|d| d.to_string()).collect();
        let shape = vec![1; dims.len()];
        Arc::new(NdArray::<i32>::try_new_from_vec_in_mem(vec![0], shape, dim_names, None).unwrap())
    }

    #[tokio::test]
    async fn test_schema_fields_are_sorted_and_typed() {
        let ds = dataset(vec![("temp", f64_array(&["x"])), ("count", i32_array(&["x"]))]).await;
        let schema = any_dataset_to_arrow_schema(&ds).unwrap();

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["count", "temp"]);
        assert_eq!(
            schema.field(0).data_type(),
            &arrow::datatypes::DataType::Int32
        );
        assert_eq!(
            schema.field(1).data_type(),
            &arrow::datatypes::DataType::Float64
        );
    }

    #[tokio::test]
    async fn test_every_field_is_nullable() {
        // Broadcasting and ragged null-padding can both introduce nulls in any
        // column, so no field may be declared non-nullable.
        let ds = dataset(vec![("temp", f64_array(&["x"]))]).await;
        let schema = any_dataset_to_arrow_schema(&ds).unwrap();
        assert!(schema.fields().iter().all(|f| f.is_nullable()));
    }

    #[tokio::test]
    async fn test_empty_dataset_yields_an_empty_schema() {
        let ds = dataset(vec![]).await;
        assert_eq!(any_dataset_to_arrow_schema(&ds).unwrap().fields().len(), 0);
    }
}
