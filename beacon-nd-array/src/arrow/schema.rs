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
