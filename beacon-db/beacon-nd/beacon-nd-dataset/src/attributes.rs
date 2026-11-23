//! A view that shows each attribute as a rank-0 variable.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use async_trait::async_trait;
use indexmap::IndexMap;
use indexmap::map::Entry;
use nd_arrow_array::{Attributes, Dimensions, NdArrowArray};

use crate::{NdDataset, NdVariable, Region, Result, VariableInfo};

/// Show each attribute as a rank-0 variable: `var.attr`, and `.attr` for a global attribute.
///
/// An attribute gets a column only when its value is not a list.
/// A variable of `dataset` keeps its name when an attribute column has the same name.
pub fn attribute_columns(dataset: Arc<dyn NdDataset>) -> Arc<dyn NdDataset> {
    let variable_names = dataset.variable_names();
    let taken: HashSet<&str> = variable_names.iter().map(|name| &**name).collect();
    let mut columns: IndexMap<Arc<str>, Arc<dyn NdVariable>> = IndexMap::new();
    let mut add = |prefix: &str, attributes: &Attributes| {
        for (attribute, value) in attributes.iter() {
            // An nd-arrow-array attribute always has length 1.
            if is_list(value.data_type()) {
                continue;
            }
            let name: Arc<str> = Arc::from(format!("{prefix}.{attribute}"));
            if taken.contains(&*name) {
                continue;
            }
            // The first attribute keeps a repeated column name.
            if let Entry::Vacant(entry) = columns.entry(name.clone()) {
                entry.insert(Arc::new(AttributeVariable::new(name, value.clone())));
            }
        }
    };
    for name in &variable_names {
        if let Some(variable) = dataset.variable(name) {
            add(name, variable.attributes());
        }
    }
    add("", dataset.attributes());
    let names = variable_names
        .into_iter()
        .chain(columns.keys().cloned())
        .collect();
    Arc::new(AttributeColumns {
        dataset,
        names,
        columns,
    })
}

/// True for `List`, `LargeList` and `FixedSizeList`.
fn is_list(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

/// The view that `attribute_columns` returns.
#[derive(Debug)]
struct AttributeColumns {
    dataset: Arc<dyn NdDataset>,
    names: Vec<Arc<str>>,
    columns: IndexMap<Arc<str>, Arc<dyn NdVariable>>,
}

impl NdDataset for AttributeColumns {
    fn name(&self) -> &str {
        self.dataset.name()
    }

    fn dimensions(&self) -> &Dimensions {
        self.dataset.dimensions()
    }

    fn attributes(&self) -> &Attributes {
        self.dataset.attributes()
    }

    fn variable_names(&self) -> Vec<Arc<str>> {
        self.names.clone()
    }

    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
        self.dataset
            .variable(name)
            .or_else(|| self.columns.get(name).cloned())
    }

    fn invented_dimensions(&self) -> &[Arc<str>] {
        self.dataset.invented_dimensions()
    }
}

/// One attribute value as a rank-0 variable without attributes.
#[derive(Debug)]
struct AttributeVariable {
    info: VariableInfo,
    value: ArrayRef,
}

impl AttributeVariable {
    fn new(name: Arc<str>, value: ArrayRef) -> Self {
        let info = VariableInfo::new(name, value.data_type().clone(), Dimensions::scalar());
        Self { info, value }
    }
}

#[async_trait]
impl NdVariable for AttributeVariable {
    fn name(&self) -> &str {
        &self.info.name
    }

    fn data_type(&self) -> &DataType {
        &self.info.data_type
    }

    fn dimensions(&self) -> &Dimensions {
        &self.info.dimensions
    }

    fn chunk_shape(&self) -> Option<&[usize]> {
        None
    }

    fn attributes(&self) -> &Attributes {
        &self.info.attributes
    }

    async fn read(&self, region: &Region) -> Result<NdArrowArray> {
        region.check(self.name(), self.dimensions())?;
        Ok(NdArrowArray::try_new_scalar(self.value.clone())?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array, ListArray};
    use arrow::datatypes::{DataType, Float64Type};
    use nd_arrow_array::{Attributes, NdArrowArray};

    use super::*;
    use crate::memory::testing::{dims, ramp};
    use crate::{MemoryDataset, MemoryVariable, NdDataset, Region, validate};

    fn names(view: &Arc<dyn NdDataset>) -> Vec<String> {
        view.variable_names()
            .iter()
            .map(|n| n.to_string())
            .collect()
    }

    /// A Float64 variable with these attributes and the values 0.0, 1.0, ..
    fn variable(name: &str, sizes: &[(&str, usize)], attributes: Attributes) -> MemoryVariable {
        let dimensions = dims(sizes);
        let values =
            Float64Array::from_iter_values((0..dimensions.num_elements()).map(|i| i as f64));
        let array = NdArrowArray::try_new(Arc::new(values), dimensions)
            .unwrap()
            .with_attributes(attributes);
        MemoryVariable::new(name, array)
    }

    /// temp(time=2) with units="degC" and valid_range=[0.0, 40.0]; global title="demo".
    fn with_attrs() -> MemoryDataset {
        let mut attributes = Attributes::new().with("units", "degC");
        let valid_range = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![Some(vec![
            Some(0.0),
            Some(40.0),
        ])]);
        attributes
            .try_insert_array("valid_range", Arc::new(valid_range))
            .unwrap();
        MemoryDataset::try_new(
            "test.nd",
            vec![variable("temp", &[("time", 2)], attributes)],
        )
        .unwrap()
        .with_attributes(Attributes::new().with("title", "demo"))
    }

    /// Variables `a` (attribute b="x") and `a.b`.
    fn clash() -> MemoryDataset {
        MemoryDataset::try_new(
            "test.nd",
            vec![
                variable("a", &[("n", 2)], Attributes::new().with("b", "x")),
                ramp("a.b", &[("n", 2)]),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_variable_attribute_and_a_global_attribute_are_columns() {
        let view = attribute_columns(Arc::new(with_attrs()));
        assert_eq!(names(&view), ["temp", "temp.units", ".title"]);
        let units = view.variable("temp.units").unwrap();
        assert_eq!(units.data_type(), &DataType::Utf8);
        assert_eq!(units.dimensions().rank(), 0);
        validate(view.as_ref()).unwrap();
    }

    #[tokio::test]
    async fn an_attribute_column_reads_its_value() {
        let view = attribute_columns(Arc::new(with_attrs()));
        let value = view
            .variable(".title")
            .unwrap()
            .read(&Region::new(vec![], vec![]))
            .await
            .unwrap();
        assert_eq!(value.values().as_string::<i32>().value(0), "demo");
    }

    #[test]
    fn a_list_attribute_is_not_a_column() {
        let view = attribute_columns(Arc::new(with_attrs()));
        assert!(view.variable("temp.valid_range").is_none());
    }

    #[test]
    fn a_real_variable_wins_a_name_clash() {
        let view = attribute_columns(Arc::new(clash()));
        assert_eq!(names(&view), ["a", "a.b"]);
        assert_eq!(
            view.variable("a.b").unwrap().data_type(),
            &DataType::Float64
        );
    }
}
