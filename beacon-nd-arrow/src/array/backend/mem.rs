use crate::array::backend::ArrayBackend;
use crate::array::compat_typings::ArrowTypeConversion;
use crate::array::subset::ArraySubset;

#[derive(Debug, Clone)]
pub struct InMemoryArrayBackend<T: ArrowTypeConversion> {
    array: ndarray::ArrayD<T>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
}

impl<T: ArrowTypeConversion> InMemoryArrayBackend<T> {
    pub fn new(array: ndarray::ArrayD<T>, shape: Vec<usize>, dimensions: Vec<String>) -> Self {
        Self {
            array,
            shape,
            dimensions,
        }
    }
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion + Clone> ArrayBackend<T> for InMemoryArrayBackend<T> {
    fn len(&self) -> usize {
        self.array.len()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        self.validate_subset(&subset)?;

        let data_view = self.array.view();
        let sliced = data_view.slice_each_axis(|axis| {
            let axis_index = axis.axis.index();
            let start = subset.start[axis_index] as isize;
            let end = (subset.start[axis_index] + subset.shape[axis_index]) as isize;
            ndarray::Slice::new(start, Some(end), 1)
        });

        Ok(sliced.to_owned())
    }
}
