#[derive(Debug, Clone)]
pub struct ArraySubset {
    pub start: Vec<usize>,
    pub shape: Vec<usize>,
}

impl ArraySubset {
    pub fn new(start: Vec<usize>, shape: Vec<usize>) -> Self {
        Self { start, shape }
    }

    /// The elements this subset covers, which is the rows it broadcasts to.
    pub fn rows(&self) -> usize {
        self.shape.iter().product()
    }
}
