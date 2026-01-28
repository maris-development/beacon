pub trait ArrowPredicate: Send + Sync {
    fn evaluate(&self, array: &arrow::array::ArrayRef) -> arrow::array::BooleanArray;
}
