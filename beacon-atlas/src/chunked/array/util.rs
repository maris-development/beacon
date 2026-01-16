/// Computes how many chunks exist in each dimension.
///
/// # Panics
/// - If `array_shape.len() != chunk_shape.len()`
/// - If any `chunk_shape[k] == 0`
pub fn chunk_counts(array_shape: &[usize], chunk_shape: &[usize]) -> Vec<usize> {
    assert_eq!(
        array_shape.len(),
        chunk_shape.len(),
        "array_shape and chunk_shape must have same length"
    );

    array_shape
        .iter()
        .zip(chunk_shape.iter())
        .map(|(&a, &c)| {
            assert!(c > 0, "chunk size must be non-zero");
            a.div_ceil(c) // ceil division
        })
        .collect()
}

/// Converts an N-dimensional chunk index into a single linear chunk ID.
///
/// # Meaning
/// A chunk index `[1,2,3]` means:
/// - 1st chunk in dimension 0
/// - 2nd chunk in dimension 1
/// - 3rd chunk in dimension 2
///
/// NOT an element index.
///
/// # Panics
/// - If lengths mismatch
/// - If chunk index is out of bounds
///
/// # Example
/// ```
/// let array_shape = [365, 721, 1440];
/// let chunk_shape = [80, 50, 50];
/// let chunk_index = [2, 3, 4];
///
/// let id = chunk_index_to_id(&array_shape, &chunk_shape, &chunk_index);
/// assert_eq!(id, 961);
/// ```
pub fn chunk_index_to_id(
    array_shape: &[usize],
    chunk_shape: &[usize],
    chunk_index: &[usize],
) -> usize {
    let counts = chunk_counts(array_shape, chunk_shape);

    assert_eq!(
        chunk_index.len(),
        counts.len(),
        "chunk_index has wrong dimensionality"
    );

    let mut id = 0usize;
    for (&i, &d) in chunk_index.iter().zip(counts.iter()) {
        assert!(i < d, "chunk index out of bounds");
        id = id * d + i;
    }

    id
}

/// Converts a linear chunk ID back into an N-dimensional chunk index.
///
/// # Panics
/// - If ID exceeds total number of chunks
pub fn id_to_chunk_index(
    mut id: usize,
    array_shape: &[usize],
    chunk_shape: &[usize],
) -> Vec<usize> {
    let counts = chunk_counts(array_shape, chunk_shape);
    let mut index = vec![0usize; counts.len()];

    for k in (0..counts.len()).rev() {
        let d = counts[k];
        index[k] = id % d;
        id /= d;
    }

    assert!(id == 0, "chunk id out of bounds");
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_counts() {
        let array = [365, 721, 1440];
        let chunk = [80, 50, 50];

        let counts = chunk_counts(&array, &chunk);
        assert_eq!(counts, vec![5, 15, 29]);
    }

    #[test]
    fn test_3d_chunk_index() {
        let array = [365, 721, 1440];
        let chunk = [80, 50, 50];
        let index = [2, 3, 4];

        let id = chunk_index_to_id(&array, &chunk, &index);
        assert_eq!(id, 961);

        let decoded = id_to_chunk_index(id, &array, &chunk);
        assert_eq!(decoded, index);
    }

    #[test]
    fn test_1d() {
        let array = [100];
        let chunk = [16];
        let index = [3];

        let id = chunk_index_to_id(&array, &chunk, &index);
        assert_eq!(id, 3);

        let decoded = id_to_chunk_index(id, &array, &chunk);
        assert_eq!(decoded, index);
    }

    #[test]
    fn test_2d() {
        let array = [100, 50];
        let chunk = [16, 10];
        let index = [2, 4];

        let id = chunk_index_to_id(&array, &chunk, &index);
        let decoded = id_to_chunk_index(id, &array, &chunk);

        assert_eq!(decoded, index);
    }

    #[test]
    fn test_5d_round_trip() {
        let array = [100, 200, 300, 400, 500];
        let chunk = [10, 20, 30, 40, 50];

        let counts = chunk_counts(&array, &chunk);

        let index = vec![
            counts[0] - 1,
            counts[1] - 1,
            counts[2] - 1,
            counts[3] - 1,
            counts[4] - 1,
        ];

        let id = chunk_index_to_id(&array, &chunk, &index);
        let decoded = id_to_chunk_index(id, &array, &chunk);

        assert_eq!(decoded, index);
    }

    #[test]
    #[should_panic]
    fn test_chunk_index_oob() {
        let array = [32];
        let chunk = [8];
        let index = [4]; // only 0..3 valid
        chunk_index_to_id(&array, &chunk, &index);
    }
}
