# Layout

collection.atlas/
    entries_mask.arrow
    entries.arrow
    __global_attributes/
        attr1.arrow
        attr2.arrow
    variables/
        variable_1/
            array.arrow
            layout.arrow
            pruning.arrow
            bloom.arrow
            attributes/
                attr1.arrow
                attr2.arrow

# layout.arrow
[dataset-index: u32] [array-index: List<FixedSized<u32;2>>] [chunk-index: List<List<u32>>] [chunk-shape: List<u32>]
