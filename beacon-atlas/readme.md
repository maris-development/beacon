# Layout

collection.atlas/
    atlas.json
    entries_mask.arrow
    entries.arrow
    columns/
        column_0/
            encoding.json
            layout.arrow
            data.arrow
            attributes/
                attribute_0.arrow
                attribute_1.arrow
        
# layout.arrow

```ascii

[dataset-index: u32] [array-start: u64, array-len: u64] [ shape: List<u32> ] [ dimensions: List<Dictionary<u32, String>> ]

```

```json encoding.json

{
    "length": 1000000,
    "size": 1000000,
    "data_type": "i32",
    "batch_size": 128_000,
}

```
