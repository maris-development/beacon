# Layout Format

Data is stored in a directory structure as follows:

```text

name.atlas/
    default/
        schema/
            schema.json
            entries.arrow (An Arrow Table describing the datasets in this atlas. Null means a deleted dataset.)
        global/
            attributes/
                <attr1>.arrow (Optional, global attributes stored as RLE arrays)
                ...
        variables/
            <variable_name>/
                index.arrow (describes in which record batch each dataset's data is stored)
                data.arrow (contains all arrays for this variable across all datasets)
                ...
                attributes/
                    <attr1>.arrow (Optional, per-variable attributes stored as RLE arrays)
                    <attr2>.arrow
                    ...

    chunked/
        mask.json (Optional, describes which datasets are inactive/deleted)
        <dataset_name>/
            schema.json
            variables/
                <variable_name>/
                    chunks.arrow

```

Datasets contain variables which are nd arrays. Stored as an ND Arrow column type as described in the [beacon-nd-arrow](../beacon-nd-arrow/README.md) README.
Attributes (global and per-variable) are stored as RLE arrow arrays.

A dataset may be stored in either "default" or "chunked" format.
