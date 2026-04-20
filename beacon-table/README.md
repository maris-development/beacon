# Beacon Table

Beacon table contains the Beacon Table Format which is the engine behind the Beacon SQL Table implementation.
This format works on each individual column being an Arrow IPC File, and a manifest JSON file that describes the table schema and the list of column files. This format allows for efficient columnar storage and retrieval, and is designed to be compatible with the Arrow ecosystem.

The table is designed to be used with the Beacon Datfusion Engine, which can read and write tables in this format. The engine can also perform various optimizations such as predicate pushdown, column pruning, and parallel execution to improve query performance.

The Beacon Table also supports additional indexes to be built alongside the main table, which can be used to further optimize query performance for specific use cases. These indexes are stored in separate files and are also described in the manifest file.

Deletes are handled by maintaining a separate delete file that contains a deletion vector, which is a bitmap indicating which rows have been deleted. When a query is executed, the engine will read the delete file and filter out any rows that have been marked as deleted. This approach allows for efficient handling of deletes without the need to rewrite the entire table, and also allows for the possibility of implementing features such as time travel or versioning in the future.

Updates can be implemented as a combination of deletes and inserts. When an update is made to a row, the original row can be marked as deleted in the delete file, and a new partition file for each column with the updated values/rows can be inserted into the main table. This approach allows for efficient handling of updates while still maintaining the integrity of the original data.
