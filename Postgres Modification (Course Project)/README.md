In PostgreSQL, indices are not created for all tables and columns. In cases where we perform a query that accesses a column very frequently, that does not have an index associated with it, the cost of the query will be high, and over a long period of time, this cost adds up to make query execution very expensive. In such cases, we could create an index on those accessed frequently and reduce the cost of subsequent executions. However, we must ensure that the index creation cost does not outweigh the rest of the execution cost.

We could change the source code of PostgreSQL such that:

On every SELECT query execution, if we have an equality predicate in that query, we track the column and the table.
We do this by making the code output (table_name, column_name) to a file on such select queries.
We do this by modifying the execMain.c file in src/backend/executor. This file contains the standard_ExecutorRun(), which is the function that executes queries.
