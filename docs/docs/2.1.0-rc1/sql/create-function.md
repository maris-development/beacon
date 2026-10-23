# CREATE FUNCTION

```sql
CREATE FUNCTION map_wod_flag(flag BIGINT) RETURNS VARCHAR
RETURN CASE
    WHEN flag = 0 THEN '1'
    WHEN flag BETWEEN 1 AND 5 THEN '3'
    WHEN flag BETWEEN 6 AND 9 THEN '4'
END
```

A SQL function is one scalar expression with a name. Beacon puts the expression in place of each
call. You do not need Rust code or a new release for a small mapping.

```sql
SELECT flag, map_wod_flag(flag) AS sdn_flag
FROM wod_profiles
WHERE map_wod_flag(flag) = '1'
```

The planner sees the expression, not a function call. Filter pushdown to Parquet and to remote
tables works as for any other expression.

## Syntax

```sql
CREATE [OR REPLACE] FUNCTION <function_name>(<argument> <type>, ...)
    [RETURNS <type>]
    RETURN <expression>
```

- Give each argument a name and a type. The body refers to the argument by its name.
- A call casts each argument to the type of the argument.
- `RETURNS` is optional. Without it, the function returns the type of the body.
- A function can call other functions. A function cannot call itself.

Beacon stores the function. It survives a restart. Beacon loads functions before views, so a view
can call a function.

## Rules for the body

The body is one scalar expression of the arguments. Beacon refuses a body that contains:

- a subquery, for example `(SELECT ...)`, `IN (SELECT ...)` or `EXISTS`
- an aggregate function, for example `sum` or `count`
- a window function, for example `row_number() OVER ()`
- a column that is not an argument

## Rules for the name

- A function name contains only letters, digits and underscores.
- A function name has no schema. Beacon refuses `public.my_function`.
- Beacon refuses the name of a built-in function, for example `abs` or `sum`.
- Names are case-sensitive, as for tables. See [Identifiers & Case](/docs/2.1.0-rc1/sql/identifiers).

A new Beacon release can add a built-in function with the name of your function. At startup, the
built-in function wins. Beacon does not register your function and logs an error with its
statement. Calls and views then use the built-in function. Create your function again under a new
name. Then run `DROP FUNCTION <name>` to remove the stored function.

## Permissions

Only the admin can create or drop a function. Any user can call a function.

## `OR REPLACE`

Define a function again. You do not drop it first:

```sql
CREATE OR REPLACE FUNCTION map_wod_flag(flag BIGINT) RETURNS VARCHAR
RETURN CASE WHEN flag = 0 THEN '1' ELSE '4' END
```

Without `OR REPLACE`, Beacon refuses a name that a function already uses.

## Show the signature

`SHOW FUNCTIONS` shows the argument names, the argument types and the return type. The admin can
run it:

```sql
SHOW FUNCTIONS LIKE 'map_wod_flag'
```

| Column | Value |
| --- | --- |
| `parameters` | the argument names, for example `[flag]` |
| `parameter_types` | the argument types, for example `[Int64]` |
| `return_type` | the return type, for example `String` |
| `description` | the comment of the function |
| `syntax_example` | the call, for example `map_wod_flag(flag)` |

## `COMMENT ON FUNCTION`

Add a comment to a function. `SHOW FUNCTIONS` shows it in the `description` column:

```sql
COMMENT ON FUNCTION map_wod_flag IS 'WOD quality flag to SeaDataNet L20'
```

`IS NULL` removes the comment. `COMMENT IF EXISTS ON FUNCTION` does nothing when the function does
not exist. Beacon stores the comment. It survives a restart, and `CREATE OR REPLACE` keeps it. A
built-in function takes no comment.

## `SHOW CREATE FUNCTION`

`SHOW CREATE FUNCTION` returns the statement that created the function. Only the admin can run it:

```sql
SHOW CREATE FUNCTION map_wod_flag
```

## `DROP FUNCTION`

`DROP FUNCTION` removes a function:

```sql
DROP FUNCTION map_wod_flag

DROP FUNCTION IF EXISTS map_wod_flag
```

Beacon refuses to drop a built-in function. Beacon also refuses to drop a function that another
function calls. Drop the other function first.
