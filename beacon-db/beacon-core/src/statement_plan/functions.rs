//! SQL macro functions:
//! `CREATE [OR REPLACE] FUNCTION <name>(<arg> <type>, ...) [RETURNS <type>] RETURN <expression>`.
//!
//! A macro function is one scalar expression. [`ScalarUDFImpl::simplify`] puts
//! the body in place of each call, with the call arguments in place of the
//! argument names. The optimizer then sees a plain expression, so filter
//! pushdown and remote unparse work as for any other expression.
//!
//! The statement text is stored in `db://__functions/<name>.json`. At startup,
//! [`load_persisted_functions`] registers each stored function before the
//! tables and views load.

use std::{any::Any, collections::HashSet, sync::Arc};

use anyhow::Context as _;
use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use beacon_datafusion_ext::consts::DEFAULT_DB_STORE_URL_OBJECT_URL;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        DFSchema,
    },
    error::Result as DFResult,
    execution::context::SessionState,
    logical_expr::{
        cast,
        execution_props::ExecutionProps,
        simplify::{ExprSimplifyResult, SimplifyContext},
        ColumnarValue, DocSection, Documentation, Expr, ExprSchemable, ScalarFunctionArgs,
        ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    optimizer::simplify_expressions::ExprSimplifier,
    physical_expr::create_physical_expr,
    prelude::SessionContext,
    sql::{
        parser::{DFParserBuilder, Statement as DFStatement},
        planner::IdentNormalizer,
        sqlparser::ast::{
            ArgMode, ColumnDef, CreateFunction, CreateFunctionBody, ExprWithAlias,
            FunctionReturnType, Ident, Statement as SqlAstStatement,
        },
    },
};
use futures::StreamExt as _;
use object_store::{path::Path, ObjectStore, ObjectStoreExt as _};

use crate::plugins::registry::PluginRegistry;

/// The `db://` folder that holds one `<name>.json` per function.
const FUNCTIONS_PREFIX: &str = "__functions";

/// The stored form of a function: its name, the statement that created it and
/// its `COMMENT ON FUNCTION` text.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedFunction {
    name: String,
    definition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
}

fn function_path(name: &str) -> Path {
    Path::from(format!("{FUNCTIONS_PREFIX}/{name}.json"))
}

/// A function made by `CREATE FUNCTION`. The body refers to each argument as an
/// unqualified column with the argument's name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SqlMacroFunction {
    name: String,
    definition: String,
    comment: Option<String>,
    arguments: Vec<String>,
    signature: Signature,
    return_type: DataType,
    body: Expr,
    /// What `SHOW FUNCTIONS` lists: the argument names and the comment.
    documentation: Documentation,
}

impl SqlMacroFunction {
    fn with_comment(mut self, comment: Option<String>) -> Self {
        self.documentation.description = comment.clone().unwrap_or_default();
        self.comment = comment;
        self
    }

    fn persisted(&self) -> PersistedFunction {
        PersistedFunction {
            name: self.name.clone(),
            definition: self.definition.clone(),
            comment: self.comment.clone(),
        }
    }

    /// The body with `args` in place of the argument names.
    fn inline(&self, args: &[Expr]) -> DFResult<Expr> {
        self.body
            .clone()
            .transform(|expr| match expr {
                Expr::Column(column) if column.relation.is_none() => {
                    match self.arguments.iter().position(|name| *name == column.name) {
                        Some(index) => Ok(Transformed::yes(args[index].clone())),
                        None => Ok(Transformed::no(Expr::Column(column))),
                    }
                }
                other => Ok(Transformed::no(other)),
            })
            .map(|transformed| transformed.data)
    }
}

impl ScalarUDFImpl for SqlMacroFunction {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&self.documentation)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> DFResult<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Simplified(self.inline(&args)?))
    }

    // Constant folding calls this before `simplify` can inline the body.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let fields: Vec<Field> = self
            .arguments
            .iter()
            .zip(&args.arg_fields)
            .map(|(name, field)| field.as_ref().clone().with_name(name))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let columns = args
            .args
            .iter()
            .map(|arg| arg.to_array(args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(args.number_rows)),
        )?;
        let body = create_physical_expr(
            &self.body,
            &DFSchema::try_from(schema)?,
            &ExecutionProps::new(),
        )?;
        body.evaluate(&batch)
    }
}

/// `udf` as a macro function, or `None` for a built-in function.
pub(crate) fn as_macro(udf: &ScalarUDF) -> Option<&SqlMacroFunction> {
    (udf.inner().as_ref() as &dyn Any).downcast_ref::<SqlMacroFunction>()
}

/// Whether `expr` calls the function `name`, directly or through a macro function.
fn calls(expr: &Expr, name: &str) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        if let Expr::ScalarFunction(call) = node {
            if call.func.name() == name
                || as_macro(&call.func).is_some_and(|function| calls(&function.body, name))
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Refuse a body that is not one scalar expression of the arguments.
fn check_body(name: &str, body: &Expr) -> anyhow::Result<()> {
    let mut refused = None;
    body.apply(|node| {
        refused = match node {
            Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::SetComparison(_) => Some("a subquery"),
            Expr::AggregateFunction(_) => Some("an aggregate function"),
            Expr::WindowFunction(_) => Some("a window function"),
            Expr::Placeholder(_) => Some("a placeholder; use the argument name"),
            _ => None,
        };
        Ok(match refused {
            Some(_) => TreeNodeRecursion::Stop,
            None => TreeNodeRecursion::Continue,
        })
    })?;
    match refused {
        Some(what) => anyhow::bail!("the body of function '{name}' must not contain {what}"),
        None => Ok(()),
    }
}

/// Plan the `CREATE FUNCTION` statement in `definition`. Also returns whether
/// the statement has `OR REPLACE`.
fn plan_function(
    state: &SessionState,
    definition: &str,
) -> anyhow::Result<(SqlMacroFunction, bool)> {
    let statement = DFParserBuilder::new(definition)
        .build()?
        .parse_statement()?;
    let DFStatement::Statement(statement) = statement else {
        anyhow::bail!("expected a CREATE FUNCTION statement");
    };
    let SqlAstStatement::CreateFunction(create) = *statement else {
        anyhow::bail!("expected a CREATE FUNCTION statement");
    };
    let or_replace = create.or_replace;
    Ok((plan_create_function(state, create, definition)?, or_replace))
}

fn plan_create_function(
    state: &SessionState,
    create: CreateFunction,
    definition: &str,
) -> anyhow::Result<SqlMacroFunction> {
    let options = state.config().options();
    let normalizer = IdentNormalizer::new(options.sql_parser.enable_ident_normalization);

    let [name] = create.name.0.as_slice() else {
        anyhow::bail!("function name '{}' must not have a schema", create.name);
    };
    let name = name
        .as_ident()
        .with_context(|| format!("invalid function name '{}'", create.name))?;
    let name = normalizer.normalize(name.clone());
    anyhow::ensure!(
        !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "function name '{name}' may contain only letters, digits and underscores"
    );
    anyhow::ensure!(
        !create.temporary,
        "CREATE TEMPORARY FUNCTION is not supported"
    );
    if let Some(language) = &create.language {
        anyhow::ensure!(
            language.value.eq_ignore_ascii_case("sql"),
            "LANGUAGE {language} is not supported; the body of a function is one SQL expression"
        );
    }
    let Some(CreateFunctionBody::Return(body)) = create.function_body else {
        anyhow::bail!("give the body of function '{name}' as RETURN <expression>");
    };

    let mut columns = Vec::new();
    for arg in create.args.unwrap_or_default() {
        anyhow::ensure!(
            matches!(arg.mode, None | Some(ArgMode::In)),
            "function '{name}' accepts only input arguments"
        );
        anyhow::ensure!(
            arg.default_expr.is_none(),
            "function '{name}' does not support argument defaults"
        );
        let arg_name = arg
            .name
            .with_context(|| format!("each argument of function '{name}' needs a name"))?;
        columns.push(ColumnDef {
            name: arg_name,
            data_type: arg.data_type,
            options: vec![],
        });
    }
    let arguments = super::actions::sql_columns_to_arrow_schema(options, columns)?;
    let schema = DFSchema::try_from(arguments.clone())
        .map_err(|error| anyhow::anyhow!("invalid arguments for function '{name}': {error}"))?;

    let body = state
        .create_logical_expr_from_sql_expr(
            ExprWithAlias {
                expr: body,
                alias: None,
            },
            &schema,
        )
        .map_err(|error| anyhow::anyhow!("invalid body for function '{name}': {error}"))?;
    check_body(&name, &body)?;
    anyhow::ensure!(
        !calls(&body, &name),
        "function '{name}' must not call itself"
    );
    let body = ExprSimplifier::new(SimplifyContext::default())
        .coerce(body, &schema)
        .map_err(|error| anyhow::anyhow!("invalid body for function '{name}': {error}"))?;

    let body_type = body.get_type(&schema)?;
    let return_type = match create.return_type {
        Some(FunctionReturnType::SetOf(_)) => {
            anyhow::bail!("function '{name}' must return one value; RETURNS SETOF is not supported")
        }
        Some(FunctionReturnType::DataType(data_type)) => {
            let result = ColumnDef {
                name: Ident::new("result"),
                data_type,
                options: vec![],
            };
            super::actions::sql_columns_to_arrow_schema(options, vec![result])?
                .field(0)
                .data_type()
                .clone()
        }
        None => body_type.clone(),
    };
    let body = if body_type == return_type {
        body
    } else {
        anyhow::ensure!(
            arrow::compute::can_cast_types(&body_type, &return_type),
            "the body of function '{name}' returns {body_type}, which does not cast to {return_type}"
        );
        cast(body, return_type.clone())
    };

    let volatility = if body.is_volatile() {
        Volatility::Volatile
    } else {
        Volatility::Immutable
    };
    let types: Vec<DataType> = arguments
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    let signature = if types.is_empty() {
        Signature::nullary(volatility)
    } else {
        Signature::exact(types, volatility)
    };

    let argument_names: Vec<String> = arguments
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let documentation = Documentation {
        doc_section: DocSection {
            include: false,
            label: "SQL functions",
            description: None,
        },
        description: String::new(),
        syntax_example: format!("{name}({})", argument_names.join(", ")),
        sql_example: None,
        arguments: Some(
            arguments
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().to_string()))
                .collect(),
        ),
        alternative_syntax: None,
        related_udfs: None,
    };

    Ok(SqlMacroFunction {
        name,
        definition: definition.to_string(),
        comment: None,
        arguments: argument_names,
        signature,
        return_type,
        body,
        documentation,
    })
}

/// Plan one `CREATE FUNCTION` statement of a plugin SQL file. Nothing is stored.
pub(crate) fn plan_plugin_sql(state: &SessionState, definition: &str) -> anyhow::Result<ScalarUDF> {
    let (function, or_replace) = plan_function(state, definition)?;
    anyhow::ensure!(
        !or_replace,
        "plugin SQL files may hold only CREATE FUNCTION"
    );
    Ok(ScalarUDF::new_from_impl(function))
}

/// The plugin that owns function `name`, if any.
fn plugin_owner(state: &SessionState, name: &str) -> Option<String> {
    state
        .config()
        .get_extension::<PluginRegistry>()
        .and_then(|registry| registry.owner_of(name))
}

/// Whether a scalar, aggregate or window function that is not a macro function
/// uses `name`.
pub(crate) fn is_built_in(state: &SessionState, name: &str) -> bool {
    state
        .scalar_functions()
        .get(name)
        .is_some_and(|udf| as_macro(udf).is_none())
        || state.aggregate_functions().contains_key(name)
        || state.window_functions().contains_key(name)
}

/// Refuse `name` when a built-in function uses it, or when a macro function
/// uses it and `or_replace` is false.
fn check_name(state: &SessionState, name: &str, or_replace: bool) -> anyhow::Result<()> {
    if let Some(plugin) = plugin_owner(state, name) {
        anyhow::bail!("'{name}' belongs to plugin '{plugin}'; choose another name");
    }
    anyhow::ensure!(
        !is_built_in(state, name),
        "'{name}' is a built-in function; choose another name"
    );
    anyhow::ensure!(
        or_replace || !state.scalar_functions().contains_key(name),
        "Function '{name}' already exists; use CREATE OR REPLACE FUNCTION"
    );
    Ok(())
}

fn function_store(session: &SessionContext) -> anyhow::Result<Arc<dyn ObjectStore>> {
    session
        .runtime_env()
        .object_store(&*DEFAULT_DB_STORE_URL_OBJECT_URL)
        .map_err(|error| anyhow::anyhow!("failed to get the database store for functions: {error}"))
}

/// Store `function` and register it, in place of a function with the same name.
async fn store_and_register(
    session: &SessionContext,
    function: SqlMacroFunction,
) -> anyhow::Result<()> {
    let persisted = function.persisted();
    function_store(session)?
        .put(
            &function_path(&persisted.name),
            serde_json::to_vec_pretty(&persisted)?.into(),
        )
        .await
        .map_err(|error| {
            anyhow::anyhow!("failed to store function '{}': {error}", persisted.name)
        })?;

    session.register_udf(ScalarUDF::new_from_impl(function));
    Ok(())
}

/// `CREATE [OR REPLACE] FUNCTION`: plan the statement, store it and register
/// the function. `OR REPLACE` keeps the comment of the old function.
pub(crate) async fn create_function(
    session: &Arc<SessionContext>,
    definition: &str,
) -> anyhow::Result<()> {
    let state = session.state();
    let (function, or_replace) = plan_function(&state, definition)?;
    check_name(&state, &function.name, or_replace)?;
    let comment = state
        .scalar_functions()
        .get(&function.name)
        .and_then(|udf| as_macro(udf))
        .and_then(|old| old.comment.clone());
    store_and_register(session, function.with_comment(comment)).await
}

/// `COMMENT ON FUNCTION <name> IS '<text>' | NULL`. `SHOW FUNCTIONS` shows the
/// comment as the description.
pub(crate) async fn comment_on_function(
    session: &Arc<SessionContext>,
    name: &str,
    comment: Option<String>,
    if_exists: bool,
) -> anyhow::Result<()> {
    let state = session.state();
    if let Some(plugin) = plugin_owner(&state, name) {
        anyhow::bail!("'{name}' belongs to plugin '{plugin}'; a plugin function takes no comment");
    }
    anyhow::ensure!(
        !is_built_in(&state, name),
        "'{name}' is a built-in function; only SQL functions take a comment"
    );
    let Some(function) = state
        .scalar_functions()
        .get(name)
        .and_then(|udf| as_macro(udf))
    else {
        if if_exists {
            return Ok(());
        }
        anyhow::bail!("Function '{name}' does not exist");
    };
    store_and_register(session, function.clone().with_comment(comment)).await
}

/// `DROP FUNCTION [IF EXISTS] <name>`. Refuses a built-in function, and a
/// function that another macro function calls. For a built-in name, it removes
/// only a stored function that the built-in replaced.
pub(crate) async fn drop_function(
    session: &Arc<SessionContext>,
    name: &str,
    if_exists: bool,
) -> anyhow::Result<()> {
    let state = session.state();
    if let Some(plugin) = plugin_owner(&state, name) {
        anyhow::bail!("'{name}' belongs to plugin '{plugin}'; use UNINSTALL {plugin}");
    }
    if is_built_in(&state, name) {
        // A stored function that a newer built-in replaced is removed; the built-in stays.
        let store = function_store(session)?;
        anyhow::ensure!(
            store.head(&function_path(name)).await.is_ok(),
            "'{name}' is a built-in function and cannot be dropped"
        );
        store
            .delete(&function_path(name))
            .await
            .map_err(|error| anyhow::anyhow!("failed to remove function '{name}': {error}"))?;
        return Ok(());
    }
    let functions = state.scalar_functions();
    if !functions.contains_key(name) {
        if if_exists {
            return Ok(());
        }
        anyhow::bail!("Function '{name}' does not exist");
    }
    if let Some(user) = functions
        .values()
        .filter_map(|udf| as_macro(udf))
        .find(|function| function.name != name && calls(&function.body, name))
    {
        anyhow::bail!(
            "Function '{name}' is used by function '{}'; drop that function first",
            user.name
        );
    }

    match function_store(session)?.delete(&function_path(name)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => anyhow::bail!("failed to remove function '{name}': {error}"),
    }
    session.deregister_udf(name);
    Ok(())
}

/// `SHOW CREATE FUNCTION <name>`: one row with the name and the statement.
pub(crate) fn show_create_function(
    session: &SessionContext,
    name: &str,
) -> anyhow::Result<RecordBatch> {
    let state = session.state();
    anyhow::ensure!(
        !is_built_in(&state, name),
        "'{name}' is a built-in function and has no CREATE statement"
    );
    let function = state
        .scalar_functions()
        .get(name)
        .and_then(|udf| as_macro(udf))
        .with_context(|| format!("Function '{name}' does not exist"))?;
    Ok(RecordBatch::try_new(
        super::logical::show_create_function_arrow_schema(),
        vec![
            Arc::new(arrow::array::StringArray::from(vec![function.name.clone()])),
            Arc::new(arrow::array::StringArray::from(vec![function
                .definition
                .clone()])),
        ],
    )?)
}

async fn read_persisted(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
) -> anyhow::Result<PersistedFunction> {
    let bytes = store.get(location).await?.bytes().await?;
    serde_json::from_slice(&bytes).context("failed to parse stored function")
}

fn register_persisted(
    session: &SessionContext,
    persisted: &PersistedFunction,
) -> anyhow::Result<()> {
    let state = session.state();
    let (function, _) = plan_function(&state, &persisted.definition)?;
    check_name(&state, &function.name, true)?;
    let function = function.with_comment(persisted.comment.clone());
    session.register_udf(ScalarUDF::new_from_impl(function));
    Ok(())
}

/// The names of the stored user functions, from the file names under `__functions/`.
pub(crate) async fn stored_function_names(session: &SessionContext) -> HashSet<String> {
    let mut names = HashSet::new();
    let store = match function_store(session) {
        Ok(store) => store,
        Err(error) => {
            tracing::error!("Failed to list functions: {error:#}");
            return names;
        }
    };
    let mut listing = store.list(Some(&Path::from(FUNCTIONS_PREFIX)));
    while let Some(entry) = listing.next().await {
        match entry {
            Ok(meta) => {
                if let Some(name) = meta
                    .location
                    .filename()
                    .and_then(|file| file.strip_suffix(".json"))
                {
                    names.insert(name.to_string());
                }
            }
            Err(error) => tracing::error!("Failed to list functions: {error}"),
        }
    }
    names
}

/// Register every stored function. A function that fails is logged and skipped,
/// so one bad entry cannot stop the database from opening.
pub(crate) async fn load_persisted_functions(session: &SessionContext) {
    let store = match function_store(session) {
        Ok(store) => store,
        Err(error) => {
            tracing::error!("Failed to load functions: {error:#}");
            return;
        }
    };

    let mut pending = Vec::new();
    let mut listing = store.list(Some(&Path::from(FUNCTIONS_PREFIX)));
    while let Some(entry) = listing.next().await {
        let location = match entry {
            Ok(meta) => meta.location,
            Err(error) => {
                tracing::error!("Failed to list functions: {error}");
                continue;
            }
        };
        match read_persisted(&store, &location).await {
            Ok(persisted) => pending.push(persisted),
            Err(error) => tracing::error!("Skipping function at {location}: {error:#}"),
        }
    }

    // A built-in function wins over a stored function with the same name.
    let state = session.state();
    pending.retain(|persisted| {
        let conflict = is_built_in(&state, &persisted.name);
        if conflict {
            tracing::error!(
                "Function '{0}' could not be registered because a built-in function '{0}' already \
                 exists. Calls to '{0}' use the built-in. Create the stored function again under \
                 a new name, then run DROP FUNCTION {0} to remove it. Stored definition: {1}",
                persisted.name,
                persisted.definition
            );
        }
        !conflict
    });

    // A function can call another function, so retry until a pass registers none.
    loop {
        let count = pending.len();
        let mut failed = Vec::new();
        for persisted in pending {
            match register_persisted(session, &persisted) {
                Ok(()) => tracing::info!("Registered function '{}'", persisted.name),
                Err(error) => failed.push((persisted, error)),
            }
        }
        if failed.len() == count {
            for (persisted, error) in failed {
                tracing::error!(
                    "Failed to load function '{}': {error:#}. Skipping it.",
                    persisted.name
                );
            }
            return;
        }
        pending = failed.into_iter().map(|(persisted, _)| persisted).collect();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use object_store::memory::InMemory;

    use super::*;

    /// Collects the log lines that a test makes.
    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    async fn store_function(store: &InMemory, name: &str, definition: &str) {
        let persisted = PersistedFunction {
            name: name.to_string(),
            definition: definition.to_string(),
            comment: None,
        };
        store
            .put(
                &function_path(name),
                serde_json::to_vec(&persisted).unwrap().into(),
            )
            .await
            .unwrap();
    }

    /// A stored function whose name a newer built-in uses (here DataFusion's
    /// `abs`) is not registered. The built-in stays, an error is logged, and
    /// `DROP FUNCTION` removes the stored function.
    #[tokio::test]
    async fn a_built_in_wins_over_a_stored_function_with_its_name() {
        let session = Arc::new(SessionContext::new());
        let store = Arc::new(InMemory::new());
        session.register_object_store(DEFAULT_DB_STORE_URL_OBJECT_URL.as_ref(), store.clone());
        store_function(
            &store,
            "abs",
            "CREATE FUNCTION abs(a BIGINT) RETURN a + 100",
        )
        .await;
        store_function(
            &store,
            "plus_one",
            "CREATE FUNCTION plus_one(a BIGINT) RETURN a + 1",
        )
        .await;

        let logs = LogBuffer::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        load_persisted_functions(&session).await;
        drop(guard);

        let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("ERROR")
                && logs.contains(
                    "Function 'abs' could not be registered because a built-in function 'abs' \
                     already exists"
                )
                && logs.contains("Stored definition: CREATE FUNCTION abs(a BIGINT) RETURN a + 100"),
            "logs were:\n{logs}"
        );
        let state = session.state();
        assert!(as_macro(&state.scalar_functions()["abs"]).is_none());
        assert!(as_macro(&state.scalar_functions()["plus_one"]).is_some());
        let rows = session
            .sql("SELECT abs(-1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // The built-in gives 1; the stored function would give -1 + 100.
        let value = arrow::compute::cast(rows[0].column(0), &DataType::Int64).unwrap();
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            1
        );

        drop_function(&session, "abs", false).await.unwrap();
        assert!(store.head(&function_path("abs")).await.is_err());
        let error = drop_function(&session, "abs", false).await.unwrap_err();
        assert!(error.to_string().contains("built-in function"), "{error}");
    }
}
