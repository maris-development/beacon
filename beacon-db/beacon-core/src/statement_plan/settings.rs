//! `SET` / `RESET` / `SHOW` for the `beacon.*` namespace.
//!
//! Everything here is an **AST rewrite**, applied in
//! [`lower_df_statement`](super::lower_df_statement) before DataFusion plans the
//! statement. The AST is the only seam that works for all three:
//! `show_variable_to_plan` validates a name at *plan* time, so a later rewrite
//! would come after the error, and DataFusion's `ConfigOptions::reset` refuses
//! any prefix that is not `datafusion`.
//!
//! Three things happen to a key:
//!
//! 1. A **startup-only** key is rejected, naming the environment variable to edit.
//! 2. A `beacon.<datafusion section>.*` key is rewritten to `datafusion.<...>`.
//!    This is the prefix alias: an operator can spell every engine option in one
//!    namespace, and `datafusion.*` keeps working unchanged.
//! 3. Everything else is left alone. A `beacon.*` key that names a real setting
//!    routes into [`BeaconOptions`] on its own, because that type is a registered
//!    `ConfigExtension`.
//!
//! `RESET` becomes a `SET` to the value the runtime booted with. DataFusion's own
//! `RESET` would restore *its* compiled default, discarding whatever the
//! operator's environment supplied — a `RESET beacon.netcdf.use_rust_reader` on a
//! server started with `BEACON_NETCDF_USE_RUST_READER=true` would silently turn
//! the Rust reader off.
//!
//! # Scope and privilege
//!
//! Unchanged from what `SET datafusion.*` already did: a `SET` applies to the one
//! shared session, so it takes effect for every later query and every user, and
//! `validate_query_plan` admits `LogicalPlan::Statement` only for a super-user.

use beacon_datafusion_ext::settings::{BeaconOptions, BootSettings, startup_only_env_var};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{ResetStatement, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Ident, ObjectName, Set, Statement as SqlAstStatement, Value,
};

/// The `beacon.` namespace, with its separator.
const BEACON_PREFIX: &str = "beacon.";

/// The top-level sections of DataFusion's own `ConfigOptions`.
///
/// `beacon.<section>.<key>` is rewritten onto `datafusion.<section>.<key>`, which
/// is what makes `beacon.` a complete alias rather than a second, partial
/// namespace. Kept as a list because DataFusion hardcodes these names in
/// `ConfigOptions::visit` and exposes no way to enumerate them.
const DATAFUSION_SECTIONS: &[&str] = &[
    "catalog",
    "execution",
    "optimizer",
    "explain",
    "sql_parser",
    "format",
    "runtime",
];

/// `beacon.*` names that stand for a DataFusion option beacon already sets from
/// an environment variable, so the SQL spelling matches the documented `BEACON_*`
/// one rather than exposing where the value happens to live.
const ALIASES: &[(&str, &str)] = &[("beacon.batch_size", "datafusion.execution.batch_size")];

/// Rewrite the settings statements in `statement`; everything else passes
/// through untouched.
pub(crate) fn rewrite_settings_statement(
    session_ctx: &SessionContext,
    statement: DFStatement,
) -> anyhow::Result<DFStatement> {
    match statement {
        DFStatement::Reset(ResetStatement::Variable(name)) => {
            reset_to_boot_value(session_ctx, name)
        }
        DFStatement::Statement(statement) => match *statement {
            SqlAstStatement::Set(Set::SingleAssignment {
                scope,
                hivevar,
                variable,
                values,
            }) => {
                let variable = resolve_object_name(&variable)?;
                Ok(single_assignment(scope, hivevar, variable, values))
            }
            SqlAstStatement::ShowVariable { variable } => {
                Ok(show_variable(resolve_show_variable(variable)?))
            }
            other => Ok(DFStatement::Statement(Box::new(other))),
        },
        other => Ok(other),
    }
}

/// `RESET <key>` as a `SET <key> = <the value the runtime started with>`.
///
/// Falls back to DataFusion's own `RESET` when nothing was recorded for the key —
/// an option whose boot value was unset has no string that would restore it, and
/// `ConfigOptions::reset` handles those correctly for the `datafusion.*` half.
fn reset_to_boot_value(
    session_ctx: &SessionContext,
    name: ObjectName,
) -> anyhow::Result<DFStatement> {
    let key = resolve_object_name(&name)?;
    let boot = BootSettings::from_config(session_ctx.state().config());

    match boot.get(&key) {
        Some(value) => Ok(single_assignment(
            None,
            false,
            key,
            vec![SqlExpr::Value(
                Value::SingleQuotedString(value.to_string()).into(),
            )],
        )),
        None => Ok(DFStatement::Reset(ResetStatement::Variable(object_name(
            &key,
        )))),
    }
}

/// The key a `beacon.*` name resolves to, or an error explaining why it cannot
/// be set. A name outside the namespace is returned unchanged.
fn resolve_key(key: &str) -> anyhow::Result<String> {
    let key = key.to_ascii_lowercase();

    let Some(rest) = key.strip_prefix(BEACON_PREFIX) else {
        // `datafusion.*`, `timezone`, and anything else DataFusion owns.
        return Ok(key);
    };

    // A real beacon setting: `ConfigOptions` routes it to the extension itself.
    if BeaconOptions::has_key(&key) {
        return Ok(key);
    }

    if let Some((_, target)) = ALIASES.iter().find(|(alias, _)| *alias == key) {
        return Ok(target.to_string());
    }

    // The prefix alias: `beacon.execution.batch_size` is
    // `datafusion.execution.batch_size`.
    let section = rest.split('.').next().unwrap_or_default();
    if DATAFUSION_SECTIONS.contains(&section) {
        return Ok(format!("datafusion.{rest}"));
    }

    if let Some(env_var) = startup_only_env_var(&key) {
        anyhow::bail!(
            "`{key}` can only be set when the server starts: set the `{env_var}` \
             environment variable and restart"
        );
    }

    anyhow::bail!(
        "unknown setting `{key}`: `SHOW SETTINGS` lists every setting that can be changed \
         at runtime"
    )
}

/// [`resolve_key`] over a dotted `ObjectName`, as `SET` and `RESET` carry it.
fn resolve_object_name(name: &ObjectName) -> anyhow::Result<String> {
    resolve_key(&join_parts(name))
}

/// The resolved key of an `ALTER SYSTEM` statement.
///
/// The same resolution the live `SET` gets, so the two statements agree on what
/// a name means and differ only in whether the value is written to disk.
pub(crate) fn resolve_statement_key(name: &ObjectName) -> anyhow::Result<String> {
    resolve_object_name(name)
}

/// [`resolve_key`] over the identifier list `SHOW` carries, preserving a trailing
/// `VERBOSE` (which DataFusion strips to widen the output, and which is not part
/// of the key).
fn resolve_show_variable(variable: Vec<Ident>) -> anyhow::Result<Vec<Ident>> {
    let verbose = variable
        .last()
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("verbose"));
    let (key_parts, suffix) = match verbose {
        true => variable.split_at(variable.len() - 1),
        false => (variable.as_slice(), &[][..]),
    };

    let key: String = key_parts
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".");

    // `SHOW ALL`, `SHOW TIMEZONE` and friends are DataFusion's, and a bare `SHOW`
    // has nothing to resolve.
    if key.is_empty() || !key.to_ascii_lowercase().starts_with(BEACON_PREFIX) {
        return Ok(variable);
    }

    let mut resolved = idents(&resolve_key(&key)?);
    resolved.extend_from_slice(suffix);
    Ok(resolved)
}

/// The dotted string form of an object name, ignoring quoting.
fn join_parts(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.clone())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn idents(key: &str) -> Vec<Ident> {
    key.split('.').map(Ident::new).collect()
}

fn object_name(key: &str) -> ObjectName {
    ObjectName::from(idents(key))
}

fn show_variable(variable: Vec<Ident>) -> DFStatement {
    DFStatement::Statement(Box::new(SqlAstStatement::ShowVariable { variable }))
}

fn single_assignment(
    scope: Option<datafusion::sql::sqlparser::ast::ContextModifier>,
    hivevar: bool,
    variable: String,
    values: Vec<SqlExpr>,
) -> DFStatement {
    DFStatement::Statement(Box::new(SqlAstStatement::Set(Set::SingleAssignment {
        scope,
        hivevar,
        variable: object_name(&variable),
        values,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::execution::context::SessionConfig;
    use datafusion::sql::parser::DFParser;

    fn parse(sql: &str) -> DFStatement {
        DFParser::parse_sql(sql).unwrap().pop_front().unwrap()
    }

    /// A session carrying the namespace and a boot snapshot, as the runtime
    /// builds one.
    fn session(default_table: &str) -> SessionContext {
        let mut config = SessionConfig::new();
        config.options_mut().extensions.insert(BeaconOptions {
            default_table: default_table.to_string(),
            ..Default::default()
        });
        config.options_mut().execution.batch_size = 4096;
        let boot = BootSettings::capture(config.options());
        SessionContext::new_with_config(config.with_extension(std::sync::Arc::new(boot)))
    }

    fn rewrite(sql: &str) -> anyhow::Result<String> {
        let ctx = session("observations");
        rewrite_settings_statement(&ctx, parse(sql)).map(|statement| statement.to_string())
    }

    /// The dotted key a rewritten `SHOW` carries.
    ///
    /// Asserted on the identifiers rather than the rendered statement:
    /// `ShowVariable`'s `Display` joins its parts with a *space*, while
    /// DataFusion's planner joins the same parts with a dot to look the key up.
    /// The rendering would therefore make a correct rewrite look wrong.
    fn show_key(sql: &str) -> anyhow::Result<String> {
        let ctx = session("observations");
        let statement = rewrite_settings_statement(&ctx, parse(sql))?;
        let DFStatement::Statement(statement) = statement else {
            panic!("`{sql}` did not stay a SHOW");
        };
        let SqlAstStatement::ShowVariable { variable } = *statement else {
            panic!("`{sql}` did not stay a SHOW");
        };
        Ok(variable
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join("."))
    }

    #[test]
    fn a_beacon_setting_passes_through_unchanged() {
        assert_eq!(
            rewrite("SET beacon.netcdf.use_rust_reader = true").unwrap(),
            "SET beacon.netcdf.use_rust_reader = true"
        );
    }

    /// The prefix alias: every DataFusion section is reachable under `beacon.`,
    /// which is the whole point of the rewrite.
    #[test]
    fn a_datafusion_section_is_rewritten_onto_the_datafusion_prefix() {
        for (input, expected) in [
            (
                "SET beacon.execution.batch_size = 8192",
                "SET datafusion.execution.batch_size = 8192",
            ),
            (
                "SET beacon.optimizer.max_passes = 1",
                "SET datafusion.optimizer.max_passes = 1",
            ),
            (
                "SET beacon.sql_parser.dialect = 'postgres'",
                "SET datafusion.sql_parser.dialect = 'postgres'",
            ),
        ] {
            assert_eq!(rewrite(input).unwrap(), expected, "for `{input}`");
        }
    }

    /// `beacon.batch_size` is the documented `BEACON_BATCH_SIZE`, which the
    /// runtime funnels into DataFusion's batch size.
    #[test]
    fn a_documented_alias_reaches_its_datafusion_option() {
        assert_eq!(
            rewrite("SET beacon.batch_size = 64000").unwrap(),
            "SET datafusion.execution.batch_size = 64000"
        );
    }

    /// `datafusion.*` keeps working exactly as before, and so does every
    /// statement that is not a setting.
    #[test]
    fn statements_outside_the_namespace_are_untouched() {
        for sql in [
            "SET datafusion.execution.batch_size = 8192",
            "SET timezone = 'UTC'",
            "SELECT 1",
        ] {
            assert_eq!(rewrite(sql).unwrap(), sql, "for `{sql}`");
        }
        assert_eq!(show_key("SHOW ALL").unwrap(), "ALL");
        assert_eq!(
            show_key("SHOW datafusion.execution.batch_size").unwrap(),
            "datafusion.execution.batch_size"
        );
    }

    /// A startup-only key would appear to work and change nothing, so it is
    /// rejected — and the error has to name the variable to edit instead.
    #[test]
    fn a_startup_only_key_names_its_environment_variable() {
        let err = rewrite("SET beacon.port = 1234").unwrap_err().to_string();
        assert!(err.contains("BEACON_PORT"), "unhelpful error: {err}");
        assert!(err.contains("restart"), "unhelpful error: {err}");

        // Including the ones behind a family prefix.
        let err = rewrite("SET beacon.s3.bucket = 'x'")
            .unwrap_err()
            .to_string();
        assert!(err.contains("BEACON_S3_*"), "unhelpful error: {err}");

        // And the cache capacities, which look settable next to their siblings.
        let err = rewrite("SET beacon.netcdf.reader_cache_size = 8")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("BEACON_NETCDF_READER_CACHE_SIZE"),
            "unhelpful error: {err}"
        );
    }

    #[test]
    fn an_unknown_key_points_at_show_settings() {
        let err = rewrite("SET beacon.nope = 1").unwrap_err().to_string();
        assert!(err.contains("beacon.nope"), "unhelpful error: {err}");
        assert!(err.contains("SHOW SETTINGS"), "unhelpful error: {err}");
    }

    /// `SHOW` is validated at plan time against the key `entries()` advertises,
    /// so an aliased name has to be rewritten before planning, not after.
    #[test]
    fn show_resolves_the_same_names_as_set() {
        assert_eq!(
            show_key("SHOW beacon.execution.batch_size").unwrap(),
            "datafusion.execution.batch_size"
        );
        assert_eq!(
            show_key("SHOW beacon.default_table").unwrap(),
            "beacon.default_table"
        );
        // A trailing VERBOSE widens the output; it is not part of the key.
        assert_eq!(
            show_key("SHOW beacon.batch_size VERBOSE").unwrap(),
            "datafusion.execution.batch_size.VERBOSE"
        );
    }

    /// The case beacon has to own: DataFusion's `RESET` restores *its* compiled
    /// default, which would discard the operator's environment value. Restoring
    /// the recorded boot value is the whole reason `RESET` is intercepted.
    #[test]
    fn reset_restores_the_value_the_runtime_booted_with() {
        assert_eq!(
            rewrite("RESET beacon.default_table").unwrap(),
            "SET beacon.default_table = 'observations'"
        );
        // …including through the alias, where DataFusion's compiled default
        // (8192) differs from what this runtime started with.
        assert_eq!(
            rewrite("RESET beacon.batch_size").unwrap(),
            "SET datafusion.execution.batch_size = '4096'"
        );
    }

    /// A key with no recorded boot value (an option that started unset) falls
    /// back to DataFusion's own `RESET`, which handles those.
    #[test]
    fn reset_falls_back_when_nothing_was_recorded() {
        let ctx = SessionContext::new();
        let rewritten =
            rewrite_settings_statement(&ctx, parse("RESET datafusion.execution.time_zone"))
                .unwrap();
        assert_eq!(
            rewritten.to_string(),
            "RESET datafusion.execution.time_zone"
        );
    }

    /// A startup-only key must be refused on `RESET` too, not only on `SET`.
    #[test]
    fn reset_rejects_a_startup_only_key() {
        let err = rewrite("RESET beacon.port").unwrap_err().to_string();
        assert!(err.contains("BEACON_PORT"), "unhelpful error: {err}");
    }

    /// Beacon turns off DataFusion's identifier normalization, so a key typed in
    /// upper case would otherwise miss both the settings table and the alias list.
    #[test]
    fn keys_are_matched_without_regard_to_case() {
        assert_eq!(
            rewrite("SET BEACON.EXECUTION.BATCH_SIZE = 8192").unwrap(),
            "SET datafusion.execution.batch_size = 8192"
        );
        assert_eq!(
            show_key("SHOW Beacon.Default_Table").unwrap(),
            "beacon.default_table"
        );
    }
}
