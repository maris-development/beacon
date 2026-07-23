use and::And;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::Column;
use datafusion::execution::SessionState;
use datafusion::logical_expr::*;
use datafusion::{arrow::datatypes::DataType, prelude::Expr};
use gt::Gt;
use gte::Gteq;
use is_not_null::IsNotNull;
use is_null::IsNull;
use lt::Lt;
use lte::Lteq;
use or::Or;

pub mod and;
pub mod between;
pub mod eq;
pub mod geo_json;
pub mod gt;
pub mod gte;
pub mod is_not_null;
pub mod is_null;
pub mod lt;
pub mod lte;
pub mod neq;
pub mod or;

/// A row filter expression for a structured query.
///
/// Boolean combinators (`and`, `or`) nest other filters; the remaining variants
/// are leaf predicates over a single column. Comparison predicates (`eq`, `neq`,
/// `gt`, `gt_eq`, `lt`, `lt_eq`, `between`) and the GeoJSON predicate are matched
/// by their fields rather than a wrapping tag.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    /// All of the contained filters must match.
    #[serde(alias = "and")]
    And(And),
    /// Any of the contained filters must match.
    #[serde(alias = "or")]
    Or(Or),
    #[serde(
        alias = "is_not_null",
        alias = "skip_fill_values",
        alias = "skip_missing"
    )]
    IsNotNull(IsNotNull),
    #[serde(alias = "is_null")]
    IsNull(IsNull),
    #[serde(untagged)]
    Between(between::Between),
    #[serde(untagged)]
    Eq(eq::Eq),
    #[serde(untagged)]
    Neq(neq::Neq),
    #[serde(untagged)]
    Gt(Gt),
    #[serde(untagged)]
    Gteq(Gteq),
    #[serde(untagged)]
    Lt(Lt),
    #[serde(untagged)]
    Lteq(Lteq),
    #[serde(untagged)]
    GeoJson(geo_json::GeoJsonFilter),
}

impl Filter {
    pub fn parse(
        &self,
        session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Filter::And(and) => and.parse(session_state, schema),
            Filter::Or(or) => or.parse(session_state, schema),
            Filter::Between(between) => between.parse(session_state, schema),
            Filter::Gt(gt) => gt.parse(session_state, schema),
            Filter::Gteq(gteq) => gteq.parse(session_state, schema),
            Filter::IsNotNull(is_not_null) => is_not_null.parse(session_state, schema),
            Filter::IsNull(is_null) => is_null.parse(session_state, schema),
            Filter::Lt(lt) => lt.parse(session_state, schema),
            Filter::Lteq(lteq) => lteq.parse(session_state, schema),
            Filter::Eq(eq) => eq.parse(session_state, schema),
            Filter::Neq(neq) => neq.parse(session_state, schema),
            Filter::GeoJson(geo_json_filter) => geo_json_filter.parse(session_state, schema),
        }
    }
}

pub(crate) fn try_coerce_number_to_schema(literal: f64, dtype: &DataType) -> Expr {
    let result = match dtype {
        DataType::Int8 => cast::i8(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Int16 => cast::i16(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Int32 => cast::i32(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Int64 => cast::i64(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt8 => cast::u8(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt16 => cast::u16(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt32 => cast::u32(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt64 => cast::u64(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Float32 => cast::f32(literal).map(|v| datafusion::logical_expr::lit(v)),
        _ => Ok(lit(literal)),
    };

    let optionally_coerced_expr = result.unwrap_or_else(|_| lit(literal));
    optionally_coerced_expr
}

pub(crate) fn get_column_type(
    schema: &datafusion::arrow::datatypes::Schema,
    column_name: &str,
) -> Option<DataType> {
    schema
        .field_with_name(column_name)
        .ok()
        .map(|field| field.data_type().clone())
}

pub(crate) fn parse_column_name(name: &str) -> Expr {
    col(Column::from_name(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::Field;
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("depth", DataType::Int32, true),
            Field::new("temperature", DataType::Float64, true),
            Field::new("platform", DataType::Utf8, true),
        ])
    }

    /// Round-trip a JSON filter through `Filter`'s (mostly untagged) deserializer
    /// and render it against the schema. Untagged matching is order-sensitive, so
    /// this is the only way to pin which variant a payload lands on.
    fn parse_json(json: &str) -> Expr {
        let filter: Filter = serde_json::from_str(json).expect("filter should deserialize");
        let session_ctx = SessionContext::new();
        filter
            .parse(&session_ctx.state(), &schema())
            .expect("filter should parse")
    }

    /// A JSON number is coerced to the column's Arrow type, so the predicate does
    /// not force the whole (integer) column to be cast to Float64 at scan time —
    /// which would defeat pushdown and statistics pruning.
    #[test]
    fn number_literals_are_coerced_to_the_column_type() {
        let expr = try_coerce_number_to_schema(3.0, &DataType::Int32);
        assert_eq!(expr, lit(3i32));
        let expr = try_coerce_number_to_schema(3.0, &DataType::UInt8);
        assert_eq!(expr, lit(3u8));
        // A type with no narrowing rule keeps the original Float64 literal.
        let expr = try_coerce_number_to_schema(3.0, &DataType::Utf8);
        assert_eq!(expr, lit(3.0f64));
    }

    /// Coercion is best-effort: a value that does not fit the column's type (or is
    /// not integral) must fall back to the Float64 literal rather than silently
    /// wrapping/truncating into a wrong-but-valid predicate.
    #[test]
    fn out_of_range_number_literals_fall_back_to_float() {
        assert_eq!(try_coerce_number_to_schema(1e18, &DataType::Int8), lit(1e18));
        assert_eq!(try_coerce_number_to_schema(-1.0, &DataType::UInt32), lit(-1.0f64));
    }

    /// A *fractional* bound on an integer column is truncated, not rejected:
    /// `1.5` becomes the `Int32` literal `1`. This is lossy — `depth >= 1.5`
    /// widens to `depth >= 1` — so the behaviour is pinned here deliberately;
    /// changing it (to reject, or to round outward per operator) must be a
    /// conscious decision rather than an accident.
    #[test]
    fn fractional_literals_truncate_into_integer_columns() {
        assert_eq!(try_coerce_number_to_schema(1.5, &DataType::Int32), lit(1i32));
        assert_eq!(try_coerce_number_to_schema(-1.5, &DataType::Int64), lit(-1i64));
    }

    /// An unknown column has no type to coerce against; the leaf predicates take
    /// the uncoerced path instead of erroring, leaving resolution to the planner.
    #[test]
    fn unknown_column_has_no_type_and_is_left_uncoerced() {
        assert_eq!(get_column_type(&schema(), "ghost"), None);
        let expr = parse_json(r#"{"column": "ghost", "eq": 3}"#);
        assert_eq!(expr, parse_column_name("ghost").eq(lit(3.0f64)));
    }

    /// The comparison variants are untagged, so the payload's *field name* alone
    /// selects the predicate. A mis-ordered variant list would silently match the
    /// wrong operator, so pin each spelling.
    #[test]
    fn untagged_comparison_variants_match_by_field_name() {
        let column = parse_column_name("depth");
        assert_eq!(
            parse_json(r#"{"column": "depth", "eq": 3}"#),
            column.clone().eq(lit(3i32))
        );
        assert_eq!(
            parse_json(r#"{"column": "depth", "neq": 3}"#),
            column.clone().not_eq(lit(3i32))
        );
        assert_eq!(
            parse_json(r#"{"column": "depth", "gt": 3}"#),
            column.clone().gt(lit(3i32))
        );
        assert_eq!(
            parse_json(r#"{"column": "depth", "gt_eq": 3}"#),
            column.clone().gt_eq(lit(3i32))
        );
        assert_eq!(
            parse_json(r#"{"column": "depth", "lt": 3}"#),
            column.clone().lt(lit(3i32))
        );
        assert_eq!(
            parse_json(r#"{"column": "depth", "lt_eq": 3}"#),
            column.clone().lt_eq(lit(3i32))
        );
        // `gt_eq` *and* `lt_eq` together is a BETWEEN, not two comparisons.
        assert_eq!(
            parse_json(r#"{"column": "depth", "gt_eq": 0, "lt_eq": 10}"#),
            column.between(lit(0i32), lit(10i32))
        );
    }

    /// The legacy client spellings (`min`/`max`, `for_query_parameter`) are aliases
    /// the API still accepts; they must produce exactly the modern form's predicate.
    #[test]
    fn legacy_aliases_produce_the_same_predicate() {
        let modern = parse_json(r#"{"column": "depth", "gt_eq": 0, "lt_eq": 10}"#);
        assert_eq!(
            parse_json(r#"{"for_query_parameter": "depth", "min": 0, "max": 10}"#),
            modern
        );
        // `skip_fill_values`/`skip_missing` are the legacy names for `is_not_null`.
        let is_not_null = parse_json(r#"{"is_not_null": {"column": "depth"}}"#);
        assert_eq!(
            parse_json(r#"{"skip_fill_values": {"column": "depth"}}"#),
            is_not_null
        );
        assert_eq!(
            parse_json(r#"{"skip_missing": {"column": "depth"}}"#),
            is_not_null
        );
    }

    /// A string value on a comparison selects the string variant, so the literal
    /// stays Utf8 rather than going through the numeric coercion path.
    #[test]
    fn string_values_select_the_string_variant() {
        assert_eq!(
            parse_json(r#"{"column": "platform", "eq": "argo"}"#),
            parse_column_name("platform").eq(lit("argo"))
        );
    }

    /// `and` folds from a `true` seed and `or` from `false`, so an *empty*
    /// combinator degenerates to a constant rather than erroring — `and: []`
    /// matches everything and `or: []` matches nothing.
    #[test]
    fn empty_boolean_combinators_fold_to_their_identity() {
        assert_eq!(parse_json(r#"{"and": []}"#), lit(true));
        assert_eq!(parse_json(r#"{"or": []}"#), lit(false));
    }

    /// Nested combinators keep their structure (and their leaves' coercion), which
    /// is what makes a nested `filter` expressive enough to replace the legacy
    /// flat `filters` list.
    #[test]
    fn nested_combinators_compose_their_leaves() {
        let expr = parse_json(
            r#"{"and": [{"column": "depth", "gt": 0}, {"or": [{"is_null": {"column": "platform"}}]}]}"#,
        );
        let expected = lit(true)
            .and(parse_column_name("depth").gt(lit(0i32)))
            .and(lit(false).or(parse_column_name("platform").is_null()));
        assert_eq!(expr, expected);
    }

    /// A GeoJSON filter is rendered through the registered `st_*` UDFs; on a bare
    /// session (no beacon functions) it must fail with a clear registry error
    /// rather than panic or silently drop the spatial predicate.
    #[test]
    fn geo_json_filter_requires_the_spatial_functions() {
        let filter: Filter = serde_json::from_str(
            r#"{"longitude_column": "lon", "latitude_column": "lat",
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}}"#,
        )
        .expect("geojson filter should deserialize");
        assert!(matches!(filter, Filter::GeoJson(_)));

        let session_ctx = SessionContext::new();
        let error = filter
            .parse(&session_ctx.state(), &schema())
            .expect_err("a bare session has no st_geojson_as_wkt");
        assert!(
            error.to_string().contains("st_geojson_as_wkt"),
            "unexpected error: {error}"
        );
    }

    /// Null literals in a filter payload are not accepted as comparison values —
    /// nullness is expressed with `is_null`/`is_not_null`, and a `= NULL` predicate
    /// would silently match nothing.
    #[test]
    fn null_comparison_values_are_rejected() {
        assert!(serde_json::from_str::<Filter>(r#"{"column": "depth", "eq": null}"#).is_err());
        // ...whereas the dedicated variant works and never touches the schema.
        assert_eq!(
            parse_json(r#"{"is_null": {"column": "depth"}}"#),
            parse_column_name("depth").is_null()
        );
    }

    /// Timestamps are rendered as nanosecond timestamp literals so they compare
    /// against a Timestamp column without a string cast.
    #[test]
    fn timestamp_values_become_nanosecond_literals() {
        let expr = parse_json(r#"{"column": "depth", "eq": "2024-01-01T00:00:00"}"#);
        let expected_nanos = "2024-01-01T00:00:00"
            .parse::<chrono::NaiveDateTime>()
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        assert_eq!(
            expr,
            parse_column_name("depth").eq(lit(ScalarValue::TimestampNanosecond(
                Some(expected_nanos),
                None
            )))
        );
    }
}
