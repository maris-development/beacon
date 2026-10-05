//! The merge rule of one table.
//!
//! A table or a `read_*` call can set the three parts of the rule with keys in
//! its options. A key that is not set takes the value of the process, which
//! the `BEACON_TYPE_WIDENING_*` variables give. Each key is the name of its
//! variable without `BEACON_`, in lowercase.
//!
//! | Key | Values |
//! |---|---|
//! | `type_widening_strategy` | `default`, `numpy` |
//! | `type_widening_on_conflict` | `fail`, `keep_first` |
//! | `type_widening_cast` | `strict`, `lenient` |
//!
//! When no one sets the cast, it follows the conflict setting: `keep_first`
//! gives `lenient`, and `fail` gives `strict`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::plan_datafusion_err;
use datafusion::error::Result;

use super::{ArrowTypeWideningStrategy, DefaultArrowTypeWidening, NumpyArrowTypeWidening, TypeConflict};
use crate::format_options::format_option;

/// The option key of the strategy.
pub const STRATEGY_KEY: &str = "type_widening_strategy";
/// The option key of the conflict setting.
pub const ON_CONFLICT_KEY: &str = "type_widening_on_conflict";
/// The option key of the cast.
pub const CAST_KEY: &str = "type_widening_cast";

/// The rules a merge can apply.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum StrategyKind {
    /// [`DefaultArrowTypeWidening`].
    #[default]
    Default,
    /// [`NumpyArrowTypeWidening`].
    Numpy,
}

impl StrategyKind {
    /// The strategy `value` names, or `value` itself when it names none.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "default" | "" => Ok(Self::Default),
            "numpy" => Ok(Self::Numpy),
            other => Err(other.to_string()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Numpy => "numpy",
        }
    }
}

/// What a scan does with a value that the type of the table cannot hold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum CastMode {
    /// The value is an error.
    #[default]
    Strict,
    /// The value reads as null. A file column that no cast reaches reads as
    /// null for the whole file.
    Lenient,
}

impl CastMode {
    /// The cast `value` names, or `value` itself when it names none.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "strict" => Ok(Self::Strict),
            "lenient" => Ok(Self::Lenient),
            other => Err(other.to_string()),
        }
    }

    /// The cast when no one sets it. `KeepFirst` puts values of two families in
    /// one column, so it reads the other family as null.
    pub fn default_for(on_conflict: TypeConflict) -> Self {
        match on_conflict {
            TypeConflict::Fail => Self::Strict,
            TypeConflict::KeepFirst => Self::Lenient,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Lenient => "lenient",
        }
    }
}

/// The parts of the rule that one table sets. `None` takes the value of the
/// process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TypeWideningOverrides {
    pub strategy: Option<StrategyKind>,
    pub on_conflict: Option<TypeConflict>,
    pub cast: Option<CastMode>,
}

impl TypeWideningOverrides {
    /// Read the three keys from the options of a table or a `read_*` call. The
    /// error names the key, the value and the permitted values.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            strategy: parse_key(options, STRATEGY_KEY, "'default', 'numpy'", StrategyKind::parse)?,
            on_conflict: parse_key(
                options,
                ON_CONFLICT_KEY,
                "'fail', 'keep_first'",
                TypeConflict::parse,
            )?,
            cast: parse_key(options, CAST_KEY, "'strict', 'lenient'", CastMode::parse)?,
        })
    }

    /// Whether the table sets no part of the rule.
    pub fn is_empty(&self) -> bool {
        self.strategy.is_none() && self.on_conflict.is_none() && self.cast.is_none()
    }

    /// The parts that are set, as stable text for a schema fingerprint.
    pub fn fingerprint_parts(&self) -> [&'static str; 3] {
        [
            self.strategy.map_or("", StrategyKind::name),
            self.on_conflict.map_or("", |value| match value {
                TypeConflict::Fail => "fail",
                TypeConflict::KeepFirst => "keep_first",
            }),
            self.cast.map_or("", CastMode::name),
        ]
    }
}

fn parse_key<T>(
    options: &HashMap<String, String>,
    key: &str,
    permitted: &str,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Option<T>> {
    let Some(value) = format_option(options, key) else {
        return Ok(None);
    };
    // A blank value would parse as the default. It is more likely a mistake.
    if value.trim().is_empty() {
        return Err(plan_datafusion_err!(
            "option '{key}' has no value. The values are {permitted}"
        ));
    }
    parse(value).map(Some).map_err(|other| {
        plan_datafusion_err!("option '{key}' names no value: '{other}'. The values are {permitted}")
    })
}

/// The rule of the process, as data. A table resolves its own keys against it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TypeWideningSettings {
    pub strategy: StrategyKind,
    pub on_conflict: TypeConflict,
    /// `None` follows [`CastMode::default_for`] the conflict setting.
    pub cast: Option<CastMode>,
}

impl TypeWideningSettings {
    /// These settings, with each part that `overrides` sets replaced.
    pub fn with_overrides(self, overrides: &TypeWideningOverrides) -> Self {
        Self {
            strategy: overrides.strategy.unwrap_or(self.strategy),
            on_conflict: overrides.on_conflict.unwrap_or(self.on_conflict),
            cast: overrides.cast.or(self.cast),
        }
    }

    /// The cast after the default applies.
    pub fn resolved_cast(&self) -> CastMode {
        self.cast
            .unwrap_or_else(|| CastMode::default_for(self.on_conflict))
    }

    /// The strategy these settings name.
    pub fn build(&self) -> Arc<dyn ArrowTypeWideningStrategy> {
        let (on_conflict, cast) = (self.on_conflict, self.resolved_cast());
        match self.strategy {
            StrategyKind::Default => Arc::new(DefaultArrowTypeWidening { on_conflict, cast }),
            StrategyKind::Numpy => Arc::new(NumpyArrowTypeWidening { on_conflict, cast }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn no_key_gives_no_override() {
        let overrides = TypeWideningOverrides::from_options(&options(&[("delimiter", ";")])).unwrap();
        assert!(overrides.is_empty());
    }

    #[test]
    fn the_keys_read_both_spellings() {
        let overrides = TypeWideningOverrides::from_options(&options(&[
            ("type_widening_strategy", "NumPy"),
            ("format.type_widening_on_conflict", "keep_first"),
            ("type_widening_cast", " strict "),
        ]))
        .unwrap();
        assert_eq!(
            overrides,
            TypeWideningOverrides {
                strategy: Some(StrategyKind::Numpy),
                on_conflict: Some(TypeConflict::KeepFirst),
                cast: Some(CastMode::Strict),
            }
        );
    }

    #[test]
    fn an_unknown_value_names_the_key_and_the_values() {
        let error = TypeWideningOverrides::from_options(&options(&[("type_widening_cast", "loose")]))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("type_widening_cast") && error.contains("loose") && error.contains("'lenient'"),
            "{error}"
        );

        let error = TypeWideningOverrides::from_options(&options(&[("type_widening_strategy", "")]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("type_widening_strategy") && error.contains("no value"), "{error}");
    }

    #[test]
    fn the_cast_follows_the_conflict_setting_when_no_one_sets_it() {
        let process = TypeWideningSettings::default();
        assert_eq!(process.resolved_cast(), CastMode::Strict);

        let keep_first = TypeWideningOverrides {
            on_conflict: Some(TypeConflict::KeepFirst),
            ..Default::default()
        };
        assert_eq!(process.with_overrides(&keep_first).resolved_cast(), CastMode::Lenient);

        // A cast of the process wins over the default of the table.
        let strict_process = TypeWideningSettings {
            cast: Some(CastMode::Strict),
            ..Default::default()
        };
        assert_eq!(
            strict_process.with_overrides(&keep_first).resolved_cast(),
            CastMode::Strict
        );
    }

    #[test]
    fn each_key_replaces_one_part() {
        let process = TypeWideningSettings {
            strategy: StrategyKind::Numpy,
            on_conflict: TypeConflict::KeepFirst,
            cast: None,
        };
        let cast_only = TypeWideningOverrides {
            cast: Some(CastMode::Strict),
            ..Default::default()
        };
        assert_eq!(
            process.with_overrides(&cast_only),
            TypeWideningSettings {
                strategy: StrategyKind::Numpy,
                on_conflict: TypeConflict::KeepFirst,
                cast: Some(CastMode::Strict),
            }
        );
        assert_eq!(
            format!("{:?}", process.with_overrides(&cast_only).build()),
            "NumpyArrowTypeWidening { on_conflict: KeepFirst, cast: Strict }"
        );
    }

    #[test]
    fn two_rules_give_two_fingerprints() {
        let lenient = TypeWideningOverrides {
            cast: Some(CastMode::Lenient),
            ..Default::default()
        };
        assert_ne!(
            lenient.fingerprint_parts(),
            TypeWideningOverrides::default().fingerprint_parts()
        );
    }
}
