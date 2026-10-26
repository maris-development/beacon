//! Carries [`FunctionHooks`] across the plugin boundary.
//!
//! Expressions cross as `datafusion-proto` `LogicalExprNode` bytes. Intervals cross
//! as [`FFI_Interval`], which holds Arrow scalars in the Arrow C Data Interface.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, LazyLock};

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::prelude::SessionContext;
use datafusion_ffi::expr::interval::FFI_Interval;
use datafusion_ffi::util::{FFI_Option, FFI_Result};
use datafusion_proto::bytes::Serializeable;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use super::hooks::{FunctionHooks, Implied};
use crate::panic_safe::panic_message;

/// An [`Implied`] predicate as FFI data.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_Implied {
    /// `true` for [`Implied::Exact`], `false` for [`Implied::Necessary`].
    pub exact: bool,
    /// The predicate as `LogicalExprNode` bytes.
    pub expr: SVec<u8>,
}

/// The hook table of one plugin function.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_FunctionHooks {
    /// The name of the function.
    pub name: SString,
    /// Calls [`FunctionHooks::implied`]. Each argument is `LogicalExprNode` bytes.
    pub implied:
        unsafe extern "C" fn(&Self, args: SVec<SVec<u8>>) -> FFI_Result<FFI_Option<FFI_Implied>>,
    /// Calls [`FunctionHooks::bounds`].
    pub bounds: unsafe extern "C" fn(
        &Self,
        args: SVec<FFI_Interval>,
    ) -> FFI_Result<FFI_Option<FFI_Interval>>,
    /// Calls [`FunctionHooks::propagate`].
    pub propagate: unsafe extern "C" fn(
        &Self,
        output: FFI_Interval,
        args: SVec<FFI_Interval>,
    ) -> FFI_Result<FFI_Option<SVec<FFI_Interval>>>,
    /// Frees the private data. Only the plugin reads it.
    pub release: unsafe extern "C" fn(&mut Self),
    /// The plugin's `Arc<dyn FunctionHooks>`.
    pub private_data: *mut c_void,
}

// SAFETY: the private data is an `Arc<dyn FunctionHooks>`, which is Send and Sync.
unsafe impl Send for FFI_FunctionHooks {}
// SAFETY: see `Send`.
unsafe impl Sync for FFI_FunctionHooks {}

impl FFI_FunctionHooks {
    /// Exports `hooks` of function `name`.
    pub fn new(name: &str, hooks: Arc<dyn FunctionHooks>) -> Self {
        Self {
            name: SString::from(name),
            implied: implied_fn,
            bounds: bounds_fn,
            propagate: propagate_fn,
            release: release_fn,
            private_data: Box::into_raw(Box::new(hooks)) as *mut c_void,
        }
    }

    fn hooks(&self) -> &Arc<dyn FunctionHooks> {
        // SAFETY: `new` stores a boxed Arc, and only `release` frees it.
        unsafe { &*(self.private_data as *const Arc<dyn FunctionHooks>) }
    }

    fn guarded<T>(&self, hook: &str, call: impl FnOnce() -> Result<T>) -> FFI_Result<T> {
        match catch_unwind(AssertUnwindSafe(call)) {
            Ok(Ok(value)) => FFI_Result::Ok(value),
            Ok(Err(error)) => FFI_Result::Err(SString::from(error.to_string().as_str())),
            Err(payload) => FFI_Result::Err(SString::from(
                format!(
                    "{hook} hook of function '{}' panicked: {}",
                    &*self.name,
                    panic_message(payload.as_ref())
                )
                .as_str(),
            )),
        }
    }
}

impl Drop for FFI_FunctionHooks {
    fn drop(&mut self) {
        // SAFETY: the plugin that made the table frees its own data.
        unsafe { (self.release)(self) }
    }
}

/// A task context with the DataFusion built-in functions, to decode expressions.
fn decode_context() -> &'static TaskContext {
    static CONTEXT: LazyLock<Arc<TaskContext>> = LazyLock::new(|| SessionContext::new().task_ctx());
    &CONTEXT
}

fn encode(expr: &Expr) -> Result<SVec<u8>> {
    Ok(SVec::from(expr.to_bytes()?.as_ref()))
}

fn decode(bytes: &[u8]) -> Result<Expr> {
    Expr::from_bytes_with_ctx(bytes, decode_context())
}

fn to_ffi_intervals<'a>(
    intervals: impl IntoIterator<Item = &'a Interval>,
) -> Result<SVec<FFI_Interval>> {
    intervals.into_iter().map(FFI_Interval::try_from).collect()
}

fn from_ffi_intervals(intervals: SVec<FFI_Interval>) -> Result<Vec<Interval>> {
    intervals.into_iter().map(Interval::try_from).collect()
}

fn into_result<T>(result: FFI_Result<T>) -> Result<T> {
    Into::<std::result::Result<T, SString>>::into(result)
        .map_err(|message| DataFusionError::Execution(message.to_string()))
}

unsafe extern "C" fn implied_fn(
    this: &FFI_FunctionHooks,
    args: SVec<SVec<u8>>,
) -> FFI_Result<FFI_Option<FFI_Implied>> {
    this.guarded("implied", || {
        let args = args
            .iter()
            .map(|bytes| decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        let implied = match this.hooks().implied(&args)? {
            None => None,
            Some(Implied::Necessary(expr)) => Some(FFI_Implied {
                exact: false,
                expr: encode(&expr)?,
            }),
            Some(Implied::Exact(expr)) => Some(FFI_Implied {
                exact: true,
                expr: encode(&expr)?,
            }),
        };
        Ok(implied.into())
    })
}

unsafe extern "C" fn bounds_fn(
    this: &FFI_FunctionHooks,
    args: SVec<FFI_Interval>,
) -> FFI_Result<FFI_Option<FFI_Interval>> {
    this.guarded("bounds", || {
        let args = from_ffi_intervals(args)?;
        let args: Vec<&Interval> = args.iter().collect();
        let bounds = this.hooks().bounds(&args)?;
        Ok(bounds.map(FFI_Interval::try_from).transpose()?.into())
    })
}

unsafe extern "C" fn propagate_fn(
    this: &FFI_FunctionHooks,
    output: FFI_Interval,
    args: SVec<FFI_Interval>,
) -> FFI_Result<FFI_Option<SVec<FFI_Interval>>> {
    this.guarded("propagate", || {
        let output = Interval::try_from(output)?;
        let args = from_ffi_intervals(args)?;
        let args: Vec<&Interval> = args.iter().collect();
        let intervals = this.hooks().propagate(&output, &args)?;
        Ok(intervals.as_ref().map(to_ffi_intervals).transpose()?.into())
    })
}

unsafe extern "C" fn release_fn(this: &mut FFI_FunctionHooks) {
    if this.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made the pointer with `Box::into_raw`.
    drop(unsafe { Box::from_raw(this.private_data as *mut Arc<dyn FunctionHooks>) });
    this.private_data = std::ptr::null_mut();
}

/// The hooks of a plugin function, called through the plugin's hook table.
#[derive(Debug)]
pub struct ForeignFunctionHooks(FFI_FunctionHooks);

impl ForeignFunctionHooks {
    /// The name of the function that the hooks belong to.
    pub fn name(&self) -> &str {
        &self.0.name
    }
}

impl From<FFI_FunctionHooks> for ForeignFunctionHooks {
    fn from(table: FFI_FunctionHooks) -> Self {
        Self(table)
    }
}

impl FunctionHooks for ForeignFunctionHooks {
    fn implied(&self, args: &[Expr]) -> Result<Option<Implied>> {
        let args = args.iter().map(encode).collect::<Result<SVec<_>>>()?;
        // SAFETY: the plugin made the table with `FFI_FunctionHooks::new`.
        let result = into_result(unsafe { (self.0.implied)(&self.0, args) })?;
        let Some(implied) = Option::<FFI_Implied>::from(result) else {
            return Ok(None);
        };
        let expr = decode(&implied.expr)?;
        Ok(Some(if implied.exact {
            Implied::Exact(expr)
        } else {
            Implied::Necessary(expr)
        }))
    }

    fn bounds(&self, args: &[&Interval]) -> Result<Option<Interval>> {
        let args = to_ffi_intervals(args.iter().copied())?;
        // SAFETY: see `implied`.
        let result = into_result(unsafe { (self.0.bounds)(&self.0, args) })?;
        Option::<FFI_Interval>::from(result)
            .map(Interval::try_from)
            .transpose()
    }

    fn propagate(&self, output: &Interval, args: &[&Interval]) -> Result<Option<Vec<Interval>>> {
        let output = FFI_Interval::try_from(output)?;
        let args = to_ffi_intervals(args.iter().copied())?;
        // SAFETY: see `implied`.
        let result = into_result(unsafe { (self.0.propagate)(&self.0, output, args) })?;
        Option::<SVec<FFI_Interval>>::from(result)
            .map(from_ffi_intervals)
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{ScalarUDF, col, lit};
    use datafusion::scalar::ScalarValue;
    use datafusion_ffi::util::FFI_Result;

    use super::*;
    use crate::scalar_functions::{
        export_scalar_functions, function, hooks_of, import_scalar_function,
    };

    /// The box interval of argument `low` to `high`.
    fn box_side(args: &[&Interval], low: usize, high: usize) -> Result<Interval> {
        Interval::try_new(args[low].lower().clone(), args[high].upper().clone())
    }

    /// `t_in_box(lon, lat, min_lon, min_lat, max_lon, max_lat)` with all three hooks.
    fn in_box() -> ScalarUDF {
        function("t_in_box")
            .args(vec![DataType::Float64; 6])
            .returns(DataType::Boolean)
            .implies(|args| {
                let lon = args[0].clone().between(args[2].clone(), args[4].clone());
                let lat = args[1].clone().between(args[3].clone(), args[5].clone());
                Ok(Some(Implied::Necessary(lon.and(lat))))
            })
            .bounds(|_| Ok(Some(Interval::TRUE_OR_FALSE)))
            .propagate(|output, args| {
                if output != &Interval::TRUE {
                    return Ok(None);
                }
                let (Some(lon), Some(lat)) = (
                    args[0].intersect(box_side(args, 2, 4)?)?,
                    args[1].intersect(box_side(args, 3, 5)?)?,
                ) else {
                    return Ok(None);
                };
                let mut result = vec![lon, lat];
                result.extend(args[2..].iter().map(|a| (*a).clone()));
                Ok(Some(result))
            })
            .invoke_arrays(|arrays| Ok(Arc::new(BooleanArray::from(vec![true; arrays[0].len()]))))
    }

    fn panicking() -> ScalarUDF {
        function("t_boom")
            .args([DataType::Float64])
            .returns(DataType::Boolean)
            .implies(|_| panic!("implies exploded"))
            .propagate(|_, _| panic!("propagate exploded"))
            .invoke_arrays(|arrays| Ok(Arc::clone(&arrays[0])))
    }

    fn plain() -> ScalarUDF {
        function("t_plain")
            .args([DataType::Float64])
            .returns(DataType::Float64)
            .invoke_arrays(|arrays| Ok(Arc::clone(&arrays[0])))
    }

    fn make() -> Vec<ScalarUDF> {
        vec![in_box(), panicking(), plain()]
    }

    /// The exported hooks of `make`, called back through the FFI table.
    fn foreign_hooks() -> Vec<ForeignFunctionHooks> {
        let FFI_Result::Ok(functions) = export_scalar_functions(make) else {
            panic!("expected functions");
        };
        functions
            .into_iter()
            .filter_map(|function| Option::<FFI_FunctionHooks>::from(function.hooks))
            .map(ForeignFunctionHooks::from)
            .collect()
    }

    fn point(v: f64) -> Interval {
        Interval::try_new(ScalarValue::from(v), ScalarValue::from(v)).unwrap()
    }

    fn box_args() -> Vec<Interval> {
        let mut args = vec![Interval::make_unbounded(&DataType::Float64).unwrap(); 2];
        args.extend([2.0, 50.0, 8.0, 56.0].map(point));
        args
    }

    #[test]
    fn only_functions_with_hooks_export_a_table() {
        let names: Vec<String> = foreign_hooks()
            .iter()
            .map(|h| h.name().to_string())
            .collect();
        assert_eq!(names, ["t_in_box", "t_boom"]);
        assert!(hooks_of(&plain()).is_none());
        assert!(hooks_of(&in_box()).is_some());
    }

    #[test]
    fn implied_crosses_the_ffi() {
        let hooks = &foreign_hooks()[0];
        let args = vec![
            col("lon"),
            col("lat"),
            lit(2.0),
            lit(50.0),
            lit(8.0),
            lit(56.0),
        ];
        let expected = col("lon")
            .between(lit(2.0), lit(8.0))
            .and(col("lat").between(lit(50.0), lit(56.0)));
        assert_eq!(
            hooks.implied(&args).unwrap(),
            Some(Implied::Necessary(expected))
        );
    }

    #[test]
    fn bounds_and_propagate_cross_the_ffi() {
        let hooks = &foreign_hooks()[0];
        let args = box_args();
        let refs: Vec<&Interval> = args.iter().collect();
        assert_eq!(hooks.bounds(&refs).unwrap(), Some(Interval::TRUE_OR_FALSE));
        let result = hooks.propagate(&Interval::TRUE, &refs).unwrap().unwrap();
        assert_eq!(result[0], Interval::make(Some(2.0), Some(8.0)).unwrap());
        assert_eq!(result[1], Interval::make(Some(50.0), Some(56.0)).unwrap());
        assert_eq!(
            hooks.propagate(&Interval::TRUE_OR_FALSE, &refs).unwrap(),
            None
        );
    }

    #[test]
    fn a_panic_in_a_hook_is_an_error() {
        let hooks = &foreign_hooks()[1];
        let err = hooks.implied(&[col("x")]).unwrap_err().to_string();
        assert!(err.contains("panicked"), "{err}");
        assert!(err.contains("implies exploded"), "{err}");
        let x = Interval::make_unbounded(&DataType::Float64).unwrap();
        let err = hooks
            .propagate(&Interval::TRUE, &[&x])
            .unwrap_err()
            .to_string();
        assert!(err.contains("propagate exploded"), "{err}");
        assert_eq!(hooks.bounds(&[&x]).unwrap(), None);
    }

    #[test]
    fn the_hooked_udf_answers_interval_analysis() {
        let udf = in_box();
        let args = box_args();
        let refs: Vec<&Interval> = args.iter().collect();
        assert_eq!(udf.evaluate_bounds(&refs).unwrap(), Interval::TRUE_OR_FALSE);
        let result = udf
            .propagate_constraints(&Interval::TRUE, &refs)
            .unwrap()
            .unwrap();
        assert_eq!(result[0], Interval::make(Some(2.0), Some(8.0)).unwrap());
    }

    #[test]
    fn a_panicking_hook_falls_back_to_the_default() {
        let udf = panicking();
        let x = Interval::make_unbounded(&DataType::Float64).unwrap();
        let hooked = hooks_of(&udf).unwrap();
        assert!(hooked.implied(&[col("x")]).is_err());
        // DataFusion's default propagation keeps the inputs.
        assert_eq!(
            udf.propagate_constraints(&Interval::TRUE, &[&x]).unwrap(),
            Some(vec![])
        );
    }

    #[test]
    fn an_imported_function_keeps_its_hooks() {
        let FFI_Result::Ok(functions) = export_scalar_functions(make) else {
            panic!("expected functions");
        };
        let udfs: Vec<ScalarUDF> = functions.into_iter().map(import_scalar_function).collect();
        assert_eq!(udfs[0].name(), "t_in_box");
        assert!(hooks_of(&udfs[0]).is_some());
        let args = box_args();
        let refs: Vec<&Interval> = args.iter().collect();
        assert_eq!(
            udfs[0].evaluate_bounds(&refs).unwrap(),
            Interval::TRUE_OR_FALSE
        );
        assert!(hooks_of(&udfs[2]).is_none());
    }
}
