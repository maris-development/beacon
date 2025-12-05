use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::error::ArrowError;
use datafusion::common::utils::take_function_args;
use datafusion::common::{
    exec_datafusion_err, exec_err, internal_err, DataFusionError, ExprSchema,
};
use datafusion::functions::core::arrow_cast::ArrowCastFunc;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion::logical_expr::{
    ColumnarValue, ExprSchemable, ReturnFieldArgs, ScalarUDF, Signature, Volatility,
};
use datafusion::{
    common::{arrow_datafusion_err, plan_datafusion_err, plan_err},
    logical_expr::ScalarUDFImpl,
    prelude::Expr,
    scalar::ScalarValue,
};

pub fn try_arrow_cast() -> ScalarUDF {
    ScalarUDF::new_from_impl(TryArrowCastFunc::new())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TryArrowCastFunc {
    signature: Signature,
}

impl TryArrowCastFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryArrowCastFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "try_arrow_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        args.iter().any(|e| e.nullable(schema).ok().unwrap_or(true))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        let [_, type_arg] = take_function_args(self.name(), args.scalar_arguments)?;

        type_arg
            .and_then(|sv| sv.try_as_str().flatten().filter(|s| !s.is_empty()))
            .map_or_else(
                || {
                    exec_err!(
                        "{} requires its second argument to be a non-empty constant string",
                        self.name()
                    )
                },
                |casted_type| match casted_type.parse::<DataType>() {
                    Ok(data_type) => Ok(Field::new(self.name(), data_type, nullable).into()),
                    Err(ArrowError::ParseError(e)) => Err(exec_datafusion_err!("{e}")),
                    Err(e) => Err(arrow_datafusion_err!(e)),
                },
            )
    }

    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> datafusion::error::Result<ExprSimplifyResult> {
        // convert this into a real cast
        let target_type = data_type_from_args(&args)?;
        // remove second (type) argument
        args.pop().unwrap();
        let arg = args.pop().unwrap();

        let source_type = info.get_data_type(&arg)?;
        let new_expr = if source_type == target_type {
            // the argument's data type is already the correct type
            arg
        } else {
            // Use an actual cast to get the correct type
            Expr::TryCast(datafusion::logical_expr::TryCast {
                expr: Box::new(arg),
                data_type: target_type,
            })
        };
        // return the newly written argument to DataFusion
        Ok(ExprSimplifyResult::Simplified(new_expr))
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        internal_err!("try_arrow_cast should have been simplified to cast")
    }
}

/// Returns the requested type from the arguments
fn data_type_from_args(args: &[Expr]) -> datafusion::error::Result<DataType> {
    if args.len() != 2 {
        return plan_err!("arrow_cast needs 2 arguments, {} provided", args.len());
    }
    let Expr::Literal(ScalarValue::Utf8(Some(val)), _) = &args[1] else {
        return plan_err!(
            "arrow_cast requires its second argument to be a constant string, got {:?}",
            &args[1]
        );
    };

    val.parse().map_err(|e| match e {
        // If the data type cannot be parsed, return a Plan error to signal an
        // error in the input rather than a more general ArrowError
        arrow::error::ArrowError::ParseError(e) => plan_datafusion_err!("{e}"),
        e => arrow_datafusion_err!(e),
    })
}
