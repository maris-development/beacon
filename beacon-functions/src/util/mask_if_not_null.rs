use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::{
    common::{exec_err, internal_err, ExprSchema},
    logical_expr::{
        conditional_expressions::CaseBuilder,
        simplify::{ExprSimplifyResult, SimplifyInfo},
        ColumnarValue, ExprSchemable, ReturnFieldArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    },
    physical_plan::expressions::CaseExpr,
    prelude::{is_null, Expr},
};

pub fn mask_if_not_null() -> ScalarUDF {
    ScalarUDF::new_from_impl(MaskIfNotNullFunc::new())
}

#[derive(Debug, Clone)]
pub struct MaskIfNotNullFunc {
    signature: Signature,
}

impl MaskIfNotNullFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MaskIfNotNullFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "mask_if_not_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        args.iter().any(|e| e.nullable(schema).ok().unwrap_or(true))
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        if arg_types.len() != 2 {
            return exec_err!(
                "mask_if_not_null requires exactly two arguments, got {}",
                arg_types.len()
            );
        }
        Ok(arg_types[1].clone())
    }

    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> datafusion::error::Result<ExprSimplifyResult> {
        if args.len() != 2 {
            return exec_err!(
                "mask_if_not_null requires exactly two arguments, got {}",
                args.len()
            );
        }
        let left = args.remove(0);
        let right = args.remove(0);

        // If the first argument is known to be non-null, we can simplify to the second argument
        if let Ok(false) = info.nullable(&left) {
            return Ok(ExprSimplifyResult::Simplified(right));
        }

        let new_expr = CaseBuilder::new(
            None,
            vec![left.is_not_null()],
            vec![right],
            Some(Box::new(Expr::Literal(
                datafusion::scalar::ScalarValue::Null,
                None,
            ))),
        )
        .end()?;

        Ok(ExprSimplifyResult::Simplified(new_expr))
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        internal_err!("invoke_with_args should not be called for mask_if_not_null")
    }
}
