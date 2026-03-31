use std::sync::Arc;

use datafusion::optimizer::{Analyzer, AnalyzerRule};

pub mod union_by_name;

pub fn get_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = Analyzer::new().rules;
    rules.push(Arc::new(union_by_name::SupercastUnionCoercion));
    rules
}
