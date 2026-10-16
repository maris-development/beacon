use indexmap::IndexMap;

use crate::NdArrayD;
use std::{collections::HashSet, sync::Arc};
pub mod gridded;
pub mod ragged;

pub struct Dataset {
    pub name: String,
    pub dimensions: IndexMap<String, usize>,
    pub arrays: IndexMap<String, Arc<dyn NdArrayD>>,
    pub invented_dimensions: HashSet<String>,
}
