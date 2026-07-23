//! CF `scale_factor` / `add_offset` decoding.
//!
//! This module converts packed numeric NetCDF variables (raw integers stored
//! with `scale_factor` and/or `add_offset` attributes) into decoded `f64`
//! values (`raw * scale + offset`).

use std::sync::Arc;

use netcdf::NcTypeDescriptor;
use num_traits::AsPrimitive;

use crate::decoders::VariableDecoder;

/// Decoder that wraps a numeric decoder and applies CF `scale_factor` /
/// `add_offset` packing, decoding raw values to `f64` (`raw * scale + offset`).
#[derive(Debug)]
pub struct ScaleOffsetVariableDecoder<T>
where
    T: NcTypeDescriptor + AsPrimitive<f64>,
{
    variable_name: String,
    inner_decoder: Arc<dyn VariableDecoder<T>>,
    scale: f64,
    offset: f64,
    fill_value: Option<f64>,
}

impl<T> ScaleOffsetVariableDecoder<T>
where
    T: NcTypeDescriptor + AsPrimitive<f64>,
{
    /// Create a scale/offset decoder.
    ///
    /// `inner_decoder` provides the raw packed values. `raw_fill_value` is the
    /// fill value in the variable's *packed* units; it is decoded with the same
    /// `raw * scale + offset` arithmetic so a packed fill cell maps exactly to
    /// the decoded fill and is nulled by the engine after decoding.
    pub fn new(
        variable_name: String,
        inner_decoder: Arc<dyn VariableDecoder<T>>,
        scale: f64,
        offset: f64,
        raw_fill_value: Option<f64>,
    ) -> Self {
        Self {
            variable_name,
            inner_decoder,
            scale,
            offset,
            fill_value: raw_fill_value.map(|f| f * scale + offset),
        }
    }
}

impl<T> VariableDecoder<f64> for ScaleOffsetVariableDecoder<T>
where
    T: NcTypeDescriptor + AsPrimitive<f64> + Copy + std::fmt::Debug + Send + Sync + 'static,
{
    fn read(
        &self,
        variable: &netcdf::Variable,
        extents: netcdf::Extents,
    ) -> anyhow::Result<ndarray::ArrayD<f64>> {
        let array = self.inner_decoder.read(variable, extents)?;
        let scale = self.scale;
        let offset = self.offset;
        Ok(array.mapv(|v| v.as_() * scale + offset))
    }

    fn fill_value(&self) -> Option<f64> {
        self.fill_value
    }

    fn variable_name(&self) -> &str {
        &self.variable_name
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::decoders::{DefaultVariableDecoder, VariableDecoder};

    use super::*;

    /// The decoded fill value equals the packed fill run through the same
    /// `raw * scale + offset` arithmetic, so packed fill cells null out.
    #[test]
    fn decoded_fill_matches_packed_fill_arithmetic() {
        let inner: Arc<dyn VariableDecoder<i16>> =
            Arc::new(DefaultVariableDecoder::<i16>::new("sst".to_string(), Some(-32768)));
        let decoder = ScaleOffsetVariableDecoder::new(
            "sst".to_string(),
            inner,
            0.01,
            273.15,
            Some(-32768.0),
        );
        assert_eq!(decoder.fill_value(), Some(-32768.0 * 0.01 + 273.15));
    }

    /// With no fill, the decoder still exposes `None`.
    #[test]
    fn no_fill_stays_none() {
        let inner: Arc<dyn VariableDecoder<i16>> =
            Arc::new(DefaultVariableDecoder::<i16>::new("sst".to_string(), None));
        let decoder =
            ScaleOffsetVariableDecoder::new("sst".to_string(), inner, 0.01, 0.0, None);
        assert_eq!(decoder.fill_value(), None);
    }
}
