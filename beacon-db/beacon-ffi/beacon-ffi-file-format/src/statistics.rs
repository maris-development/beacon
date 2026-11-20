//! File statistics as protobuf bytes, the form that `datafusion-ffi` also uses.

use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use prost::Message;
use stabby::vec::Vec as SVec;

/// Encodes `statistics` as `datafusion_proto_common::Statistics` bytes.
pub fn encode_statistics(statistics: &Statistics) -> SVec<u8> {
    let bytes = datafusion_proto_common::Statistics::from(statistics).encode_to_vec();
    let mut out = SVec::new();
    for byte in bytes {
        out.push(byte);
    }
    out
}

/// Decodes bytes from [`encode_statistics`].
pub fn decode_statistics(bytes: &[u8]) -> Result<Statistics> {
    let proto = datafusion_proto_common::Statistics::decode(bytes)
        .map_err(|e| DataFusionError::Execution(format!("cannot decode file statistics: {e}")))?;
    Statistics::try_from(&proto)
}
