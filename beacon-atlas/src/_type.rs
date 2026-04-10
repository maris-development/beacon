#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[repr(transparent)]
pub struct Timestamp(i64);

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Timestamp(value)
    }
}
