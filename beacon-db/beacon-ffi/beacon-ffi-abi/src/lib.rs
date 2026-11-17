//! The base of the Beacon plugin FFI: the ABI version and the panic helpers.
//!
//! Each `beacon-ffi-*` crate depends on this crate. It depends on no other Beacon crate.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// The ABI version of the plugin SDK. Beacon refuses a plugin with a different value.
///
/// It is the one version of all FFI types of the SDK: each table reports it in its
/// `version` field, and an import refuses a table with another value. Increase it on
/// any change of a table layout, of an FFI enum value, or of the `beacon.nd` encoding.
pub const BEACON_PLUGIN_SDK_ABI: u32 = 7;

/// [`BEACON_PLUGIN_SDK_ABI`] as the tables carry it.
pub const ABI_VERSION: u64 = BEACON_PLUGIN_SDK_ABI as u64;

/// Returns [`BEACON_PLUGIN_SDK_ABI`]. The `version` field of each FFI table points here.
pub extern "C" fn sdk_abi_version() -> u64 {
    ABI_VERSION
}

/// The ABI versions of a plugin, as `beacon_plugin_abi` returns them.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeaconPluginAbi {
    /// The value of [`BEACON_PLUGIN_SDK_ABI`] in the plugin.
    pub sdk_abi: u32,
    /// The value of `datafusion_ffi::version()` in the plugin.
    pub ffi_major: u64,
}

/// The name of the ABI symbol, with a trailing NUL.
pub const ABI_SYMBOL: &[u8] = b"beacon_plugin_abi\0";

/// The type of the [`ABI_SYMBOL`] function.
pub type AbiFn = extern "C" fn() -> BeaconPluginAbi;

/// Returns the message of a panic payload.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Runs `drop_work` and swallows a panic in it.
pub fn drop_quietly(drop_work: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(drop_work)) {
        // The payload drop can panic too.
        std::mem::forget(payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_abi_version_is_seven() {
        assert_eq!(BEACON_PLUGIN_SDK_ABI, 7);
        assert_eq!(sdk_abi_version(), 7);
    }

    #[test]
    fn panic_message_reads_text_payloads() {
        let payload = catch_unwind(|| panic!("boom")).unwrap_err();
        assert_eq!(panic_message(payload.as_ref()), "boom");
        let payload = catch_unwind(|| panic!("boom {}", 1)).unwrap_err();
        assert_eq!(panic_message(payload.as_ref()), "boom 1");
        let payload = catch_unwind(|| std::panic::panic_any(7)).unwrap_err();
        assert_eq!(panic_message(payload.as_ref()), "unknown panic");
    }

    struct PanicsOnDrop;

    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("drop boom");
        }
    }

    #[test]
    fn drop_quietly_swallows_a_panic() {
        drop_quietly(|| drop(PanicsOnDrop));
    }
}
