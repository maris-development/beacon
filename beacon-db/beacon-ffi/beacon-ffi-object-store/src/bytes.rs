//! Byte buffers that cross the plugin boundary without a copy.

use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};

use ::bytes::Bytes;

/// A [`Bytes`] buffer as FFI data.
///
/// The sender keeps the buffer alive in `owner`. The receiver reads `len` bytes at
/// `data` and calls `release` once when it no longer needs them. `release` may run
/// on any thread. Dropping an `FFI_Bytes` calls `release`.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_Bytes {
    data: *const u8,
    len: usize,
    owner: *mut c_void,
    release: unsafe extern "C" fn(*mut c_void),
}

// SAFETY: the owner is a boxed `Bytes`, which is Send and Sync, and `release` may run on any thread.
unsafe impl Send for FFI_Bytes {}
// SAFETY: see `Send`. The data is read-only.
unsafe impl Sync for FFI_Bytes {}

unsafe extern "C" fn release_bytes(owner: *mut c_void) {
    // SAFETY: `From<Bytes>` stores a boxed `Bytes`, and the receiver calls this once.
    let owner = unsafe { Box::from_raw(owner as *mut Bytes) };
    // A panic must not unwind across the boundary.
    let _ = catch_unwind(AssertUnwindSafe(move || drop(owner)));
}

impl From<Bytes> for FFI_Bytes {
    fn from(bytes: Bytes) -> Self {
        let data = bytes.as_ptr();
        let len = bytes.len();
        Self {
            data,
            len,
            owner: Box::into_raw(Box::new(bytes)) as *mut c_void,
            release: release_bytes,
        }
    }
}

impl Drop for FFI_Bytes {
    fn drop(&mut self) {
        // SAFETY: the owner is still alive, and this is the only release of it.
        unsafe { (self.release)(self.owner) }
    }
}

/// Keeps a received buffer alive inside a [`Bytes`].
struct ForeignOwner {
    data: *const u8,
    len: usize,
    owner: *mut c_void,
    release: unsafe extern "C" fn(*mut c_void),
}

// SAFETY: the sender's owner is Send and Sync, and `release` may run on any thread.
unsafe impl Send for ForeignOwner {}
// SAFETY: see `Send`. The data is read-only.
unsafe impl Sync for ForeignOwner {}

impl AsRef<[u8]> for ForeignOwner {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the sender keeps `len` bytes at `data` alive until `release`.
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }
}

impl Drop for ForeignOwner {
    fn drop(&mut self) {
        // SAFETY: the owner is still alive, and `Bytes` drops its owner once.
        unsafe { (self.release)(self.owner) }
    }
}

impl From<FFI_Bytes> for Bytes {
    fn from(bytes: FFI_Bytes) -> Self {
        // The owner takes over the release, so `FFI_Bytes::drop` must not run.
        let bytes = ManuallyDrop::new(bytes);
        let owner = ForeignOwner {
            data: bytes.data,
            len: bytes.len,
            owner: bytes.owner,
            release: bytes.release,
        };
        if owner.len == 0 {
            // An empty buffer can have an invalid `data` pointer; do not read it.
            drop(owner);
            return Bytes::new();
        }
        Bytes::from_owner(owner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ::bytes::Bytes;

    use super::*;

    struct Counted {
        data: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for Counted {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for Counted {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counted(drops: &Arc<AtomicUsize>) -> Bytes {
        Bytes::from_owner(Counted {
            data: vec![4, 5, 6],
            drops: Arc::clone(drops),
        })
    }

    #[test]
    fn bytes_round_trip_keeps_pointer() {
        let b = Bytes::from(vec![1u8, 2, 3]);
        let p = b.as_ptr();
        let back: Bytes = FFI_Bytes::from(b).into();
        assert_eq!(back.as_ptr(), p);
        assert_eq!(&back[..], &[1, 2, 3]);
    }

    #[test]
    fn bytes_release_runs_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let back: Bytes = FFI_Bytes::from(counted(&drops)).into();
        let clone = back.clone();
        let slice = back.slice(1..);
        drop(back);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(&slice[..], &[5, 6]);
        drop(clone);
        drop(slice);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ffi_bytes_drop_releases() {
        let drops = Arc::new(AtomicUsize::new(0));
        drop(FFI_Bytes::from(counted(&drops)));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn empty_bytes_round_trip() {
        let back: Bytes = FFI_Bytes::from(Bytes::new()).into();
        assert!(back.is_empty());
    }
}
