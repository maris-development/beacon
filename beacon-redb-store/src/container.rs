//! The single physical file that holds *everything*: redb's index and an
//! append-only blob **heap**, in one `.db` file.
//!
//! # Layout
//!
//! ```text
//! [ slot A (4 KiB) ][ slot B (4 KiB) ][ ... extents appended at EOF ... ]
//! ```
//!
//! - **Superblock**: two 4 KiB slots (dual-buffered, checksummed). Written
//!   alternately with a monotonic sequence number so a torn write always leaves
//!   one intact slot. Records redb's extent table, redb's logical length, and the
//!   allocation high-water mark.
//! - **Extents**: every allocation — a chunk of redb's address space or a blob —
//!   is appended at end-of-file and never moved. The extent table only ever
//!   grows; a crash between allocating and recording leaks space, it never
//!   corrupts.
//!
//! # Two regions, one file
//!
//! redb doesn't own the file — it talks to a [`RegionBackend`] that maps redb's
//! logical `[0, len)` address space onto a set of "redb extents" in the file
//! (splitting reads/writes that straddle an extent boundary). Blob bytes live in
//! their own extents. Both grow by appending at EOF; the extent table keeps them
//! apart.
//!
//! # Zero-copy reads
//!
//! The whole file is `mmap`ped. A blob read returns [`bytes::Bytes::from_owner`]
//! over an [`MmapSlice`] — a pointer into the mapping, no copy, no allocation.
//! The `Arc<Mmap>` keeps the mapping alive for as long as the `Bytes` lives. The
//! heap is append-only and bytes below the high-water mark are immutable, so an
//! outstanding slice can never be mutated underneath a reader.
//!
//! # Durability ordering
//!
//! Blob bytes are `pwrite`n and the file `fsync`ed *before* the redb transaction
//! that records their extent commits. So committed metadata can never reference
//! bytes that a crash could lose.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path as FsPath;
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use redb::StorageBackend;
use serde::{Deserialize, Serialize};

/// Size of one superblock slot.
const SLOT_SIZE: u64 = 4096;
/// Two slots, so a torn write leaves the other intact.
const NUM_SLOTS: u64 = 2;
/// First byte available for extents (after both slots).
const DATA_START: u64 = SLOT_SIZE * NUM_SLOTS;
/// Slot framing magic.
const SLOT_MAGIC: u64 = 0x4245_4143_4f4e_5342; // "BEACONSB"
/// Minimum growth when extending redb's region.
const REDB_MIN_GROW: u64 = 1 << 20; // 1 MiB

/// A contiguous physical byte range within the file.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Extent {
    pub offset: u64,
    pub len: u64,
}

/// The persisted superblock payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Superblock {
    /// redb's logical length (what [`StorageBackend::len`] returns).
    redb_logical_len: u64,
    /// Physical extents backing redb's address space, in logical order.
    redb_extents: Vec<Extent>,
    /// Next free offset — the allocation high-water mark (== file length).
    alloc_cursor: u64,
}

struct State {
    sb: Superblock,
    /// Sequence number of the last-written slot; the next write uses `seq + 1`.
    seq: u64,
}

/// The single-file container shared by the object store (for the heap) and the
/// [`RegionBackend`] (for redb).
pub struct Container {
    file: File,
    /// Current mapping of the whole file, swapped out on growth. Reads take a
    /// snapshot `Arc<Mmap>` so growth never invalidates an in-flight slice.
    map: RwLock<Arc<Mmap>>,
    state: Mutex<State>,
}

impl std::fmt::Debug for Container {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Container").finish_non_exhaustive()
    }
}

impl Container {
    /// Open (or create) the container file at `path`, taking an exclusive lock.
    pub fn open(path: impl AsRef<FsPath>) -> io::Result<Arc<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        // One writer per file — same model as redb/SQLite/LMDB.
        file.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "beacon.db is already locked by another process",
            )
        })?;

        let file_len = file.metadata()?.len();
        let (sb, seq, fresh) = if file_len < DATA_START {
            file.set_len(DATA_START)?; // reserve the two superblock slots
            (
                Superblock {
                    redb_logical_len: 0,
                    redb_extents: Vec::new(),
                    alloc_cursor: DATA_START,
                },
                0,
                true,
            )
        } else {
            let (sb, seq) = read_superblock(&file)?;
            (sb, seq, false)
        };

        let map = Self::map_file(&file)?;
        let container = Arc::new(Self {
            file,
            map: RwLock::new(Arc::new(map)),
            state: Mutex::new(State { sb, seq }),
        });
        if fresh {
            let mut st = container.state.lock().unwrap();
            container.persist_locked(&mut st)?;
        }
        Ok(container)
    }

    fn map_file(file: &File) -> io::Result<Mmap> {
        let len = file.metadata()?.len().max(DATA_START);
        // SAFETY: we never expose mutable access to the mapping, and bytes below
        // the high-water mark are immutable, so the mapping is a stable view.
        unsafe { MmapOptions::new().len(len as usize).map(file) }
    }

    /// Re-map after the file has grown, so subsequent reads see new extents.
    fn remap(&self) -> io::Result<()> {
        let new = Self::map_file(&self.file)?;
        *self.map.write().unwrap() = Arc::new(new);
        Ok(())
    }

    /// Persist the superblock to the next slot and fsync. Also flushes any blob
    /// or redb bytes written to the file before this call.
    fn persist_locked(&self, st: &mut State) -> io::Result<()> {
        st.seq += 1;
        let slot = st.seq % NUM_SLOTS;
        let payload = bincode::serialize(&st.sb)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let max_payload = (SLOT_SIZE as usize) - SLOT_HEADER;
        if payload.len() > max_payload {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("superblock too large: {} > {max_payload}", payload.len()),
            ));
        }
        let mut buf = vec![0u8; SLOT_SIZE as usize];
        buf[0..8].copy_from_slice(&SLOT_MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&st.seq.to_le_bytes());
        buf[16..24].copy_from_slice(&checksum(&payload).to_le_bytes());
        buf[24..28].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        buf[SLOT_HEADER..SLOT_HEADER + payload.len()].copy_from_slice(&payload);
        self.file.write_all_at(&buf, slot * SLOT_SIZE)?;
        self.file.sync_data()
    }

    // ---- redb region (RegionBackend delegates here) -----------------------

    fn redb_len(&self) -> u64 {
        self.state.lock().unwrap().sb.redb_logical_len
    }

    fn redb_set_len(&self, new_len: u64) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        let mut cap: u64 = st.sb.redb_extents.iter().map(|e| e.len).sum();
        let grew = new_len > cap;
        while cap < new_len {
            // Grow geometrically (at least double, at least REDB_MIN_GROW) so the
            // extent count stays O(log size) and the table fits one slot.
            let deficit = new_len - cap;
            let grow = deficit.max(cap).max(REDB_MIN_GROW);
            let offset = st.sb.alloc_cursor;
            self.file.set_len(offset + grow)?; // new bytes are zero-filled
            st.sb.redb_extents.push(Extent { offset, len: grow });
            st.sb.alloc_cursor = offset + grow;
            cap += grow;
        }
        st.sb.redb_logical_len = new_len;
        self.persist_locked(&mut st)?;
        drop(st);
        if grew {
            self.remap()?;
        }
        Ok(())
    }

    /// Translate a redb logical read across the extent table.
    fn redb_read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let st = self.state.lock().unwrap();
        let mut done = 0usize;
        let mut logical_base = 0u64;
        for ext in &st.sb.redb_extents {
            if done == out.len() {
                break;
            }
            let ext_end = logical_base + ext.len;
            let cur = offset + done as u64;
            if cur < ext_end {
                let within = cur - logical_base;
                let n = ((ext.len - within) as usize).min(out.len() - done);
                self.file
                    .read_exact_at(&mut out[done..done + n], ext.offset + within)?;
                done += n;
            }
            logical_base = ext_end;
        }
        if done < out.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "redb read past end of mapped region",
            ));
        }
        Ok(())
    }

    /// Translate a redb logical write across the extent table.
    fn redb_write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let st = self.state.lock().unwrap();
        let mut done = 0usize;
        let mut logical_base = 0u64;
        for ext in &st.sb.redb_extents {
            if done == data.len() {
                break;
            }
            let ext_end = logical_base + ext.len;
            let cur = offset + done as u64;
            if cur < ext_end {
                let within = cur - logical_base;
                let n = ((ext.len - within) as usize).min(data.len() - done);
                self.file
                    .write_all_at(&data[done..done + n], ext.offset + within)?;
                done += n;
            }
            logical_base = ext_end;
        }
        if done < data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "redb write past end of mapped region",
            ));
        }
        Ok(())
    }

    fn redb_sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    // ---- heap (blobs) -----------------------------------------------------

    /// Append `chunks` to the heap as one contiguous blob, fsync, and return the
    /// extent. The bytes are durable when this returns, so the caller may safely
    /// record the extent in a redb transaction.
    pub fn heap_write(&self, chunks: &[Bytes]) -> io::Result<Extent> {
        let total: u64 = chunks.iter().map(|c| c.len() as u64).sum();
        let mut st = self.state.lock().unwrap();
        let offset = st.sb.alloc_cursor;
        self.file.set_len(offset + total)?;
        let mut w = offset;
        for chunk in chunks {
            self.file.write_all_at(chunk, w)?;
            w += chunk.len() as u64;
        }
        st.sb.alloc_cursor = offset + total;
        self.persist_locked(&mut st)?; // fsyncs the blob bytes + the new cursor
        drop(st);
        self.remap()?;
        Ok(Extent { offset, len: total })
    }

    /// Zero-copy read of a heap blob's extents as `Bytes` pointing into the mmap.
    pub fn read_extents(&self, extents: &[Extent]) -> Vec<Bytes> {
        let map = self.map.read().unwrap().clone();
        extents
            .iter()
            .map(|e| {
                Bytes::from_owner(MmapSlice {
                    map: map.clone(),
                    off: e.offset as usize,
                    len: e.len as usize,
                })
            })
            .collect()
    }
}

/// Bytes in the slot before the payload: magic(8) + seq(8) + checksum(8) + len(4).
const SLOT_HEADER: usize = 28;

/// A zero-copy view into a shared memory map. Keeps the mapping alive.
struct MmapSlice {
    map: Arc<Mmap>,
    off: usize,
    len: usize,
}

impl AsRef<[u8]> for MmapSlice {
    fn as_ref(&self) -> &[u8] {
        &self.map[self.off..self.off + self.len]
    }
}

/// redb's view of the container: its address space is these extents.
#[derive(Debug)]
pub struct RegionBackend {
    container: Arc<Container>,
}

impl RegionBackend {
    pub fn new(container: Arc<Container>) -> Self {
        Self { container }
    }
}

impl StorageBackend for RegionBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(self.container.redb_len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.container.redb_read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.container.redb_set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.container.redb_sync()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.container.redb_write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        // Called once when the Database is dropped; make sure everything is
        // durable. The file lock is released when the last `Arc<Container>` drops.
        self.container.redb_sync()
    }
}

/// Read the superblock, choosing the valid slot with the highest sequence.
fn read_superblock(file: &File) -> io::Result<(Superblock, u64)> {
    let mut best: Option<(u64, Superblock)> = None;
    for slot in 0..NUM_SLOTS {
        let mut buf = vec![0u8; SLOT_SIZE as usize];
        if file.read_exact_at(&mut buf, slot * SLOT_SIZE).is_err() {
            continue;
        }
        if u64::from_le_bytes(buf[0..8].try_into().unwrap()) != SLOT_MAGIC {
            continue;
        }
        let seq = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let stored_ck = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let len = u32::from_le_bytes(buf[24..28].try_into().unwrap()) as usize;
        if SLOT_HEADER + len > SLOT_SIZE as usize {
            continue;
        }
        let payload = &buf[SLOT_HEADER..SLOT_HEADER + len];
        if checksum(payload) != stored_ck {
            continue; // torn/corrupt slot
        }
        let Ok(sb) = bincode::deserialize::<Superblock>(payload) else {
            continue;
        };
        if best.as_ref().is_none_or(|(s, _)| seq > *s) {
            best = Some((seq, sb));
        }
    }
    match best {
        Some((seq, sb)) => Ok((sb, seq)),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no valid superblock slot (file corrupt or not a beacon.db)",
        )),
    }
}

/// FNV-1a 64-bit — a cheap, dependency-free integrity check for torn-write
/// detection of a superblock slot (not a cryptographic hash).
fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
