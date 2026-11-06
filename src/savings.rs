//! Best-effort physical data-space attribution for successful Btrfs dedupe.
//! Measurement never decides whether sharing is safe and never fails a job.
use crate::{
    btrfs_io::{self, Key},
    scan, Range,
};
use anyhow::{ensure, Context, Result};
use std::{
    fs::File,
    io,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
};

const ENCODED: u32 = 0x8;
const SHARED: u32 = 0x2000;
const UNSAFE: u32 = 0x2 | 0x4 | 0x80 | 0x100 | 0x200 | 0x400 | 0x800 | 0x1000;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Extent {
    logical: u64,
    physical: u64,
    length: u64,
    reserved64: [u64; 2],
    flags: u32,
    reserved: [u32; 3],
}
#[repr(C)]
struct Map {
    start: u64,
    length: u64,
    flags: u32,
    mapped: u32,
    count: u32,
    reserved: u32,
    extents: [Extent; 64],
}
#[repr(C)]
struct Header {
    start: u64,
    length: u64,
    flags: u32,
    mapped: u32,
    count: u32,
    reserved: u32,
}
nix::ioctl_readwrite!(fiemap, b'f', 11, Header);

fn mappings(file: &File, range: Range) -> Result<Vec<Extent>> {
    let end = range
        .offset
        .checked_add(range.len)
        .context("range overflow")?;
    let mut cursor = range.offset;
    let mut result = Vec::new();
    while cursor < end {
        let mut map = Map {
            start: cursor,
            length: end - cursor,
            flags: 0,
            mapped: 0,
            count: 64,
            reserved: 0,
            extents: [Extent::default(); 64],
        };
        // SAFETY: the C-layout buffer includes its initialized header and 64
        // writable extents; the file descriptor and storage remain alive.
        unsafe {
            fiemap(
                file.as_raw_fd(),
                std::ptr::addr_of_mut!(map).cast::<Header>(),
            )
        }
        .map_err(io::Error::from)?;
        ensure!(map.mapped <= 64, "invalid FIEMAP extent count");
        if map.mapped == 0 {
            break;
        }
        for extent in &map.extents[..map.mapped as usize] {
            let finish = extent
                .logical
                .checked_add(extent.length)
                .context("extent overflow")?;
            ensure!(
                extent.length > 0 && finish > cursor,
                "FIEMAP did not advance"
            );
            cursor = finish;
            result.push(*extent);
        }
        if result.last().is_some_and(|e| e.flags & 1 != 0) {
            break;
        }
    }
    Ok(result)
}

fn le64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

// Require the complete physical allocation, rather than a slice left over
// after earlier remaps. The file-extent descriptor supplies the actual encoded
// byte count; FIEMAP's length alone would overestimate compressed allocations.
fn allocation(extent: Extent, data: &[u8]) -> Option<u64> {
    if extent.flags & (UNSAFE | SHARED) != 0
        || data.len() < 53
        || data[20] != 1
        || data[17..20] != [0, 0, 0]
        || le64(data, 21)? != extent.physical
        || le64(data, 37)? != 0
        || le64(data, 45)? != extent.length
    {
        return None;
    }
    let disk = le64(data, 29)?;
    let whole = if data[16] == 0 { disk } else { le64(data, 8)? };
    if disk == 0 || whole != extent.length {
        return None;
    }
    Some(disk)
}

struct Before {
    extent: Extent,
    allocated: Option<u64>,
}
pub(crate) struct Snapshot {
    range: Range,
    size: u64,
    fingerprint: Vec<u8>,
    extents: Vec<Before>,
}
impl Snapshot {
    pub(crate) fn capture(file: &File, range: Range) -> Option<Self> {
        Self::capture_inner(file, range).ok()
    }
    fn capture_inner(file: &File, range: Range) -> Result<Self> {
        ensure!(
            crate::backend::filesystem(file)?.key.starts_with(b"btrfs:"),
            "not Btrfs"
        );
        let metadata = file.metadata()?;
        let fingerprint = scan::fingerprint(&metadata);
        let extents = mappings(file, range)?
            .into_iter()
            .map(|extent| {
                let allocated = if extent.flags & (UNSAFE | SHARED) == 0 && extent.physical != 0 {
                    let key = Key {
                        ino: metadata.ino(),
                        kind: 108,
                        offset: extent.logical,
                    };
                    btrfs_io::search(file, 0, key, key, 0)
                        .ok()
                        .and_then(|items| {
                            items
                                .first()
                                .and_then(|item| allocation(extent, &item.data))
                        })
                } else {
                    None
                };
                let allocated = allocated.filter(|bytes| {
                    let key = Key {
                        ino: extent.physical,
                        kind: 168,
                        offset: *bytes,
                    };
                    btrfs_io::search(file, 2, key, key, 0)
                        .ok()
                        .is_some_and(|items| {
                            items.first().is_some_and(|item| {
                                le64(&item.data, 0) == Some(1)
                                    && le64(&item.data, 16).is_some_and(|flags| flags & 1 != 0)
                            })
                        })
                });
                Before { extent, allocated }
            })
            .collect();
        ensure!(
            scan::fingerprint(&file.metadata()?) == fingerprint,
            "file changed during measurement"
        );
        Ok(Self {
            range,
            size: metadata.len(),
            fingerprint,
            extents,
        })
    }
    pub(crate) fn finish(self, file: &File, accepted: u64) -> Option<(u64, u64)> {
        self.finish_inner(file, accepted).ok()
    }
    fn finish_inner(self, file: &File, accepted: u64) -> Result<(u64, u64)> {
        ensure!(
            accepted > 0 && accepted <= self.range.len,
            "invalid dedupe progress"
        );
        ensure!(
            scan::fingerprint(&file.metadata()?) == self.fingerprint,
            "file changed during measurement"
        );
        let range = Range {
            offset: self.range.offset,
            len: accepted,
        };
        let after = mappings(file, range)?;
        ensure!(
            after.iter().all(|extent| extent.flags & UNSAFE == 0),
            "uncertain post-dedupe mapping"
        );
        let end = range
            .offset
            .checked_add(accepted)
            .context("range overflow")?;
        let mut saved = 0u64;
        let mut unmeasured = 0u64;
        let mut credited = std::collections::HashSet::new();
        for before in self.extents {
            let extent = before.extent;
            let extent_end = extent
                .logical
                .checked_add(extent.length)
                .context("extent overflow")?
                .min(self.size);
            let overlap = end
                .min(extent_end)
                .saturating_sub(range.offset.max(extent.logical));
            if overlap == 0 {
                continue;
            }
            // Previously shared allocations remain referenced elsewhere, so
            // replacing this reference is conservatively assigned zero savings.
            if extent.flags & SHARED != 0 && extent.flags & UNSAFE == 0 {
                continue;
            }
            let entire = range.offset <= extent.logical && end >= extent_end;
            if let Some(bytes) = before.allocated.filter(|_| entire) {
                let physical_end = extent
                    .physical
                    .checked_add(bytes)
                    .context("allocation overflow")?;
                let retained = after.iter().any(|new| {
                    if new.flags & UNSAFE != 0 {
                        return true;
                    }
                    if new.flags & ENCODED != 0 {
                        new.physical >= extent.physical && new.physical < physical_end
                    } else {
                        new.physical < physical_end
                            && new.physical.saturating_add(new.length) > extent.physical
                    }
                });
                if !retained && credited.insert(extent.physical) {
                    saved = saved.saturating_add(bytes);
                }
                // A mapping that remained on the same allocation saved zero.
            } else {
                unmeasured = unmeasured.saturating_add(overlap);
            }
        }
        ensure!(
            scan::fingerprint(&file.metadata()?) == self.fingerprint,
            "file changed during measurement"
        );
        Ok((saved, accepted.saturating_sub(unmeasured)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn descriptor(compressed: bool, disk: u64) -> (Extent, Vec<u8>) {
        let extent = Extent {
            logical: 0,
            physical: 65536,
            length: 131072,
            ..Default::default()
        };
        let mut data = vec![0; 53];
        data[8..16].copy_from_slice(&131072u64.to_le_bytes());
        data[16] = u8::from(compressed);
        data[20] = 1;
        data[21..29].copy_from_slice(&65536u64.to_le_bytes());
        data[29..37].copy_from_slice(&disk.to_le_bytes());
        data[45..53].copy_from_slice(&131072u64.to_le_bytes());
        (extent, data)
    }
    #[test]
    fn allocation_counts_encoded_bytes_and_excludes_shared_or_partial_extents() {
        let (extent, data) = descriptor(true, 8192);
        assert_eq!(allocation(extent, &data), Some(8192));
        assert_eq!(
            allocation(
                Extent {
                    flags: SHARED,
                    ..extent
                },
                &data
            ),
            None
        );
        let mut partial = data.clone();
        partial[37..45].copy_from_slice(&4096u64.to_le_bytes());
        assert_eq!(allocation(extent, &partial), None);
        let (extent, data) = descriptor(false, 131072);
        assert_eq!(allocation(extent, &data), Some(131072));
        assert_eq!(std::mem::size_of::<Header>(), 32);
        assert_eq!(std::mem::offset_of!(Map, extents), 32);
        assert_eq!(std::mem::size_of::<Extent>(), 56);
    }
}
