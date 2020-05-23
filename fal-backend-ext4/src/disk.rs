use std::{
    io::{self, prelude::*, SeekFrom},
    sync::{atomic, Mutex, MutexGuard},
};

use crate::{block_group, Filesystem};

/// The kind of a given block, which allows for better caching priority, and proper journal
/// support.
pub enum BlockKind {
    /// Data, i.e. the contents of files and directories.
    // TODO: Should the contents of directories be cached more than files' data, especially with
    // HTREEs?
    Data,

    /// Metadata, which contains information about inodes, block groups, extents, etc. By default,
    /// every write of this kind must go through the journal first.
    Metadata,

    /// Global blocks, such as the superblock and the journal superblock. These aren't cached, but
    /// always stored parsed in memory. Those blocks will also go through the journal.
    // TODO: Should the block group descriptor tables be considered global?
    Global,

    /// Blocks of unknown kind, treated the same way as data blocks.
    Unknown,
}

/// The underlying disk of an ext2/3/4 filesystem.
pub struct Disk<T> {
    inner: T,
    info: fal::DiskInfo,
    // TODO: Cache
}

impl<T> Disk<T> {
    pub fn into_inner(self) -> T {
        // TODO: Write everything from the write cache first.
        self.inner
    }
    // pub(crate) because the cache shouldn't be bypassed.
    pub(crate) fn inner(&self) -> &T {
        &self.inner
    }
    fn phys_blocks_per_block(&self, filesystem: &Filesystem<T>) -> u32 {
        filesystem.superblock.block_size() / self.info.block_size
    }
}
impl<T: fal::DeviceRo> Disk<T> {
    /// Wrap a `Read` + `Write` + `Seek` device for optimal caching and journaling.
    pub fn new(inner: T) -> Result<Self, fal::DeviceError> {
        let info = inner.disk_info()?;

        Ok(Self {
            inner,
            info,
        })
    }

    /// Read a block, which is checked for existence in the block group descriptor tables for debug
    /// builds.
    pub fn read_block(
        &self,
        filesystem: &Filesystem<T>,
        kind: BlockKind,
        block_address: u64,
        buffer: &mut [u8],
    ) -> Result<(), fal::DeviceError> {
        debug_assert!(block_address * u64::from(self.phys_blocks_per_block(filesystem)) < self.info.block_count);
        debug_assert!(block_group::block_exists(block_address, filesystem).unwrap_or(false));
        self.read_block_raw(filesystem, kind, block_address, buffer)
    }
    /// Read a block, bypassing the existence check. This is necessary for the code handling the
    /// block group descriptor tables, as it would cause infinite recursion otherwise.
    pub fn read_block_raw(
        &self,
        filesystem: &Filesystem<T>,
        _kind: BlockKind,
        block_address: u64,
        buffer: &mut [u8],
    ) -> Result<(), fal::DeviceError> {
        self.inner.read_blocks(
            block_address * u64::from(self.phys_blocks_per_block(filesystem)),
            buffer,
        )?;
        Ok(())
    }
}
impl<T: fal::Device> Disk<T> {
    /// Write a block, bypassing the existence check.
    pub fn write_block_raw(
        &self,
        filesystem: &Filesystem<T>,
        _kind: BlockKind,
        block_address: u64,
        buffer: &[u8],
    ) -> Result<(), fal::DeviceError> {
        // TODO: Write to the journal first, and once that write is complete, perform the actual
        // write. This behavior can be configured (data=journal|ordered|writeback). By default,
        // metadata will always go through the journal (ordered); while data is written directly,
        // after the metadata it updates is written (e.g. the new length when appending to a file).

        filesystem
            .info
            .kbs_written
            .fetch_add(buffer.len() as u64, atomic::Ordering::Acquire);

        self.inner.write_blocks(
            block_address * u64::from(filesystem.superblock.block_size()),
            buffer,
        )?;
        Ok(())
    }
    /// Write a block, checking for its existence if debug assertions are enabled.
    pub fn write_block(
        &self,
        filesystem: &Filesystem<T>,
        kind: BlockKind,
        block_address: u64,
        buffer: &[u8],
    ) -> Result<(), fal::DeviceError> {
        debug_assert!(block_group::block_exists(block_address, filesystem).unwrap_or(false));
        self.write_block_raw(filesystem, kind, block_address, buffer)
    }
}
