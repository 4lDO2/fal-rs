use crate::{calculate_crc32c, read_block, read_block_to_raw, write_block_raw, superblock, superblock::Superblock, Filesystem};
use std::{
    convert::{TryFrom, TryInto},
    io,
    ops::Range,
    sync::atomic,
};

use bitflags::bitflags;
use quick_error::quick_error;
use scroll::{Pread, Pwrite};

#[derive(Debug, Pread, Pwrite)]
pub struct BlockGroupDescriptorBase {
    pub block_bm_baddr_lo: u32,
    pub inode_bm_baddr_lo: u32,
    pub inode_table_baddr_lo: u32,
    pub unalloc_block_count_lo: u16,
    pub unalloc_inode_count_lo: u16,
    pub dir_count_lo: u16,
    pub flags: u16,
    pub exclude_bm_lo: u32,
    pub block_bm_csum_lo: u16,
    pub inode_bm_csum_lo: u16,
    pub itable_unused_lo: u16,
    pub csum: u16,
}

#[derive(Debug, Pread, Pwrite)]
pub struct BlockGroupDescriptor64Ext {
    pub block_bm_baddr_hi: u32,
    pub inode_bm_baddr_hi: u32,
    pub inode_table_baddr_hi: u32,
    pub unalloc_block_count_hi: u16,
    pub unalloc_inode_count_hi: u16,
    pub dir_count_hi: u16,
    pub itable_unused_hi: u16,
    pub exclude_bm_hi: u32,
    pub block_bm_csum_hi: u16,
    pub inode_bm_csum_hi: u16,
    pub reserved: u32,
}

#[derive(Debug)]
pub struct BlockGroupDescriptor {
    pub base: BlockGroupDescriptorBase,
    pub ext: Option<BlockGroupDescriptor64Ext>,
}

bitflags! {
    pub struct BlockGroupDescriptorFlags: u16 {
        const INODE_UNINIT = 0x1;
        const BLOCK_BITMAP_UNINIT = 0x2;
        const INODE_TABLE_ZEROED = 0x4;
    }
}

impl BlockGroupDescriptor {
    pub const SIZE: u64 = 32;
    pub const SIZE_64BIT: u64 = 64;

    pub fn block_usage_bm_start_baddr(&self) -> u64 {
        u64::from(self.base.block_bm_baddr_lo)
            | self
                .ext
                .as_ref()
                .map(|ext| (u64::from(ext.block_bm_baddr_hi) << 32))
                .unwrap_or(0)
    }
    pub fn inode_usage_bm_start_baddr(&self) -> u64 {
        u64::from(self.base.inode_bm_baddr_lo)
            | self
                .ext
                .as_ref()
                .map(|ext| (u64::from(ext.inode_bm_baddr_hi) << 32))
                .unwrap_or(0)
    }
    pub fn inode_table_start_baddr(&self) -> u64 {
        u64::from(self.base.inode_table_baddr_lo)
            | self
                .ext
                .as_ref()
                .map(|ext| (u64::from(ext.inode_table_baddr_hi) << 32))
                .unwrap_or(0)
    }
    pub fn unalloc_block_count(&self) -> u32 {
        u32::from(self.base.unalloc_block_count_lo)
            | self.ext.as_ref().map(|ext| (u32::from(ext.unalloc_block_count_hi) << 16)).unwrap_or(0)
    }
    pub fn unalloc_inode_count(&self) -> u32 {
        u32::from(self.base.unalloc_inode_count_lo)
            | self.ext.as_ref().map(|ext| (u32::from(ext.unalloc_inode_count_hi) << 16)).unwrap_or(0)
    }
    pub fn set_unalloc_block_count(&mut self, count: u32) {
        self.base.unalloc_block_count_lo = count as u16;
        if let Some(ref mut ext) = self.ext {
            ext.unalloc_block_count_hi = (count >> 16) as u16;
        }
    }
    pub fn set_unalloc_inode_count(&mut self, count: u32) {
        self.base.unalloc_inode_count_lo = count as u16;
        if let Some(ref mut ext) = self.ext {
            ext.unalloc_inode_count_hi = (count >> 16) as u16;
        }
    }
    pub fn block_bm_checksum(&self) -> u32 {
        u32::from(self.base.block_bm_csum_lo)
            | self.ext.as_ref().map(|ext| (u32::from(ext.block_bm_csum_hi) << 16)).unwrap_or(0)
    }
    pub fn set_block_bm_checksum(&mut self, checksum: u32) {
        self.base.block_bm_csum_lo = checksum as u16;
        if let Some(ref mut ext) = self.ext {
            ext.block_bm_csum_hi = (checksum >> 16) as u16;
        }
    }
    pub fn set_inode_bm_checksum(&mut self, checksum: u32) {
        self.base.inode_bm_csum_lo = checksum as u16;
        if let Some(ref mut ext) = self.ext {
            ext.inode_bm_csum_hi = (checksum >> 16) as u16;
        }
    }
    pub fn inode_bm_checksum(&self) -> u32 {
        u32::from(self.base.inode_bm_csum_lo)
            | self.ext.as_ref().map(|ext| (u32::from(ext.inode_bm_csum_hi) << 16)).unwrap_or(0)
    }
    fn calculate_crc32c(seed: u32, block_group: u32, bytes: &[u8]) -> u32 {
        let mut checksum = calculate_crc32c(seed, &block_group.to_le_bytes());
        checksum = calculate_crc32c(checksum, &bytes[..0x1E]);
        checksum = calculate_crc32c(checksum, &[0u8; 2]);
        if bytes.len() > 0x20 {
            checksum = calculate_crc32c(checksum, &bytes[0x20..]);
        }
        checksum
    }
    pub fn parse(bytes: &[u8], superblock: &Superblock, index: u32) -> Result<Self, scroll::Error> {
        let this = Self {
            base: bytes.pread_with(0, scroll::LE)?,
            ext: if superblock.is_64bit() {
                Some(bytes.pread_with(Self::SIZE as usize, scroll::LE)?)
            } else {
                None
            },
        };

        if BlockGroupDescriptor::calculate_crc32c(superblock.checksum_seed().unwrap(), index, bytes) as u16 != this.base.csum {
            return Err(scroll::Error::BadInput { size: 2, msg: "block group descriptor checksum mismatch" });
        }

        if BlockGroupDescriptorFlags::from_bits(this.base.flags).is_none() {
            return Err(scroll::Error::BadInput { size: 2, msg: "invalid block group flags bitmask" });
        }

        Ok(this)
    }
    pub fn serialize(this: &Self, seed: u32, block_group: u32, bytes: &mut [u8]) -> Result<(), scroll::Error> {
        bytes.pwrite_with(&this.base, 0, scroll::LE)?;

        if let Some(ref ext) = this.ext {
            bytes.pwrite_with(ext, Self::SIZE as usize, scroll::LE)?;
        }
        let new_checksum = Self::calculate_crc32c(seed, block_group, bytes) as u16;
        bytes.pwrite_with(new_checksum, 0x1E, scroll::LE)?;
        Ok(())
    }
    pub fn flags(&self) -> BlockGroupDescriptorFlags {
        BlockGroupDescriptorFlags::from_bits(self.base.flags).unwrap()
    }
    fn bitmap<'a, 'b, D: fal::Device>(&self, filesystem: &'b Filesystem<D>, start: u64, block_bytes: &'a mut [u8], baddr: u64) -> Result<Option<Bitmap<'a, 'b>>, BgdError> {
        let bm_block_index = (block_group_offset(&filesystem.superblock, baddr) / 8) / u64::from(filesystem.superblock.block_size());

        read_block_to_raw(
            filesystem,
            start + bm_block_index,
            block_bytes,
        )?;
        Ok(Some(Bitmap { bytes: block_bytes, superblock: &filesystem.superblock }))
    }
    pub fn block_bitmap<'a, 'b, D: fal::Device>(&self, filesystem: &'b Filesystem<D>, block_bytes: &'a mut [u8], baddr: u64) -> Result<Option<Bitmap<'a, 'b>>, BgdError> {
        let bm_start_baddr = self.block_usage_bm_start_baddr();
        self.bitmap(filesystem, bm_start_baddr, block_bytes, baddr)
    }
    pub fn inode_bitmap<'a, 'b, D: fal::Device>(&self, filesystem: &'b Filesystem<D>, block_bytes: &'a mut [u8], baddr: u64) -> Result<Option<Bitmap<'a, 'b>>, BgdError> {
        let bm_start_baddr = self.inode_usage_bm_start_baddr();
        self.bitmap(filesystem, bm_start_baddr, block_bytes, baddr)
    }
}

pub fn block_group_address(superblock: &superblock::Superblock, offset: u64) -> u64 {
    offset / u64::from(superblock.block_size())
}
pub fn block_group_offset(superblock: &superblock::Superblock, offset: u64) -> u64 {
    offset % u64::from(superblock.block_size())
}

quick_error! {
    #[derive(Debug)]
    pub enum BgdError {
        IoError(err: io::Error) {
            from()
            description("disk i/o error")
            cause(err)
        }
        ParseError(err: scroll::Error) {
            from()
            description("block group descriptor parsing error")
            cause(err)
            display("block group descriptor parsing error: {}", err)
        }
        ChecksumMismatch {
            description("invalid block group descriptor checksum")
        }
    }
}

fn get_block_group_descriptor<'a, D: fal::Device>(
    filesystem: &Filesystem<D>,
    index: u64,
    block_bytes: &'a mut [u8],
) -> Result<(&'a mut [u8], u64), BgdError> {
    let size = if filesystem.superblock.is_64bit() {
        BlockGroupDescriptor::SIZE_64BIT
    } else {
        BlockGroupDescriptor::SIZE
    };

    let bgdt_first_block = block_group_address(
        &filesystem.superblock,
        superblock::SUPERBLOCK_OFFSET + superblock::SUPERBLOCK_LEN - 1,
    ) + 1;
    let bgdt_offset = bgdt_first_block * u64::from(filesystem.superblock.block_size());
    let absolute_offset = bgdt_offset + index * size;

    read_block_to_raw(
        filesystem,
        block_group_address(&filesystem.superblock, absolute_offset),
        block_bytes,
    )?;
    let rel_offset = block_group_offset(&filesystem.superblock, absolute_offset);
    let descriptor_bytes =
        &mut block_bytes[rel_offset as usize..rel_offset as usize + size as usize];

    Ok((descriptor_bytes, absolute_offset))
}
pub fn load_block_group_descriptor<D: fal::Device>(filesystem: &Filesystem<D>, index: u64) -> Result<BlockGroupDescriptor, BgdError> {
    let mut block_bytes = vec![0; filesystem.superblock.block_size() as usize];

    let (descriptor_bytes, _) = get_block_group_descriptor(filesystem, index, &mut block_bytes)?;
    let desc = BlockGroupDescriptor::parse(
        descriptor_bytes,
        &filesystem.superblock,
        index as u32,
    )?;
    if filesystem.superblock.has_metadata_checksums() {
        if desc.base.csum != BlockGroupDescriptor::calculate_crc32c(filesystem.superblock.checksum_seed().unwrap(), index as u32, descriptor_bytes) as u16 {
            return Err(BgdError::ChecksumMismatch);
        }
    }
    Ok(desc)
}
pub fn store_block_group_descriptor<D: fal::DeviceMut>(
    filesystem: &Filesystem<D>,
    bgdesc: &mut BlockGroupDescriptor,
    index: u64,
) -> Result<(), BgdError> {
    let mut block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];

    let (descriptor_bytes, absolute_offset) = get_block_group_descriptor(filesystem, index, &mut block_bytes)?;
    BlockGroupDescriptor::serialize(bgdesc, filesystem.superblock.checksum_seed().unwrap(), index as u32, descriptor_bytes)?;

    if filesystem.superblock.has_metadata_checksums() {
        bgdesc.base.csum = BlockGroupDescriptor::calculate_crc32c(filesystem.superblock.checksum_seed().unwrap(), index as u32, descriptor_bytes) as u16;
        fal::write_u16(descriptor_bytes, 0x1E, bgdesc.base.csum);
    }

    write_block_raw(filesystem, absolute_offset, &block_bytes)?;

    Ok(())
}
pub fn inode_block_group_index(superblock: &superblock::Superblock, inode: u32) -> u32 {
    (inode - 1) / superblock.inodes_per_group
}
pub fn inode_index_inside_group(superblock: &superblock::Superblock, inode: u32) -> u32 {
    (inode - 1) % superblock.inodes_per_group
}
pub fn inode_exists<D: fal::Device>(inode: u32, filesystem: &Filesystem<D>) -> Result<bool, BgdError> {
    if inode == 0 {
        return Ok(false);
    }

    let group_index = inode_block_group_index(&filesystem.superblock, inode);
    let index_inside_group = inode_index_inside_group(&filesystem.superblock, inode);

    let descriptor = load_block_group_descriptor(filesystem, group_index.into())?;

    if descriptor.flags().contains(BlockGroupDescriptorFlags::INODE_UNINIT) {
        return Ok(false);
    }

    let bm_start_baddr = descriptor.inode_usage_bm_start_baddr();
    let bm_block_index = u32::try_from(
        u64::from(index_inside_group / 8) / u64::from(filesystem.superblock.block_size()),
    )
    .unwrap();

    let block_bytes = read_block(filesystem, bm_start_baddr + u64::from(bm_block_index))?;

    let byte_index_inside_bm =
        u32::try_from(u64::from(index_inside_group) / u64::from(filesystem.superblock.block_size()))
            .unwrap();

    let bm_byte = block_bytes[usize::try_from(byte_index_inside_bm).unwrap()];
    let bm_bit = 1 << ((inode - 1) % 8);

    Ok(bm_byte & bm_bit != 0)
}
pub fn free_inode<D: fal::DeviceMut>(
    _inode: u32,
    _filesystem: &mut Filesystem<D>,
) -> io::Result<()> {
    unimplemented!()
}
pub struct Bitmap<'a, 'b> {
    pub bytes: &'a mut [u8],
    pub superblock: &'b Superblock,
}
impl<'a, 'b> Bitmap<'a, 'b> {
    pub fn set_rel(&mut self, index_inside_group: u32, value: bool) {
        let byte_index_inside_bm = (index_inside_group / 8) as usize;
        self.bytes[byte_index_inside_bm] &= !(1 << (index_inside_group % 8));
        self.bytes[byte_index_inside_bm] |= (value as u8) << (index_inside_group % 8);
    }
    pub fn get_rel(&self, index_inside_group: u32) -> bool {
        let byte_index_inside_bm = index_inside_group / 8;
        (self.bytes[byte_index_inside_bm as usize]) & (1 << (index_inside_group % 8)) != 0
    }
    pub fn set(&mut self, baddr: u64, value: bool) {
        self.set_rel((baddr % u64::from(self.superblock.blocks_per_group)).try_into().unwrap(), value)
    }
    pub fn get(&self, baddr: u64) -> bool {
        self.get_rel((baddr % u64::from(self.superblock.blocks_per_group)).try_into().unwrap())
    }
    pub fn find_first_rel(&self) -> Option<u32> {
        for (idx, byte) in self.bytes.iter().copied().enumerate() {
            if byte != 0xFF {
                let mut byte = byte;
                for i in 0..8 {
                    if byte & 1 == 0 {
                        return Some(idx as u32 * 8 + i as u32)
                    }
                    byte >>= 1;
                }
            }
        }
        None
    }
    pub fn free_blocks_after_start_rel(&self, start: u32, max_bytes_to_read: usize) -> u32 {
        let mut count = start % 8;

        for mut byte in self.bytes.iter().copied().skip(start as usize / 8).take(max_bytes_to_read) {
            if byte == 0x00 {
                count += 8;
            } else {
                for _ in 0..8 {
                    if byte & 1 == 0 {
                        count += 1;
                    }
                    byte >>= 1;
                }
            }
        }
        count
    }
    pub fn reserve_rel(&mut self, range: std::ops::Range<u32>) {
        for index in range {
            self.set_rel(index, true)
        }
    }
    pub fn checksum(&self, superblock: &Superblock, size: usize) -> u32 {
        calculate_crc32c(superblock.checksum_seed().unwrap(), &self.bytes[..size])
    }
}
pub fn read_block_bitmap<'a, 'b, D: fal::Device>(baddr: u64, filesystem: &'b Filesystem<D>, block_bytes: &'a mut [u8]) -> Result<Option<Bitmap<'a, 'b>>, BgdError> {
    let group_index = baddr / u64::from(filesystem.superblock.blocks_per_group);

    let descriptor = load_block_group_descriptor(filesystem, group_index)?;

    if descriptor.flags().contains(BlockGroupDescriptorFlags::BLOCK_BITMAP_UNINIT) {
        return Ok(None)
    }
    descriptor.block_bitmap(filesystem, block_bytes, baddr)
}
pub fn block_exists<D: fal::Device>(baddr: u64, filesystem: &Filesystem<D>) -> Result<bool, BgdError> {
    let mut block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];
    Ok(read_block_bitmap(baddr, filesystem, &mut block_bytes)?.map(|bitmap: Bitmap| bitmap.get(baddr)).unwrap_or(false))
}
pub fn set_block_exists<D: fal::DeviceMut>(baddr: u64, exists: bool, filesystem: &Filesystem<D>) -> Result<(), BgdError> {
    let mut block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];
    let mut bitmap: Bitmap = read_block_bitmap(baddr, filesystem, &mut block_bytes)?.unwrap_or_else(|| unimplemented!("allocating new blocks"));
    bitmap.set(baddr, exists);
    write_block_raw(filesystem, baddr, &block_bytes)?;
    Ok(())
}

quick_error! {
    #[derive(Debug)]
    pub enum AllocateBlockError {
        DiskIoError(err: io::Error) {
            from()
            cause(err)
            description("disk i/o error")
            display("disk i/o error: {}", err)
        }
        ParseError(err: scroll::Error) {
            from()
            cause(err)
            description("parsing error")
            display("parsing error: {}", err)
        }
        BgdError(err: BgdError) {
            from()
            cause(err)
            description("block group descriptor table error")
            display("block group descriptor table error: {}", err)
        }
        InsufficientSpace(size: u32) {
            description("no block group containing the requested amount of blocks")
            display("no block group containing {} free contiguous blocks", size)
        }
        ChecksumMismatch {
            description("some of the block group descriptors had (possibly) corrupted data")
        }
    }
}
struct DescriptorHandle<'a> {
    desc_bytes: &'a mut [u8],
    desc: &'a mut BlockGroupDescriptor,
    rel_index: u32,
    abs_index: u32,
}
impl<'a> DescriptorHandle<'a> {
    fn write(&mut self) {
    }
}
fn iter_through_bgdescs<T, D: fal::DeviceMut, F: FnMut(DescriptorHandle) -> Result<Option<T>, AllocateBlockError>>(filesystem: &Filesystem<D>, mut function: F) -> Result<Option<T>, AllocateBlockError> {
    let block_group_count = filesystem.superblock.block_count() / u64::from(filesystem.superblock.blocks_per_group);
    let mut block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];

    let size = if filesystem.superblock.is_64bit() {
        BlockGroupDescriptor::SIZE_64BIT
    } else {
        BlockGroupDescriptor::SIZE
    };

    let bgdt_first_block = block_group_address(
        &filesystem.superblock,
        superblock::SUPERBLOCK_OFFSET + superblock::SUPERBLOCK_LEN - 1,
    ) + 1;
    let bgdt_block_count = block_group_count as u32 * filesystem.superblock.block_size() / size as u32;

    let descs_per_block = filesystem.superblock.block_size() / size as u32;

    'block_loop: for rel_baddr in 0..bgdt_block_count {
        read_block_to_raw(filesystem, bgdt_first_block + u64::from(rel_baddr), &mut block_bytes)?;
        for rel_index in 0..descs_per_block {
            let abs_index = rel_baddr * descs_per_block + rel_index;
            let rel_offset = rel_index * size as u32;

            let desc_bytes =
                &mut block_bytes[rel_offset as usize..rel_offset as usize + size as usize];

            let mut desc = BlockGroupDescriptor::parse(desc_bytes, &filesystem.superblock, abs_index)?;

            let handle = DescriptorHandle {
                desc_bytes,
                desc: &mut desc,
                rel_index,
                abs_index,
            };
            match function(handle)? {
                Some(t) => {
                    BlockGroupDescriptor::serialize(&desc, filesystem.superblock.checksum_seed().unwrap(), abs_index, desc_bytes)?;
                    write_block_raw(filesystem, bgdt_first_block + u64::from(rel_baddr), &block_bytes)?;
                    return Ok(Some(t));
                }
                None => continue,
            }
        }
    }

    Ok(None)
}

enum ThingToAllocate {
    Blocks,
    Inodes,
}

fn allocate_blocks_or_inodes<D: fal::DeviceMut>(filesystem: &Filesystem<D>, thing_to_allocate: ThingToAllocate, len: u32) -> Result<Range<u64>, AllocateBlockError> {
    let mut bitmap_block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];

    // Find the first block group with a free block count of at least len.
    match iter_through_bgdescs(filesystem, |handle| -> Result<Option<Range<u64>>, AllocateBlockError> {
        let unalloc_count = match thing_to_allocate {
            ThingToAllocate::Blocks => handle.desc.unalloc_block_count(),
            ThingToAllocate::Inodes => handle.desc.unalloc_inode_count(),
        };
        let word = match thing_to_allocate {
            ThingToAllocate::Blocks => "block",
            ThingToAllocate::Inodes => "inode",
        };

        if unalloc_count >= len {
            log::trace!("block group {bg} had {free} free {word}s", bg=handle.abs_index, free=handle.desc.unalloc_block_count(), word=word);

            // FIXME: Can bitmaps span multiple blocks?

            let mut bitmap = match {
                match thing_to_allocate {
                    ThingToAllocate::Blocks => handle.desc.block_bitmap(filesystem, &mut bitmap_block_bytes, 0)?,
                    ThingToAllocate::Inodes => handle.desc.inode_bitmap(filesystem, &mut bitmap_block_bytes, 0)?,
                }
            } {
                Some(bm) => bm,
                None => {
                    log::trace!("no {word} bitmap found", word=word);
                    return Ok(None);
                }
            };
            let current_checksum = match thing_to_allocate {
                ThingToAllocate::Blocks => handle.desc.block_bm_checksum(),
                ThingToAllocate::Inodes => handle.desc.inode_bm_checksum(),
            };
            let bitmap_size = match thing_to_allocate {
                ThingToAllocate::Blocks => filesystem.superblock.clusters_per_group(),
                ThingToAllocate::Inodes => filesystem.superblock.inodes_per_group(),
            } as usize / 8;
            if bitmap.checksum(&filesystem.superblock, bitmap_size) != current_checksum {
                // TODO: Should the allocation really fail in this case?
                return Err(AllocateBlockError::ChecksumMismatch);
            }

            let first_unused_rel_baddr = match bitmap.find_first_rel() {
                Some(f) => f,
                None => {
                    log::error!("no free {word} was found, even though the unused {word} count said the opposite.", word=word);
                    return Ok(None);
                }
            };

            // TODO
            let available_blocks_after = bitmap.free_blocks_after_start_rel(first_unused_rel_baddr, 0xFFFF_FFFF);

            if available_blocks_after >= len {

                bitmap.reserve_rel(first_unused_rel_baddr..first_unused_rel_baddr + len);

                let new_checksum = bitmap.checksum(&filesystem.superblock, bitmap_size);

                match thing_to_allocate {
                    ThingToAllocate::Blocks => {
                        handle.desc.set_block_bm_checksum(new_checksum);
                        filesystem.info.free_blocks.fetch_sub(u64::from(len), atomic::Ordering::Acquire);
                        handle.desc.set_unalloc_block_count(handle.desc.unalloc_block_count() - len)
                    }
                    ThingToAllocate::Inodes => {
                        handle.desc.set_inode_bm_checksum(new_checksum);
                        filesystem.info.free_inodes.fetch_sub(len, atomic::Ordering::Acquire);
                        handle.desc.set_unalloc_inode_count(handle.desc.unalloc_inode_count() - len)
                    }
                }

                let bm_baddr = match thing_to_allocate {
                    ThingToAllocate::Blocks => handle.desc.block_usage_bm_start_baddr(),
                    ThingToAllocate::Inodes => handle.desc.inode_usage_bm_start_baddr(),
                };

                write_block_raw(
                    filesystem,
                    bm_baddr,
                    &bitmap_block_bytes,
                )?;

                let desc_start_block = u64::from(handle.abs_index) * u64::from(filesystem.superblock.blocks_per_group);
                let abs_block_range = desc_start_block + u64::from(first_unused_rel_baddr)..u64::from(desc_start_block) + u64::from(first_unused_rel_baddr) + u64::from(len);

                log::trace!("Found a {word} region {rel_start}..{rel_end} ({abs_start}..{abs_end} absolute)", word=word, rel_start=first_unused_rel_baddr, rel_end=first_unused_rel_baddr + len, abs_start=abs_block_range.start, abs_end=abs_block_range.end);


                #[cfg(debug_assertions)]
                {
                    for block in abs_block_range.clone() {
                        if !block_exists(block, filesystem).unwrap() {
                            log::error!("{word} {addr} didn't exist, after it was allocated", word=word, addr=block);
                        }
                    }
                }

                return Ok(Some(abs_block_range));
            }
        }
        Ok(None)
    })? {
        Some(t) => Ok(t),
        None => Err(AllocateBlockError::InsufficientSpace(len)),
    }
}
pub fn allocate_blocks<D: fal::DeviceMut>(filesystem: &Filesystem<D>, len: u32) -> Result<Range<u64>, AllocateBlockError> {
    allocate_blocks_or_inodes(filesystem, ThingToAllocate::Blocks, len)
}
pub fn allocate_inodes<D: fal::DeviceMut>(filesystem: &Filesystem<D>, len: u32) -> Result<Range<u64>, AllocateBlockError> {
    allocate_blocks_or_inodes(filesystem, ThingToAllocate::Inodes, len)
}
