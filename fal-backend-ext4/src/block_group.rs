use crate::{calculate_crc32c, read_block, read_block_to_raw, write_block_raw, superblock, superblock::Superblock, Filesystem};
use std::{convert::{TryFrom, TryInto}, io, ops::Range};

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
    pub fn block_bm_checksum(&self) -> u32 {
        u32::from(self.base.block_bm_csum_lo)
            | self.ext.as_ref().map(|ext| (u32::from(ext.block_bm_csum_hi) << 16)).unwrap_or(0)
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
    pub fn parse(bytes: &[u8], superblock: &Superblock) -> Result<Self, scroll::Error> {
        let this = Self {
            base: bytes.pread_with(0, scroll::LE)?,
            ext: if superblock.is_64bit() {
                Some(bytes.pread_with(Self::SIZE as usize, scroll::LE)?)
            } else {
                None
            },
        };

        if BlockGroupDescriptorFlags::from_bits(this.base.flags).is_none() {
            return Err(scroll::Error::BadInput { size: 2, msg: "invalid block group flags bitmask" });
        }

        Ok(this)
    }
    pub fn serialize(this: &Self, bytes: &mut [u8]) -> Result<(), scroll::Error> {
        bytes.pwrite_with(&this.base, 0, scroll::LE)?;

        if let Some(ref ext) = this.ext {
            bytes.pwrite_with(ext, Self::SIZE as usize, scroll::LE)?;
        }
        Ok(())
    }
    pub fn flags(&self) -> BlockGroupDescriptorFlags {
        BlockGroupDescriptorFlags::from_bits(self.base.flags).unwrap()
    }
    pub fn bitmap<'a, 'b, D: fal::Device>(&self, filesystem: &'b Filesystem<D>, block_bytes: &'a mut [u8], baddr: u64) -> Result<Option<Bitmap<'a, 'b>>, BgdError> {
        let bm_start_baddr = self.block_usage_bm_start_baddr();
        let bm_block_index = (block_group_offset(&filesystem.superblock, baddr) / 8) / u64::from(filesystem.superblock.block_size());

        read_block_to_raw(
            filesystem,
            bm_start_baddr + bm_block_index,
            block_bytes,
        )?;
        Ok(Some(Bitmap { bytes: block_bytes, superblock: &filesystem.superblock }))
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
    BlockGroupDescriptor::serialize(bgdesc, descriptor_bytes)?;

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
        let byte_index_inside_bm = (index_inside_group / 8) / self.superblock.block_size();
        self.bytes[byte_index_inside_bm as usize] &= !(1 << (index_inside_group % 8));
        self.bytes[byte_index_inside_bm as usize] |= (value as u8) << (index_inside_group % 8);
    }
    pub fn get_rel(&self, index_inside_group: u32) -> bool {
        let byte_index_inside_bm = (index_inside_group / 8) / self.superblock.block_size();
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
                        return Some(idx as u32 + i as u32)
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
}
pub fn read_block_bitmap<'a, 'b, D: fal::Device>(baddr: u64, filesystem: &'b Filesystem<D>, block_bytes: &'a mut [u8]) -> Result<Option<Bitmap<'a, 'b>>, BgdError> {
    let group_index = baddr / u64::from(filesystem.superblock.blocks_per_group);
    let index_inside_group = baddr % u64::from(filesystem.superblock.blocks_per_group);

    let descriptor = load_block_group_descriptor(filesystem, group_index)?;

    if descriptor.flags().contains(BlockGroupDescriptorFlags::BLOCK_BITMAP_UNINIT) {
        return Ok(None)
    }
    descriptor.bitmap(filesystem, block_bytes, baddr)
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
    }
}
pub fn allocate_blocks<D: fal::DeviceMut>(filesystem: &Filesystem<D>, len: u32) -> Result<Range<u64>, AllocateBlockError> {
    // Find the first block group with a free block count of at least len.
    let block_group_count = filesystem.superblock.block_count() / u64::from(filesystem.superblock.blocks_per_group);
    let mut block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];
    let mut bitmap_block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];

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
            let rel_offset = rel_index * size as u32;
            let descriptor_bytes =
                &mut block_bytes[rel_offset as usize..rel_offset as usize + size as usize];
            let descriptor = BlockGroupDescriptor::parse(descriptor_bytes, &filesystem.superblock)?;
            if descriptor.unalloc_block_count() >= len {
                log::trace!("block group {} had {} free blocks", rel_baddr * descs_per_block + rel_index, descriptor.unalloc_block_count());

                // FIXME: Can bitmaps span multiple blocks?

                let mut bitmap = match descriptor.bitmap(filesystem, &mut bitmap_block_bytes, 0)? {
                    Some(bm) => bm,
                    None => {
                        log::trace!("no bitmap found");
                        continue
                    }
                };
                let first_unused_rel_baddr = match bitmap.find_first_rel() {
                    Some(f) => f,
                    None => {
                        log::error!("no free block was found, even though the unused block count said the opposite.");
                        continue
                    }
                };
                // TODO
                let available_blocks_after = bitmap.free_blocks_after_start_rel(first_unused_rel_baddr, 0xFFFF_FFFF);

                if available_blocks_after >= len {

                    bitmap.reserve_rel(first_unused_rel_baddr..first_unused_rel_baddr + len);

                    let bm_start_baddr = descriptor.block_usage_bm_start_baddr();
                    // FIXME: the zero
                    let bm_block_index = (block_group_offset(&filesystem.superblock, 0) / 8) / u64::from(filesystem.superblock.block_size());
                    dbg!(bm_block_index);

                    write_block_raw(
                        filesystem,
                        bm_start_baddr + bm_block_index,
                        &bitmap_block_bytes,
                    )?;

                    let desc_index = rel_baddr * descs_per_block + rel_index;
                    let desc_start_block = u64::from(desc_index) * u64::from(filesystem.superblock.blocks_per_group);
                    let abs_block_range = desc_start_block + u64::from(first_unused_rel_baddr)..u64::from(desc_start_block) + u64::from(first_unused_rel_baddr) + u64::from(len);

                    log::trace!("Found a region {}..{} ({}..{} absolute)", first_unused_rel_baddr, first_unused_rel_baddr + len, abs_block_range.start, abs_block_range.end);

                    #[cfg(debug_assertions)]
                    {
                        for block in abs_block_range.clone() {
                            if !block_exists(block, filesystem).unwrap() {
                                log::error!("{} didn't exist", block);
                            }
                        }
                    }

                    return Ok(abs_block_range);
                }
            }
        }
    }

    Err(AllocateBlockError::InsufficientSpace(len))
}
