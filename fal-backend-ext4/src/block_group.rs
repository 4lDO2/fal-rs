use crate::{calculate_crc32c, read_block, read_block_to_raw, write_block_raw, superblock, superblock::Superblock, Filesystem};
use std::{convert::{TryFrom, TryInto}, io};

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
}

pub fn block_address(superblock: &superblock::Superblock, offset: u64) -> u64 {
    offset / u64::from(superblock.block_size())
}
pub fn block_offset(superblock: &superblock::Superblock, offset: u64) -> u64 {
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

    let bgdt_first_block = block_address(
        &filesystem.superblock,
        superblock::SUPERBLOCK_OFFSET + superblock::SUPERBLOCK_LEN - 1,
    ) + 1;
    let bgdt_offset = bgdt_first_block * u64::from(filesystem.superblock.block_size());
    let absolute_offset = bgdt_offset + index * size;

    read_block_to_raw(
        filesystem,
        block_address(&filesystem.superblock, absolute_offset),
        block_bytes,
    )?;
    let rel_offset = block_offset(&filesystem.superblock, absolute_offset);
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
        let byte_index_inside_bm = index_inside_group / self.superblock.block_size();
        self.bytes[byte_index_inside_bm as usize] = (value as u8) << (index_inside_group % 8)
    }
    pub fn get_rel(&self, index_inside_group: u32) -> bool {
        let byte_index_inside_bm = index_inside_group / self.superblock.block_size();
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
}
pub fn read_block_bitmap<'a, 'b, D: fal::Device>(baddr: u64, filesystem: &'b Filesystem<D>, block_bytes: &'a mut [u8]) -> Result<Option<Bitmap<'a, 'b>>, BgdError> {
    let group_index = baddr / u64::from(filesystem.superblock.blocks_per_group);
    let index_inside_group = baddr % u64::from(filesystem.superblock.blocks_per_group);

    let descriptor = load_block_group_descriptor(filesystem, group_index)?;

    if descriptor.flags().contains(BlockGroupDescriptorFlags::BLOCK_BITMAP_UNINIT) {
        return Ok(None)
    }

    let bm_start_baddr = descriptor.block_usage_bm_start_baddr();
    let bm_block_index = (index_inside_group / 8) / u64::from(filesystem.superblock.block_size());

    read_block_to_raw(
        filesystem,
        bm_start_baddr + bm_block_index,
        block_bytes,
    )?;
    Ok(Some(Bitmap { bytes: block_bytes, superblock: &filesystem.superblock }))
}
fn block_exists_inner<'a, D: fal::Device>(baddr: u64, filesystem: &Filesystem<D>, block_bytes: &'a mut [u8]) -> Result<Option<&'a mut u8>, BgdError> {
    let bitmap = read_block_bitmap(baddr, filesystem, block_bytes)?;
    let index_inside_group = baddr % u64::from(filesystem.superblock.blocks_per_group);

    let byte_index_inside_bm =
        u32::try_from(index_inside_group / u64::from(filesystem.superblock.block_size())).unwrap();

    let bm_byte = &mut block_bytes[usize::try_from(byte_index_inside_bm).unwrap()];
    Ok(Some(bm_byte))
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
