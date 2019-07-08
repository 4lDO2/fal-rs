use crate::{read_block, read_u16, read_u32, superblock};
use std::{
    convert::TryFrom,
    io::{self, prelude::*},
};

#[derive(Debug)]
pub struct BlockGroupDescriptor {
    pub block_usage_bm_baddr: u32,
    pub inode_usage_bm_baddr: u32,
    pub inode_table_start_baddr: u32,
    pub unalloc_block_count: u16,
    pub unalloc_inode_count: u16,
    pub dir_count: u16,
}

impl BlockGroupDescriptor {
    pub const SIZE: u64 = 32;
}

pub fn block_address(superblock: &superblock::Superblock, offset: u64) -> u32 {
    (offset / superblock.block_size) as u32
}
pub fn block_offset(superblock: &superblock::Superblock, offset: u64) -> u32 {
    (offset % superblock.block_size) as u32
}
pub fn load_block_group_descriptor<D: Read + Seek + Write>(
    filesystem: &mut crate::Filesystem<D>,
    index: u32,
) -> io::Result<BlockGroupDescriptor> {
    let bgdt_first_block = block_address(
        &filesystem.superblock,
        superblock::SUPERBLOCK_OFFSET + superblock::SUPERBLOCK_LEN - 1,
    ) + 1;
    let bgdt_offset = u64::from(bgdt_first_block) * u64::from(filesystem.superblock.block_size);
    let absolute_offset = u64::from(bgdt_offset) + u64::from(index) * BlockGroupDescriptor::SIZE;
    let block_bytes = read_block(
        filesystem,
        block_address(&filesystem.superblock, absolute_offset),
    )?;
    let rel_offset = block_offset(&filesystem.superblock, absolute_offset);
    let descriptor_bytes = &block_bytes[rel_offset as usize
        ..rel_offset as usize + usize::try_from(BlockGroupDescriptor::SIZE).unwrap()];

    Ok(BlockGroupDescriptor {
        block_usage_bm_baddr: read_u32(&descriptor_bytes, 0),
        inode_usage_bm_baddr: read_u32(&descriptor_bytes, 4),
        inode_table_start_baddr: read_u32(&descriptor_bytes, 8),
        unalloc_block_count: read_u16(&descriptor_bytes, 12),
        unalloc_inode_count: read_u16(&descriptor_bytes, 14),
        dir_count: read_u16(&descriptor_bytes, 16),
    })
}
pub fn inode_block_group_index(superblock: &superblock::Superblock, inode: u32) -> u32 {
    (inode - 1) / superblock.inodes_per_group
}
