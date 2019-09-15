use crate::{read_block, read_block_to_raw, read_u16, read_u32, superblock, Filesystem};
use std::{convert::TryFrom, io};

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
pub fn load_block_group_descriptor<D: fs_core::Device>(
    filesystem: &Filesystem<D>,
    index: u32,
) -> io::Result<BlockGroupDescriptor> {
    let bgdt_first_block = block_address(
        &filesystem.superblock,
        superblock::SUPERBLOCK_OFFSET + superblock::SUPERBLOCK_LEN - 1,
    ) + 1;
    let bgdt_offset = u64::from(bgdt_first_block) * u64::from(filesystem.superblock.block_size);
    let absolute_offset = u64::from(bgdt_offset) + u64::from(index) * BlockGroupDescriptor::SIZE;

    let mut block_bytes = vec![0; usize::try_from(filesystem.superblock.block_size).unwrap()];

    read_block_to_raw(
        filesystem,
        block_address(&filesystem.superblock, absolute_offset),
        &mut block_bytes,
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
pub fn inode_index_inside_group(superblock: &superblock::Superblock, inode: u32) -> u32 {
    (inode - 1) % superblock.inodes_per_group
}
pub fn inode_exists<D: fs_core::Device>(
    inode: u32,
    filesystem: &Filesystem<D>,
) -> io::Result<bool> {
    if inode == 0 {
        return Ok(false);
    }

    let group_index = inode_block_group_index(&filesystem.superblock, inode);
    let index_inside_group = inode_index_inside_group(&filesystem.superblock, inode);

    let descriptor = load_block_group_descriptor(filesystem, group_index)?;

    let bm_start_baddr = descriptor.inode_usage_bm_baddr;
    let bm_block_index =
        u32::try_from(u64::from(index_inside_group / 8) / filesystem.superblock.block_size)
            .unwrap();

    let block_bytes = read_block(filesystem, bm_start_baddr + bm_block_index)?;

    let byte_index_inside_bm =
        u32::try_from(u64::from(index_inside_group) / filesystem.superblock.block_size).unwrap();

    let bm_byte = block_bytes[usize::try_from(byte_index_inside_bm).unwrap()];
    let bm_bit = 1 << ((inode - 1) % 8);

    Ok(bm_byte & bm_bit != 0)
}
pub fn free_inode<D: fs_core::DeviceMut>(
    _inode: u32,
    _filesystem: &mut Filesystem<D>,
) -> io::Result<()> {
    unimplemented!()
}
pub fn block_exists<D: fs_core::Device>(
    baddr: u32,
    filesystem: &Filesystem<D>,
) -> io::Result<bool> {
    let group_index = baddr / filesystem.superblock.blocks_per_group;
    let index_inside_group = baddr % filesystem.superblock.blocks_per_group;

    let descriptor = load_block_group_descriptor(filesystem, group_index)?;

    let bm_start_baddr = descriptor.block_usage_bm_baddr;
    let bm_block_index =
        u32::try_from(u64::from(index_inside_group / 8) / filesystem.superblock.block_size)
            .unwrap();

    let mut block_bytes = vec![0; usize::try_from(filesystem.superblock.block_size).unwrap()];
    read_block_to_raw(
        filesystem,
        bm_start_baddr + bm_block_index,
        &mut block_bytes,
    )?;

    let byte_index_inside_bm =
        u32::try_from(u64::from(index_inside_group) / filesystem.superblock.block_size).unwrap();

    let bm_byte = block_bytes[usize::try_from(byte_index_inside_bm).unwrap()];
    let bm_bit = 1 << (baddr % 8);
    Ok(bm_byte & bm_bit != 0)
}
