use std::{convert::TryFrom, mem, io::{self, prelude::*}};

use crate::{block_group, Filesystem, read_block, read_u16, read_u32};

pub const ROOT: u32 = 2;

#[derive(Debug)]
pub struct Inode {
    pub type_and_perm: u16,
    pub uid: u16,
    pub size_low: u32,
    pub last_access_time: u32,
    pub creation_time: u32,
    pub last_modification_time: u32,
    pub deletion_time: u32,
    pub gid: u16,
    pub hard_link_count: u16,
    pub disk_sector_count: u32,
    pub flags: u32,
    pub os_specific_1: u32,
    pub direct_ptrs: [u32; 12],
    pub singly_indirect_ptr: u32,
    pub doubly_indirect_ptr: u32,
    pub triply_indirect_ptr: u32,
    pub generation_number: u32,

    pub extended_atribute_block: u32,
    pub size_high_or_acl: u32,
    pub fragment_baddr: u32,
    pub os_specific_2: [u8; 12],
}
impl Inode {
    pub fn load<R: Read + Seek + Write>(filesystem: &mut Filesystem<R>, inode_address: u32) -> io::Result<Self> {
        let block_group_index = block_group::inode_block_group_index(&filesystem.superblock, inode_address);
        let block_group_descriptor = block_group::load_block_group_descriptor(filesystem, block_group_index)?;
        let inode_index_in_group = (inode_address - 1) % filesystem.superblock.inodes_per_group;
        let inode_size = filesystem.superblock.inode_size();

        let containing_block_index_in_group = block_group_descriptor.inode_table_start_baddr + u32::try_from(
            u64::from(inode_index_in_group * u32::from(inode_size)) / filesystem.superblock.block_size
        ).unwrap();
        let containing_block_index = containing_block_index_in_group + block_group_index * filesystem.superblock.blocks_per_group;

        let max_inodes_in_block = u32::try_from(filesystem.superblock.block_size / u64::from(inode_size)).unwrap();

        let inode_index_in_block = usize::try_from(inode_index_in_group % max_inodes_in_block).unwrap();
        let inode_size = usize::from(inode_size);

        let containing_block = read_block(filesystem, containing_block_index)?;
        let inode_bytes = &containing_block[inode_index_in_block * inode_size..inode_index_in_block * inode_size + inode_size];
        Ok(Self::parse(inode_bytes))
    }
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            type_and_perm: read_u16(bytes, 0),
            uid: read_u16(bytes, 2),
            size_low: read_u32(bytes, 4),
            last_access_time: read_u32(bytes, 8),
            creation_time: read_u32(bytes, 12),
            last_modification_time: read_u32(bytes, 16),
            deletion_time: read_u32(bytes, 20),
            gid: read_u16(bytes, 24),
            hard_link_count: read_u16(bytes, 26),
            disk_sector_count: read_u32(bytes, 28),
            flags: read_u32(bytes, 32),
            os_specific_1: read_u32(bytes, 36),
            direct_ptrs: {
                let mut direct_ptrs = [0u32; 12];
                for index in 0..12 {
                    let mut le_bytes = [0u8; mem::size_of::<u32>()];
                    le_bytes.copy_from_slice(&bytes[40 + index * 4 .. 40 + index * 4 + 4]);
                    let direct_ptr = u32::from_le_bytes(le_bytes);
                    direct_ptrs[index] = direct_ptr;
                }
                direct_ptrs
            },
            singly_indirect_ptr: read_u32(bytes, 88),
            doubly_indirect_ptr: read_u32(bytes, 92),
            triply_indirect_ptr: read_u32(bytes, 96),
            generation_number: read_u32(bytes, 100),
            extended_atribute_block: read_u32(bytes, 104),
            size_high_or_acl: read_u32(bytes, 108),
            fragment_baddr: read_u32(bytes, 112),
            os_specific_2: {
                let mut os_specific_bytes = [0u8; 12];
                os_specific_bytes.copy_from_slice(&bytes[116..128]);
                os_specific_bytes
            },
        }
    }
}
