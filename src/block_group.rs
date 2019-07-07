#[derive(Debug)]
pub struct BlockGroupDescriptor {
    block_usage_bm_baddr: u32,
    inode_usage_bm_baddr: u32,
    inode_table_start_baddr: u32,
    unalloc_block_count: u16,
    unalloc_inode_count: u16,
    dir_count: u16,
}

impl BlockGroupDescriptor {
    pub const SIZE: u64 = 32;
}
