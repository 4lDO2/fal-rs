use scroll::{Pread, Pwrite};

#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct HtreeRoot {
    pub dot_inode: u32,
    pub dot_rec_len: u16, // This has to be 12.
    pub dot_name_len: u8, // This has to be 1.
    pub dot_file_ty: u8,
    pub dot_name: [u8; 4], // This has to be ".\0\0\0".

    pub dotdot_inode: u32,
    pub dotdot_rec_len: u16, // This has to be block_size - 12.
    pub dotdot_name_len: u8, // This has to be 2.
    pub dotdot_file_ty: u8,
    pub dotdot_name: [u8; 4], // This has to be "..\0\0".

    pub reserved_zero: u32,
    pub hash_version: u8,
    pub info_len: u8,
    pub indirect_levels: u8,
    pub unused_flags: u8,
    pub limit: u16,
    pub count: u16,
    pub block: u32,
    // the actual entries come after
}

#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct HtreeNode {
    pub fake_inode: u32,
    pub fake_rec_len: u16,
    pub name_len: u8,
    pub file_ty: u8,
    pub max_entry_count: u16,
    pub entry_count: u16,
    pub block: u32,
    // the actual entries come after
}

#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct HtreeEntry {
    pub hash: u32,
    pub block: u32,
}

#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct HtreeTail {
    pub zero: u32,
    pub checksum: u32,
}

#[derive(Debug)]
pub struct HtreeRootBlock {
    pub header: HtreeRoot,
    pub entries: Vec<HtreeEntry>,
}

impl HtreeRootBlock {
    pub fn parse(bytes: &[u8]) -> Result<Self, scroll::Error> {
        let mut offset = 0;
        let header: HtreeRoot = bytes.gread_with(&mut offset, scroll::LE)?;

        // TODO: Validate count and limit.

        let entries = (1..header.count)
            .map(|_| bytes.gread_with(&mut offset, scroll::LE))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { header, entries })
    }
}
