use crate::{Checksum, superblock::ChecksumType};

use fal::{read_u8, read_u32, read_u64, read_uuid};

use uuid::Uuid;

#[derive(Debug)]
pub struct Header {
    pub checksum: Checksum,
    pub fsid: Uuid,

    pub logical_addr: u64,
    pub flags: u64,

    pub chunk_tree_uuid: Uuid,
    pub generation: u64,
    pub owner: u64,
    pub item_count: u32,
    pub level: u8,
}

impl Header {
    pub const LEN: usize = 101;

    pub fn parse(checksum_type: ChecksumType, bytes: &[u8]) -> Self {
        let checksum_bytes = &bytes[..32];
        let checksum = Checksum::new(checksum_type, checksum_bytes);

        //assert_eq!(checksum, Checksum::calculate(checksum_type, &bytes[32..]));

        let fsid = read_uuid(bytes, 32);
        
        let logical_addr = read_u64(bytes, 48);
        let flags = read_u64(bytes, 56);

        let chunk_tree_uuid = read_uuid(bytes, 64);
        let generation = read_u64(bytes, 80);
        let owner = read_u64(bytes, 88);
        let item_count = read_u32(bytes, 96);
        let level = read_u8(bytes, 100);

        Header {
            checksum,
            fsid,

            logical_addr,
            flags,

            chunk_tree_uuid,
            generation,
            owner,
            item_count,
            level,
        }
    }
}
