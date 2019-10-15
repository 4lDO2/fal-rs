use crate::{Checksum, DevItem, DiskChunk, DiskKey, DiskKeyType, superblock::ChecksumType};

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

        assert_eq!(checksum, Checksum::calculate(checksum_type, &bytes[32..]));

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

#[derive(Debug)]
pub struct Node {
    pub header: Header,
    pub key_ptrs: Vec<KeyPtr>,
}

#[derive(Debug)]
pub struct KeyPtr {
    pub key: DiskKey,
    pub block_ptr: u64,
    pub generation: u64,
}

impl KeyPtr {
    pub const LEN: usize = 33;
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            key: DiskKey::parse(&bytes[..17]),
            block_ptr: read_u64(bytes, 17),
            generation: read_u64(bytes, 25),
        }
    }
}

impl Node {
    pub fn parse(header: Header, bytes: &[u8]) -> Self {
        let key_ptrs = (0..header.item_count as usize).map(|i| KeyPtr::parse(&bytes[i * 33 .. (i + 1) * 33])).collect();

        Self {
            header,
            key_ptrs,
        }
    }
}

#[derive(Debug)]
pub struct Leaf {
    pub header: Header,
    pub items: Vec<Item>,
    pub values: Vec<Value>,
}

#[derive(Debug)]
pub struct Item {
    pub key: DiskKey,
    pub offset: u32,
    pub size: u32,
}

#[derive(Debug)]
pub enum Value {
    Chunk(DiskChunk),
    Device(DevItem),
}

impl Item {
    pub const LEN: usize = 25;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            key: DiskKey::parse(&bytes[..17]),
            offset: read_u32(bytes, 17),
            size: read_u32(bytes, 21),
        }
    }
}

impl Leaf {
    pub fn parse(header: Header, bytes: &[u8]) -> Self {
        let items = (0..header.item_count as usize).map(|i| Item::parse(&bytes[i * 25 .. (i + 1) * 25])).collect::<Vec<_>>();

        let values = items.iter().map(|item: &Item| {
            let value_bytes = &bytes[item.offset as usize .. item.offset as usize + item.size as usize];
            match item.key.ty {
                DiskKeyType::ChunkItem => Value::Chunk(DiskChunk::parse(value_bytes)),
                DiskKeyType::DevItem => Value::Device(DevItem::parse(value_bytes)),
                other => unimplemented!("{:?}", other),
            }
        }).collect();

        Self {
            header,
            items,
            values,
        }
    }
}

#[derive(Debug)]
pub enum Tree {
    Internal(Node),
    Leaf(Leaf),
}

impl Tree {
    pub fn parse(checksum_type: ChecksumType, bytes: &[u8]) -> Self {
        let header = Header::parse(checksum_type, &bytes);

        match header.level {
            0 => Self::Leaf(Leaf::parse(header, &bytes[101..])),
            _ => Self::Internal(Node::parse(header, &bytes[101..])),
        }
    }
}
