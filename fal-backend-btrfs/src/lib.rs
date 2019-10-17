pub mod filesystem;
pub mod items;
pub mod superblock;
pub mod tree;

use bitflags::bitflags;
use crc::{crc32, Hasher32};
use enum_primitive::*;

use fal::{read_u8, read_u32, read_u64};

mod sizes {
    pub const K: u64 = 1024;
    pub const M: u64 = K * K;
    pub const G: u64 = M * K;
    pub const T: u64 = G * K;
    pub const P: u64 = T * K;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DiskKey {
    pub oid: u64,
    pub ty: DiskKeyType,
    pub offset: u64,
}

impl DiskKey {
    const LEN: usize = 17;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            oid: read_u64(bytes, 0),
            ty: DiskKeyType::from_u8(read_u8(bytes, 8)).unwrap(),
            offset: read_u64(bytes, 9),
        }
    }
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    pub enum DiskKeyType {
        InodeItem = 1,
        InodeRef = 12,
        InodeExtref = 13,
        XattrItem = 24,
        OrphanItem = 48,
        DirLogItem = 60,
        DirLogIndex = 72,
        DirItem = 84,
        DirIndex = 96,
        ExtentData = 108,
        ExtentCsum = 128,
        RootItem = 132,
        RootBackref = 144,
        RootRef = 156,
        ExtentItem = 168,
        MetadataItem = 169,
        TreeBlockRef = 176,
        ExtentDataRef = 178,
        ExtentRefV0 = 180,
        SharedBlockRef = 182,
        SharedDataRef = 184,
        BlockGroupItem = 192,
        FreeSpaceInfo = 198,
        FreeSpaceExtent = 199,
        FreeSpaceBitmap = 200,
        DevExtent = 204,
        DevItem = 216,
        ChunkItem = 228,
        QgroupStatus = 240,
        QgroupInfo = 242,
        QgroupLimit = 244,
        QgroupRelation = 246,
        TemporaryItem = 248,
        PersistentItem = 249,
        DevReplace = 250,
    }
}


bitflags! {
    pub struct HeaderFlags: u64 {
        const WRITTEN = 1 << 0;
        const RELOC = 1 << 1;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Checksum {
    Crc32(u32),
}

impl Checksum {
    pub fn new(ty: superblock::ChecksumType, bytes: &[u8]) -> Self {
        match ty {
            superblock::ChecksumType::Crc32 => Self::Crc32(read_u32(bytes, 0)),
        }
    }
    pub fn calculate(ty: superblock::ChecksumType, bytes: &[u8]) -> Self {
        match ty {
            superblock::ChecksumType::Crc32 => {
                let mut hasher = crc32::Digest::new_with_initial(crc32::CASTAGNOLI, 0);
                hasher.write(bytes);
                Checksum::Crc32(hasher.sum32())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Timespec {
    pub sec: i64,
    pub nsec: u32,
}

impl Timespec {
    pub const LEN: usize = 12;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            sec: read_u64(bytes, 0) as i64,
            nsec: read_u32(bytes, 8),
        }
    }
}

pub fn read_timespec(bytes: &[u8], offset: usize) -> Timespec {
    Timespec::parse(&bytes[offset..offset + Timespec::LEN])
}
