pub mod filesystem;
pub mod superblock;
pub mod tree;

use bitflags::bitflags;
use crc::{crc32, Hasher32};
use enum_primitive::*;
use fal::{read_u8, read_u16, read_u32, read_u64, read_uuid};

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
    /// The type of a block group or chunk.
    pub struct BlockGroupType: u64 {
        const DATA = 1 << 0;
        const SYSTEM = 1 << 1;
        const METADATA = 1 << 2;
        const RAID0 = 1 << 3;
        const RAID1 = 1 << 4;
        const DUP = 1 << 5;
        const RAID10 = 1 << 6;
        const RAID5 = 1 << 7;
        const RAID6 = 1 << 8;
        const RESERVED = 1 << 48 | 1 << 49;
    }
}

#[derive(Debug)]
pub struct DiskChunk {
    pub len: u64,
    pub owner: u64,

    pub stripe_length: u64,
    pub ty: BlockGroupType,

    pub io_alignment: u32,
    pub io_width: u32,

    pub sector_size: u32,
    pub stripe_count: u16,
    pub sub_stripe_count: u16,
    pub stripe: Stripe,
}

impl DiskChunk {
    const LEN: usize = 80;
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            len: read_u64(bytes, 0),
            owner: read_u64(bytes, 8),

            stripe_length: read_u64(bytes, 16),
            ty: BlockGroupType::from_bits(read_u64(bytes, 24)).unwrap(),

            io_alignment: read_u32(bytes, 32),
            io_width: read_u32(bytes, 36),

            sector_size: read_u32(bytes, 40),
            stripe_count: read_u16(bytes, 44),
            sub_stripe_count: read_u16(bytes, 46),
            stripe: Stripe::parse(&bytes[48..]),
        }
    }
}

#[derive(Debug)]
pub struct Stripe {
    pub device_id: u64,
    pub offset: u64,
    pub device_uuid: uuid::Uuid,
}

impl Stripe {
    const LEN: usize = 32;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            device_id: read_u64(bytes, 0),
            offset: read_u64(bytes, 8),
            device_uuid: read_uuid(bytes, 16),
        }
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
