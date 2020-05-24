pub mod chunk_map;
pub mod filesystem;
pub mod items;
pub mod superblock;
pub mod tree;

use std::cmp::Ordering;

use bitflags::bitflags;
use crc::{crc32, Hasher32};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive as _;
use zerocopy::{AsBytes, FromBytes, Unaligned};

use fal::{read_u32, read_u64, read_u8};

#[allow(non_camel_case_types)]
type u64_le = zerocopy::byteorder::U64<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
type u32_le = zerocopy::byteorder::U32<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
type u16_le = zerocopy::byteorder::U16<byteorder::LittleEndian>;

#[allow(non_camel_case_types)]
type i64_le = zerocopy::byteorder::I64<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
type i32_le = zerocopy::byteorder::I32<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
type i16_le = zerocopy::byteorder::I16<byteorder::LittleEndian>;

mod sizes {
    pub const K: u64 = 1024;
    pub const M: u64 = K * K;
    pub const G: u64 = M * K;
    pub const T: u64 = G * K;
    pub const P: u64 = T * K;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct DiskKey {
    pub oid: u64_le,
    pub ty: u8,
    pub offset: u64_le,
}

impl DiskKey {
    pub fn ty(&self) -> Option<DiskKeyType> {
        DiskKeyType::from_u8(self.ty)
    }
    fn compare(&self, other: &Self) -> Ordering {
        self.compare_without_offset(other)
            .then(self.offset.get().cmp(&other.offset.get()))
    }
    pub fn compare_without_offset(&self, other: &Self) -> Ordering {
        self.oid.get()
            .cmp(&other.oid.get())
            .then((&{self.ty}).cmp(&{other.ty}))
    }
}

impl PartialOrd for DiskKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}
impl Ord for DiskKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, FromPrimitive)]
pub enum DiskKeyType {
    Unknown = 0,
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
    UuidSubvol = 251,
    UuidReceivedSubvol = 252,
    StringItem = 253,
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct Timespec {
    pub sec: i64_le,
    pub nsec: u32_le,
}

pub mod oid {
    pub const ROOT_TREE: u64 = 1;
    pub const EXTENT_TREE: u64 = 2;
    pub const CHUNK_TREE: u64 = 3;
    pub const DEV_TREE: u64 = 4;
    pub const FS_TREE: u64 = 5;
    pub const CSUM_TREE: u64 = 7;
    pub const QUOTA_TREE: u64 = 8;
    pub const UUID_TREE: u64 = 9;
    pub const FREE_SPACE_TREE: u64 = 10;

    pub const ROOT_TREE_DIR: u64 = 6;
    pub const DEV_STATS: u64 = 0;
}
