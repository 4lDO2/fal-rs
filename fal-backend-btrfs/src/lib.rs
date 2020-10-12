#![feature(iter_map_while)]

extern crate alloc;

pub mod chunk_map;
pub mod filesystem;
pub mod items;
pub mod superblock;
pub mod tree;

pub use filesystem::Filesystem;

use core::cmp::Ordering;
use core::convert::TryFrom;
use core::fmt;

use bitflags::bitflags;
use crc::{crc32, Hasher32};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive as _;
use thiserror::Error;
use zerocopy::{AsBytes, FromBytes, Unaligned};

use crate::superblock::ChecksumType;

#[allow(non_camel_case_types)]
pub type u64_le = zerocopy::byteorder::U64<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
pub type u32_le = zerocopy::byteorder::U32<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
pub type u16_le = zerocopy::byteorder::U16<byteorder::LittleEndian>;

#[allow(non_camel_case_types)]
pub type i64_le = zerocopy::byteorder::I64<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
pub type i32_le = zerocopy::byteorder::I32<byteorder::LittleEndian>;
#[allow(non_camel_case_types)]
pub type i16_le = zerocopy::byteorder::I16<byteorder::LittleEndian>;

mod sizes {
    pub const K: u64 = 1024;
    pub const M: u64 = K * K;
    pub const G: u64 = M * K;
    pub const T: u64 = G * K;
    pub const P: u64 = T * K;
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
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
        self.oid
            .get()
            .cmp(&other.oid.get())
            .then((&{ self.ty }).cmp(&{ other.ty }))
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
impl fmt::Debug for DiskKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiskKey")
            .field("oid", &self.oid.get())
            .field("ty", &DiskKeyType::from_u8(self.ty))
            .field("offset", &self.offset.get())
            .finish()
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct PackedUuid {
    pub bytes: [u8; 16],
}
impl fmt::Debug for PackedUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", uuid::Uuid::from_bytes(self.bytes))
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
    Crc32c(u32),
    Xxhash(u64),
    Sha256([u8; 32]),
    Blake2([u8; 32]),
}

#[derive(Debug, Error)]
pub enum InvalidChecksum {
    #[error("checksum mismatch")]
    Mismatch,

    #[error("unsupported (disabled at compile-time) checksum: {0:?}. supported checksums: {:?}", Checksum::supported_checksums().collect::<Vec<_>>())]
    UnsupportedChecksum(ChecksumType),
}

pub struct UnsupportedChecksum {
    #[cfg(all(feature = "nightly", feature = "crc32c", feature = "xxhash", feature = "sha256", feature = "blake2"))]
    _unconstructible: !,
    #[cfg(all(not(feature = "nightly"), feature = "crc32c", feature = "xxhash", feature = "sha256", feature = "blake2"))]
    _unconstructible: core::convert::Infallible,

    _ignored: (),
}

impl Checksum {
    pub const CRC32C_SEED: u32 = 0;
    pub const XXHASH_SEED: u64 = 0;

    pub fn parse(ty: ChecksumType, bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 32 {
            return None;
        }

        Some(match ty {
            ChecksumType::Crc32c => {
                Self::Crc32c(u32::from_le_bytes(<[u8; 4]>::try_from(&bytes[..4]).ok()?))
            }
            ChecksumType::Xxhash => {
                Self::Xxhash(u64::from_le_bytes(<[u8; 8]>::try_from(&bytes[..8]).ok()?))
            }
            ChecksumType::Sha256 => Self::Sha256(<[u8; 32]>::try_from(&bytes[..32]).ok()?),
            ChecksumType::Blake2 => Self::Sha256(<[u8; 32]>::try_from(&bytes[..32]).ok()?),
        })
    }
    pub fn serialize(&self, bytes: &mut [u8]) -> Option<()> {
        if bytes.len() < 32 {
            return None;
        }

        match self {
            &Checksum::Crc32c(hash) => {
                bytes[..4].copy_from_slice(&u32::to_le_bytes(hash));
                for byte in &mut bytes[4..] {
                    *byte = 0;
                }
            }
            &Checksum::Xxhash(hash) => {
                bytes[..8].copy_from_slice(&u64::to_le_bytes(hash));
                for byte in &mut bytes[8..] {
                    *byte = 0;
                }
            }
            &Checksum::Sha256(ref hash) => bytes[..32].copy_from_slice(hash),
            &Checksum::Blake2(ref hash) => bytes[..32].copy_from_slice(hash),
        }

        Some(())
    }
    pub fn calculate(ty: superblock::ChecksumType, bytes: &[u8]) -> Result<Self, UnsupportedChecksum> {
        match ty {
            #[cfg(feature = "crc32c")]
            ChecksumType::Crc32c => Ok({
                let mut hasher =
                    crc32::Digest::new_with_initial(crc32::CASTAGNOLI, Self::CRC32C_SEED);
                hasher.write(bytes);
                Checksum::Crc32c(hasher.sum32())
            }),
            #[cfg(feature = "xxhash")]
            ChecksumType::Xxhash => Ok({
                use core::hash::Hasher;

                let mut hasher = twox_hash::XxHash64::with_seed(Self::XXHASH_SEED);
                hasher.write(bytes);
                Checksum::Xxhash(hasher.finish())
            }),
            #[cfg(feature = "sha256")]
            ChecksumType::Sha256 => Ok({
                use sha2::digest::Digest;

                let mut hasher = sha2::Sha256::default();
                hasher.input(bytes);
                Checksum::Sha256(hasher.result().into())
            }),
            #[cfg(feature = "blake2")]
            ChecksumType::Blake2 => Ok({
                use blake2::digest::{Input, VariableOutput};

                let mut hasher = blake2::VarBlake2b::new(32).unwrap();
                hasher.input(bytes);

                let mut ret: Option<[u8; 32]> = None;
                hasher.variable_result(|slice| ret = <[u8; 32]>::try_from(slice).ok());
                Checksum::Blake2(ret.unwrap())
            }),

            #[allow(unreachable_patterns)]
            // NOTE: Since all patterns are checked for, we can omit this error type altogether
            // when all checksum algorithms are included, and use the unconstructible never type.
            _ => Err(UnsupportedChecksum { _ignored: (), }),
        }
    }
    pub fn ty(&self) -> ChecksumType {
        match self {
            &Self::Crc32c(_) => ChecksumType::Crc32c,
            &Self::Xxhash(_) => ChecksumType::Xxhash,
            &Self::Sha256(_) => ChecksumType::Sha256,
            &Self::Blake2(_) => ChecksumType::Blake2,
        }
    }
    pub fn size(&self) -> usize {
        self.ty().size()
    }
    pub fn bytes(self) -> [u8; 32] {
        let mut ret = [0u8; 32];
        self.serialize(&mut ret);
        ret
    }

    const SUPPORTED_CHECKSUMS: [Option<ChecksumType>; 4] = [
        #[cfg(feature = "crc32c")]
        Some(ChecksumType::Crc32c),
        #[cfg(not(feature = "crc32c"))]
        None,
        #[cfg(feature = "xxhash")]
        Some(ChecksumType::Xxhash),
        #[cfg(not(feature = "xxhash"))]
        None,
        #[cfg(feature = "sha256")]
        Some(ChecksumType::Sha256),
        #[cfg(not(feature = "sha256"))]
        None,
        #[cfg(feature = "blake2b")]
        Some(ChecksumType::Blake2),
        #[cfg(not(feature = "blake2b"))]
        None,
    ];
    pub fn supported_checksums() -> impl Iterator<Item = ChecksumType> + 'static {
        Self::SUPPORTED_CHECKSUMS.iter().copied().filter_map(|c| c)
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
    pub const FS_TREE: u64 = 5; // 5 also happens to be the default subvolume ID
    pub const CSUM_TREE: u64 = 7;
    pub const QUOTA_TREE: u64 = 8;
    pub const UUID_TREE: u64 = 9;
    pub const FREE_SPACE_TREE: u64 = 10;

    pub const ROOT_TREE_DIR: u64 = 6;
    pub const DEV_STATS: u64 = 0;

    pub const FIRST_USER_OBJECTID: u64 = 256;
    pub const LAST_USER_OBJECTID: u64 = (-256i64) as u64;
}
