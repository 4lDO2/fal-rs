use core::{fmt, mem};

use bitflags::bitflags;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive as _;
use thiserror::Error;
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned};

use crate::{
    items::{ChunkItem, DevItem},
    sizes, u16_le, u32_le, u64_le, Checksum, DiskKey, InvalidChecksum, PackedUuid,
};

const SUPERBLOCK_OFFSETS: [u64; 4] = [64 * sizes::K, 64 * sizes::M, 256 * sizes::G, 1 * sizes::P];
const MAGIC: u64 = 0x4D5F_5366_5248_425F; // ASCII for "_BHRfS_M"

#[derive(Clone, Copy, Debug, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct Superblock {
    pub checksum: [u8; 32],
    pub fs_id: PackedUuid,
    pub byte_number: u64_le,
    pub flags: u64_le,
    pub magic: u64_le,
    pub generation: u64_le,
    pub root: u64_le,
    pub chunk_root: u64_le,
    pub log_root: u64_le,

    pub log_root_transid: u64_le,
    pub total_byte_count: u64_le,
    pub total_bytes_used: u64_le,
    pub root_dir_objectid: u64_le,
    pub device_count: u64_le,

    pub sector_size: u32_le,
    pub node_size: u32_le,
    pub unused_leaf_size: u32_le,
    pub stripe_size: u32_le,
    pub system_chunk_array_size: u32_le,
    pub chunk_root_gen: u64_le,

    pub optional_flags: u64_le,
    pub flags_for_write_support: u64_le,
    pub required_flags: u64_le,

    pub checksum_type: u16_le,

    pub root_level: u8,
    pub chunk_root_level: u8,
    pub log_root_level: u8,

    pub device_item: DevItem,
    //pub device_label: [u8; 256],
    pub device_label: DeviceLabel,

    pub cache_generation: u64_le,
    pub uuid_tree_generation: u64_le,
    pub metadata_uuid: PackedUuid,
    pub _rsvd: [u64_le; 28],

    //pub system_chunk_array: [u8; 2048],
    pub system_chunk_array: SystemChunkArray,
    pub root_backups: [RootBackup; 4],
}
//#[derive(Debug)]
//pub struct SystemChunkArray(pub Vec<(DiskKey, ChunkItem)>);

#[repr(packed)]
pub struct DeviceLabel([u8; 256]);
impl Clone for DeviceLabel {
    fn clone_from(&mut self, source: &Self) {
        self.0.copy_from_slice(&source.0)
    }
    fn clone(&self) -> Self {
        let mut new = Self([0u8; 256]);
        new.clone_from(self);
        new
    }
}
impl Copy for DeviceLabel {}

// XXX: Const generics; for some obscure reason, only arrays with length of up to 32 bytes actually
// support basic traits.

unsafe impl zerocopy::FromBytes for DeviceLabel {
    // "Ironic"
    fn only_derive_is_allowed_to_implement_this_trait()
    where
        Self: Sized,
    {
    }
}
unsafe impl zerocopy::AsBytes for DeviceLabel {
    fn only_derive_is_allowed_to_implement_this_trait()
    where
        Self: Sized,
    {
    }
}
unsafe impl zerocopy::Unaligned for DeviceLabel {
    fn only_derive_is_allowed_to_implement_this_trait()
    where
        Self: Sized,
    {
    }
}

impl fmt::Display for DeviceLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = if let Some(zero_idx) = self.0.iter().copied().position(|b| b == 0) {
            &self.0[..zero_idx]
        } else {
            &self.0[..]
        };
        write!(f, "{}", String::from_utf8_lossy(bytes))
    }
}
impl fmt::Debug for DeviceLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"")?;
        fmt::Display::fmt(self, f)?;
        write!(f, "\"")
    }
}

#[repr(packed)]
pub struct SystemChunkArray([u8; 2048]);
impl Clone for SystemChunkArray {
    fn clone_from(&mut self, source: &Self) {
        self.0.copy_from_slice(&source.0)
    }
    fn clone(&self) -> Self {
        let mut new = Self([0u8; 2048]);
        new.clone_from(self);
        new
    }
}
impl Copy for SystemChunkArray {}

unsafe impl zerocopy::FromBytes for SystemChunkArray {
    fn only_derive_is_allowed_to_implement_this_trait()
    where
        Self: Sized,
    {
    }
}
unsafe impl zerocopy::AsBytes for SystemChunkArray {
    fn only_derive_is_allowed_to_implement_this_trait()
    where
        Self: Sized,
    {
    }
}
unsafe impl zerocopy::Unaligned for SystemChunkArray {
    fn only_derive_is_allowed_to_implement_this_trait()
    where
        Self: Sized,
    {
    }
}
impl fmt::Debug for SystemChunkArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.iter().copied()).finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, FromPrimitive)]
#[repr(u16)]
pub enum ChecksumType {
    Crc32c = 0,
    Xxhash = 1,
    Sha256 = 2,
    Blake2 = 3,
}
impl ChecksumType {
    pub fn size(self) -> usize {
        match self {
            Self::Crc32c => 4,
            Self::Xxhash => 8,
            Self::Sha256 => 32,
            Self::Blake2 => 32,
        }
    }
}

impl Superblock {
    pub fn load<D: fal::DeviceRo>(device: &mut D) -> Self {
        let disk_info = device.disk_info().unwrap();
        let disk_size = disk_info.block_count * u64::from(disk_info.block_size);
        // FIXME

        let mut block = [0u8; 4096];

        SUPERBLOCK_OFFSETS
            .iter()
            .copied()
            .filter(|offset| offset + 4096 < disk_size)
            .map(|offset| {
                device
                    .read_blocks(offset / u64::from(disk_info.block_size), &mut block)
                    .unwrap();

                *Self::parse(&block).unwrap()
            })
            .max_by_key(|sb| sb.generation.get())
            .unwrap()
    }
    pub fn parse<'a>(
        block: &'a [u8],
    ) -> Result<LayoutVerified<&'a [u8], Self>, SuperblockParseError> {
        let superblock =
            LayoutVerified::<&'a [u8], Self>::new_unaligned(&block[..mem::size_of::<Superblock>()])
                .expect("calling btrfs::Superblock::parse with insufficient bytes");

        if superblock.magic.get() != MAGIC {
            return Err(SuperblockParseError::BadMagicNumber);
        }

        let checksum_ty = ChecksumType::from_u16(superblock.checksum_type.get()).ok_or(
            SuperblockParseError::UnrecognizedChecksumTy(superblock.checksum_type.get()),
        )?;

        let stored_checksum = Checksum::parse(checksum_ty, &block[..32]).unwrap();
        let calculated_checksum = Checksum::calculate(checksum_ty, &block[32..])
            .ok_or(InvalidChecksum::UnsupportedChecksum(checksum_ty))?;

        if calculated_checksum == stored_checksum {
            Ok(superblock)
        } else {
            Err(InvalidChecksum::Mismatch.into())
        }
    }
    /// Panics if the checksum_ty field has been set to something else after validation.
    pub fn checksum_ty(&self) -> ChecksumType {
        ChecksumType::from_u16(self.checksum_type.get())
            .expect("checksum_ty has been set to invalid value")
    }
}

#[derive(Debug, Error)]
pub enum SuperblockParseError {
    #[error("bad magic number")]
    BadMagicNumber,

    #[error("unrecognized checksum type: {0}")]
    UnrecognizedChecksumTy(u16),

    #[error("invalid checksum: {0}")]
    InvalidChecksum(#[from] InvalidChecksum),
}

#[derive(Clone, Copy, Debug, Default, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct RootBackup {
    pub tree_root: u64_le,
    pub tree_root_generation: u64_le,
    pub chunk_root: u64_le,
    pub chunk_root_generation: u64_le,
    pub extent_root: u64_le,
    pub extent_root_generation: u64_le,
    pub filesystem_root: u64_le,
    pub filesystem_root_generation: u64_le,
    pub device_root: u64_le,
    pub device_root_generation: u64_le,
    pub checksum_root: u64_le,
    pub checksum_root_generation: u64_le,
    pub total_bytes: u64_le,
    pub bytes_used: u64_le,
    pub device_count: u64_le,
    pub tree_root_level: u8,
    pub chunk_root_level: u8,
    pub extent_root_level: u8,
    pub filesystem_root_level: u8,
    pub device_root_level: u8,
    pub checksum_root_level: u8,
}

impl SystemChunkArray {
    pub fn iter<'a>(
        &'a self,
        sys_chunk_array_size: usize,
    ) -> impl Iterator<Item = (LayoutVerified<&'a [u8], DiskKey>, &'a ChunkItem)> + 'a {
        struct Iter<'a>(&'a [u8]);

        impl<'a> Iterator for Iter<'a> {
            type Item = (LayoutVerified<&'a [u8], DiskKey>, &'a ChunkItem);

            fn next(&mut self) -> Option<Self::Item> {
                let key_size = mem::size_of::<DiskKey>();

                if self.0.len() < key_size {
                    return None;
                }

                // TODO: Verify that types are correct etc.

                let key = LayoutVerified::<_, DiskKey>::new_unaligned(&self.0[..key_size])?;
                self.0 = &self.0[key_size..];

                let item_size = ChunkItem::struct_size(&self.0[..ChunkItem::BASE_LEN])?.get();
                let item = ChunkItem::parse(&self.0[..item_size])?;

                self.0 = &self.0[item_size..];

                Some((key, item))
            }
        }

        /*let stride = mem::size_of::<DiskKey>() + mem::size_of::<ChunkItem>();

        (0..bytes.len() / stride)
            .map(move |i| {
                let key_bytes = &bytes[i * stride..i * stride + DiskKey::LEN];
                let chunk_bytes = &bytes[i * stride + DiskKey::LEN..(i + 1) * stride];

                let key = DiskKey::parse(key_bytes);
                assert_eq!(key.ty, DiskKeyType::ChunkItem);

                let chunk = ChunkItem::parse(chunk_bytes);
                assert!(chunk.ty.contains(BlockGroupType::SYSTEM));

                // Only RAID 0 is supported so far.
                assert_eq!(
                    chunk.stripe_count, 1,
                    "Unimplemented RAID configuration with stripe count {}",
                    chunk.stripe_count
                );
                assert_eq!(
                    chunk.sub_stripe_count, 0,
                    "Unimplemented RAID configuration with sub stripe count (used for RAID 10) {}",
                    chunk.sub_stripe_count
                );

                (key, chunk)
            })
        */
        Iter(&self.0[..sys_chunk_array_size])
    }
}

bitflags! {
    pub struct SuperblockFlags: u64 {
        const WRITTEN = 1 << 0;
        const RELOC = 1 << 1;

        const ERROR = 1 << 2;
        const SEEDING = 1 << 32;
        const METADUMP = 1 << 33;
        const METADUMP_V2 = 1 << 34;
        const CHANGING_FSID = 1 << 35;
        const CHANGING_FSID_V2 = 1 << 36;
    }
}
