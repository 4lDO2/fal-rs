use core::{fmt, mem};

use arrayvec::ArrayVec;
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
const MAGIC: u64 = u64::from_le_bytes(*b"_BHRfS_M");

#[derive(Clone, Copy, AsBytes, FromBytes, Unaligned)]
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

    pub compat_flags: u64_le,
    pub ro_compat_flags: u64_le,
    pub incompat_flags: u64_le,

    pub checksum_type: u16_le,

    pub root_level: u8,
    pub chunk_root_level: u8,
    pub log_root_level: u8,

    pub device_item: DevItem,
    // stores a [u8; 256] internally, but newtyped
    pub device_label: DeviceLabel,

    pub cache_generation: u64_le,
    pub uuid_tree_generation: u64_le,
    pub metadata_uuid: PackedUuid,
    pub _rsvd: [u64_le; 28],

    //pub system_chunk_array: [u8; 2048],
    pub system_chunk_array: SystemChunkArray,
    pub root_backups: [RootBackup; 4],
}
#[repr(packed)]
#[derive(Clone, Copy)]
pub struct DeviceLabel([u8; 256]);

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
        write!(f, "\"{}\"", self)
    }
}

#[repr(packed)]
#[derive(Clone, Copy)]
pub struct SystemChunkArray([u8; 2048]);

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

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("superblock parsing error: {0}")]
    ParseError(#[from] SuperblockParseError),
}

impl Superblock {
    pub const SIZE: u16 = mem::size_of::<Self>() as u16;

    pub fn load<D: fal::DeviceRo>(device: &mut D) -> Result<Box<Self>, LoadError> {
        let disk_info = device.disk_info_blocking().unwrap();
        let disk_size = disk_info.block_count * u64::from(disk_info.block_size);
        // FIXME

        let mut block = fal::ioslice::IoBox::alloc_uninit(Self::SIZE as usize);

        SUPERBLOCK_OFFSETS
            .iter()
            .copied()
            .filter(|offset| offset + u64::from(Superblock::SIZE) < disk_size)
            .map(|offset| {
                device
                    .read_blocking(offset, &mut [block.as_ioslice_mut()])?;

                Self::parse(&block)
            })
            .max_by_key(|sb| sb.generation.get())
            .expect("iterator should not be empty")
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

        let stored_checksum = superblock.stored_checksum();
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
    pub fn stored_checksum(&self) -> Checksum {
        Checksum::parse(self.checksum_ty(), &self.checksum)
            .expect("expected Checksum::parse to succeed for arrays with a length of 32 bytes")
    }
    pub fn calculate_checksum(&self) -> Checksum {
        Checksum::calculate(self.checksum_ty(), self.as_bytes())
    }
    /// Gets the read-only compatible flags. All of these flags have to be supported by this
    /// implementation to be able to modify the filesystem.
    ///
    /// The return value is either as Ok(flags) if no unrecognized flags were found,
    /// or Err((flags, rest)) if there were flags from the future (or if everything was malformed).
    pub fn ro_compat_flags(&self) -> Result<RoCompatFlags, (RoCompatFlags, u64)> {
        let flags = self.ro_compat_flags.get();

        match RoCompatFlags::from_bits(flags) {
            Some(f) => Ok(f),
            None => Err((
                RoCompatFlags::from_bits_truncate(flags),
                flags & !RoCompatFlags::all().bits(),
            )),
        }
    }
    /// Gets the incompatible flags. If any of the flags on-disk were unrecognized, this would mean
    /// that the filesystem would fail to mount (hence only the bitmask in the Err variant).
    pub fn incompat_flags(&self) -> Result<IncompatFlags, u64> {
        let flags = self.incompat_flags.get();

        match IncompatFlags::from_bits(flags) {
            Some(f) => Ok(f),
            None => Err(flags & !IncompatFlags::all().bits()),
        }
    }
    pub fn compat_flags(&self) -> CompatFlags {
        CompatFlags
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CompatFlags;

bitflags! {
    pub struct RoCompatFlags: u64 {
        const FREE_SPACE_TREE = 1 << 0;
        const FREE_SPACE_TREE_VALID = 1 << 1;
    }
}
bitflags! {
    pub struct IncompatFlags: u64 {
        const MIXED_BACKREF = 1 << 0;
        const DEFAULT_SUBVOL = 1 << 1;
        const MIXED_GROUPS = 1 << 2;
        const COMPRESS_LZO = 1 << 3;
        const COMPRESS_ZSTD = 1 << 4;
        const BIG_METADATA = 1 << 5;
        const EXTENDED_IREF = 1 << 6;
        const RAID56 = 1 << 7;
        const SKINNY_METADATA = 1 << 8;
        const NO_HOLES = 1 << 9;
        const METADATA_UUID = 1 << 10;
        const RAID1C34 = 1 << 11;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn superblock_struct_size() {
        assert_eq!(mem::size_of::<Superblock>(), 4096);
    }
}

struct RoCompatFlagsDbg(Result<RoCompatFlags, (RoCompatFlags, u64)>);
struct IncompatFlagsDbg(Result<IncompatFlags, u64>);

impl fmt::Debug for RoCompatFlagsDbg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Ok(recognized_flags) => write!(f, "[read-only compatible flags {:?}, all recognized]", recognized_flags),
            Err((recognized_flags, remainder)) => write!(f, "[read-only compatible flags {:?}, some of them were not recognized: {:#0x}, thus the filesystem must be mounted immutably]", recognized_flags, remainder),
        }
    }
}
impl fmt::Debug for IncompatFlagsDbg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Ok(recognized_flags) => write!(f, "[backwards-incompatible flags {:?}, all recognized]", recognized_flags),
            // TODO: Print the recognized flags too
            Err(remainder) => write!(f, "[incompatible flags with the mask {:#0x} were detected, some may have been recognized]", remainder),
        }
    }
}

impl fmt::Debug for Superblock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {

        f.debug_struct("Superblock")
            .field("checksum", &self.stored_checksum())
            .field("calculated_checksum", &self.calculate_checksum())
            .field("byte_number", &self.byte_number.get())
            .field("flags", &self.flags.get())
            .field("magic", &self.magic.get())
            .field("generation", &self.generation.get())
            .field("root", &self.root.get())
            .field("chunk_root", &self.chunk_root.get())
            .field("log_root", &self.log_root.get())
            .field("log_root_transid", &self.log_root_transid.get())
            .field("total_byte_count", &self.total_byte_count.get())
            .field("total_bytes_used", &self.total_bytes_used.get())
            .field("root_dir_objectid", &self.root_dir_objectid.get())
            .field("device_count", &self.device_count.get())
            .field("sector_size", &self.sector_size.get())
            .field("node_size", &self.node_size.get())
            .field("unused_leaf_size", &self.unused_leaf_size.get())
            .field("stripe_size", &self.stripe_size.get())
            .field("system_chunk_array_size", &self.system_chunk_array_size.get())
            .field("chunk_root_gen", &self.chunk_root_gen.get())
            .field("compat_flags", &self.compat_flags())
            .field("ro_compat_flags", &RoCompatFlagsDbg(self.ro_compat_flags()))
            .field("incompat_flags", &IncompatFlagsDbg(self.incompat_flags()))
            .field("root_level", &self.root_level)
            .field("chunk_root_level", &self.chunk_root_level)
            .field("log_root_level", &self.log_root_level)
            .field("device_item", &self.device_item)
            .field("device_label", &self.device_label)
            .field("cache_generation", &self.cache_generation.get())
            .field("uuid_tree_generation", &self.uuid_tree_generation.get())
            .field("metadata_uuid", &self.metadata_uuid.get())
            // NOTE: We currently ignore _rsvd.
            .field("system_chunk_array")
            .field("root_backups", &self.root_backups)
            .finish()
    }
}
