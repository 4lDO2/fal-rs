use core::{fmt, mem, slice};
use core::convert::TryFrom;
use core::num::NonZeroUsize;

use crate::{
    superblock::{ChecksumType, Superblock},
    DiskKey, Timespec,

    u64_le, u32_le, u16_le,
};

use bitflags::bitflags;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive as _;
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned};

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

#[derive(Eq, Hash, PartialEq)]
#[repr(packed)]
pub struct ChunkItem {
    pub len: u64_le,
    pub owner: u64_le,

    pub stripe_length: u64_le,
    pub ty: u64_le,

    pub io_alignment: u32_le,
    pub io_width: u32_le,

    pub sector_size: u32_le,
    pub stripe_count: u16_le,
    pub sub_stripe_count: u16_le,
    pub stripes: [Stripe],
}

impl ChunkItem {
    pub const BASE_LEN: usize = 48;

    pub fn struct_size(bytes: &[u8]) -> Option<NonZeroUsize> {
        if bytes.len() < Self::BASE_LEN {
            return None;
        }
        let stripe_count = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[46..48]).ok()?);
        Self::BASE_LEN + stripe_count as usize * mem::size_of::<Stripe>();
    }

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let stripe_count = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[44..56]).ok()?);

        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }

            if Self::BASE_LEN + stripe_count as usize * mem::size_of::<Stripe>() >= bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr() as *const Stripe;
            let len = stripe_count as usize;

            // I'm amazed this actually works and was allowed by miri when I tested a similar
            // example. It's a little bizarre to create the slice you first want at the end and
            // then eventually cast it to the whole struct.
            Some(&*(slice::from_raw_parts(begin, len) as *const [Stripe] as *const Self))
        }
    }
    pub fn stripe_for_dev(&self, superblock: &Superblock) -> &Stripe {
        self.stripes
            .iter()
            .find(|stripe| &stripe.device_uuid == &superblock.device_item.uuid)
            .expect("Using the superblock of a different filesystem")
    }
}
impl fmt::Debug for ChunkItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkItem")
            .field("len", &self.len.get())
            .field("owner", &self.owner.get())
            .field("stripe_length", &self.stripe_length.get())
            .field("ty", &BlockGroupType::from_bits_truncate(self.ty.get()))
            .field("io_alignment", &self.io_alignment.get())
            .field("io_width", &self.io_width.get())
            .field("sector_size", &self.sector_size.get())
            .field("stripe_count", &self.stripe_count.get())
            .field("sub_stripe_count", &self.sub_stripe_count.get())
            .field("stripes", &&self.stripes)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct Stripe {
    pub device_id: u64_le,
    pub offset: u64_le,
    pub device_uuid: [u8; 16],
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct RootItem {
    pub inode_item: InodeItem,
    pub generation: u64_le,
    pub root_directory_id: u64_le,
    pub addr: u64_le,
    pub byte_limit: u64_le,
    pub bytes_used: u64_le,
    pub last_snapshot: u64_le,
    pub flags: u64_le,
    pub refs: u32_le,

    pub drop_progress: DiskKey,
    pub drop_level: u8,
    pub level: u8,

    pub generation_v2: u64_le,
    pub uuid: [u8; 16],
    pub parent_uuid: [u8; 16],
    pub received_uuid: [u8; 16],

    pub c_xid: u64_le,
    pub o_xid: u64_le,
    pub s_xid: u64_le,
    pub r_xid: u64_le,

    pub ctime: Timespec,
    pub otime: Timespec,
    pub stime: Timespec,
    pub rtime: Timespec,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct InodeItem {
    pub generation: u64_le,
    pub transaction_id: u64_le,
    pub size: u64_le,
    pub byte_count: u64_le,
    pub block_group: u64_le,
    pub hardlink_count: u32_le,
    pub uid: u32_le,
    pub gid: u32_le,
    pub mode: u32_le,
    pub rdev: u64_le,
    pub flags: u64_le,
    pub sequence: u64_le,

    pub _rsvd: [u64_le; 4],

    pub atime: Timespec,
    pub ctime: Timespec,
    pub mtime: Timespec,
    pub otime: Timespec,
}

bitflags! {
    pub struct InodeFlags: u64 {
        const NO_DATA_SUM = 0x1;
        const NO_DATA_COW = 0x2;
        const READONLY = 0x4;
        const DONT_COMPRESS = 0x8;
        const PREALLOCATE = 0x10;
        const SYNCHRONOUS = 0x20;
        const IMMUTABLE = 0x40;
        const APPEND_ONLY = 0x80;
        const NO_DUMP = 0x100;
        const NOATIME = 0x200;
        const SYNCHRONOUS_DIR_OPS = 0x400;
        const COMPRESS = 0x800;
        const ROOT_ITEM_INIT = 0x80000000;
    }
}
bitflags! {
    pub struct RootItemFlags: u64 {
        const SUBVOLUME_READONLY = 0x1;
    }
}

#[repr(packed)]
pub struct InodeRef {
    pub index: u64_le,
    pub name_len: u16_le,
    pub name: [u8],
}
impl fmt::Debug for InodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InodeRef")
            .field("index", &{self.index})
            .field("name_len", &{self.name_len})
            .field("name", &self.name)
            .finish()
    }
}
impl InodeRef {
    pub const BASE_LEN: usize = 10;

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let name_len = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[8..10]));

        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }

            if Self::BASE_LEN + name_len as usize >= bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr();
            let len = name_len as usize;

            // I'm amazed this actually works and was allowed by miri when I tested a similar
            // example. It's a little bizarre to create the slice you first want at the end and
            // then eventually cast it to the whole struct.
            Ok(&*(slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
}

#[derive(Clone, Debug, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct RootRef {
    pub subtree_id: u64_le,
    pub sequence: u64_le,
    pub name_len: u16_le,
}

#[derive(Clone, Debug, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct DirItem {
    pub location: DiskKey,
    pub xid: u64_le,
    pub data_len: u16_le,
    pub name_len: u16_le,
    pub ty: u8,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, FromPrimitive)]
pub enum Filetype {
    Unknown = 0,
    RegularFile = 1,
    Directory = 2,
    CharacterDevice = 3,
    BlockDevice = 4,
    Fifo = 5,
    Socket = 6,
    Symlink = 7,

    // Only interally used, not user-visible.
    Xattr = 8,
}

impl DirItem {
    pub fn file_type(&self) -> Option<Filetype> {
        Filetype::from_u8(self.ty)
    }
}

#[repr(packed)]
pub struct FileExtentItem {
    pub generation: u64_le,
    pub device_size: u64_le,
    pub compression: u8,
    pub encryption: u8,
    pub other_encoding: u16_le,
    pub ty: u8,

    /// Either inline data or `FileExtentItemExtOnDisk`.
    pub rest: [u8],
}

#[derive(Clone, Copy, Debug, FromPrimitive)]
pub enum FileExtentItemType {
    Inline = 0,
    Reg = 1,
    Prealloc = 2,
}

pub enum FileExtentItemExt<'a> {
    Inline(&'a [u8]),
    OnDisk(LayoutVerified<&'a [u8], FileExtentItemExtOnDisk>)
}

#[derive(Clone, Copy, Debug, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct FileExtentItemExtOnDisk {
    disk_bytenr: u64_le,
    disk_byte_count: u64_le,
    disk_offset: u64_le,
    byte_count: u64_le,
}

impl FileExtentItem {
    pub fn ty(&self) -> Option<FileExtentItemType> {
        FileExtentItem::from_u8(self.ty)
    }
    pub const BASE_LEN: usize = 21;

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let len = match FileExtentItemType::from_u8(bytes[20])? {
            FileExtentItemType::Inline => {
                let device_size = u64::from_le_bytes(<[u8; 8]>::try_from(&bytes[8..16]));
                usize::try_from(device_size).ok()?
            }
            FileExtentItemType::Prealloc | FileExtentItemType::Reg => mem::size_of::<FileExtentItemExtOnDisk>(),
        };

        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }

            if Self::BASE_LEN + len > bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr();

            Ok(&*(slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
    pub fn ext<'a>(&'a self) -> Option<FileExtentItemExt<'a>> {
        Some(match self.ty()? {
            FileExtentItemType::Inline => FileExtentItemExt::Inline(&self.rest),
            FileExtentItemType::Prealloc | FileExtentItemType::Reg => FileExtentItemExt::OnDisk(LayoutVerified::new_unaligned(&self.rest)),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct BlockGroupItem {
    pub bytes_used: u64_le,
    pub chunk_oid: u64_le,
    pub flags: u64_le,
}

impl BlockGroupItem {
    pub fn ty(&self) -> Option<BlockGroupType> {
        BlockGroupType::from_bits(self.flags)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct ExtentItem {
    pub reference_count: u64_le,
    pub allocation_xid: u64_le,
    pub flags: u64_le,
}

bitflags! {
    pub struct ExtentFlags: u64 {
        const DATA = 0x1;
        const TREE_BLOCK = 0x2;
        const FULL_BACKREF = 0x80;
    }
}

impl ExtentItem {
    pub fn flags(&self) -> Option<ExtentFlags> {
        ExtentFlags::from_bits(self.flags)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct DevStatsItem {
    pub write_errs: u64_le,
    pub read_errs: u64_le,
    pub flush_errs: u64_le,

    pub corruption_errs: u64_le,
    pub generation_errs: u64_le,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct DevExtent {
    pub chunk_tree: u64_le,
    pub chunk_oid: u64_le,
    pub chunk_offset: u64_le,
    pub len: u64_le,
    pub chunk_tree_uuid: [u8; 16],
}

#[derive(Eq, Hash, PartialEq)]
#[repr(packed)]
pub struct CsumItem {
    pub data: [u8],
}

impl CsumItem {
    pub fn parse<'a>(bytes: &'a [u8]) -> &'a Self {
        unsafe {
            let begin = bytes.as_ptr();
            let len = bytes.len();

            &*(std::slice::from_raw_parts(begin, len) as *const [u8] as *const Self)
        }
    }
}
impl fmt::Debug for CsumItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsumItem")
            .field("data", &&self.data)
            .finish()
    }
}

#[derive(Eq, Hash, PartialEq)]
#[repr(packed)]
pub struct UuidItem {
    // Based on print-io.c from the btrfs source code. It seems like UUID items are simply an array
    // of subvolume ids.
    pub subvolumes: [u64],
}

impl UuidItem {
    pub fn parse<'a>(bytes: &'a [u8]) -> &'a Self {
        unsafe {
            let begin = bytes.as_ptr() as *const u64;
            let len = bytes.len() / mem::size_of::<u64>();

            &*(std::slice::from_raw_parts(begin, len) as *const [u64] as *const Self)
        }
    }
}
impl fmt::Debug for UuidItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UuidItem")
            .field("submodules", &&self.subvolumes)
            .finish()
    }
}

#[derive(Debug, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct DevItem {
    pub id: u64_le,
    pub size: u64_le,
    pub bytes_used: u64_le,
    pub io_alignment: u32_le,
    pub io_width: u32_le,
    pub sector_size: u32_le,
    pub type_and_info: u64_le,
    pub generation: u64_le,
    pub start_byte: u64_le,
    pub group: u32_le,
    pub seek_speed: u8,
    pub bandwidth: u8,
    pub uuid: [u8; 16],
    pub fs_uuid: [u8; 16],
}
