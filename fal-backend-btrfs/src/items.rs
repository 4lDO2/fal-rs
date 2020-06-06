use core::borrow::Borrow;
use core::convert::TryFrom;
use core::num::NonZeroUsize;
use core::{fmt, mem, slice};

use alloc::borrow::Cow;

use crate::{
    superblock::{IncompatFlags, Superblock},
    u16_le, u32_le, u64_le, DiskKey, PackedUuid, Timespec,
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
        let stripe_count = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[44..46]).ok()?);
        Some(
            NonZeroUsize::new(Self::BASE_LEN + stripe_count as usize * mem::size_of::<Stripe>())
                .unwrap(),
        )
    }

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let stripe_count = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[44..46]).ok()?);

        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }

            if Self::BASE_LEN + stripe_count as usize * mem::size_of::<Stripe>() > bytes.len() {
                dbg!(stripe_count);
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
    pub fn size_in_bytes(&self) -> usize {
        Self::BASE_LEN + self.stripes.len() * mem::size_of::<Stripe>()
    }
    pub fn stripe_for_dev(&self, superblock: &Superblock) -> &Stripe {
        self.stripes
            .iter()
            .find(|stripe| &stripe.device_uuid == &superblock.device_item.uuid)
            .expect("Using the superblock of a different filesystem")
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.size_in_bytes()) }
    }
}
impl ToOwned for ChunkItem {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let mut b = vec![0u8; self.size_in_bytes()].into_boxed_slice();
        b.copy_from_slice(self.as_bytes());

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(slice::from_raw_parts(
                ptr as *const u8 as *const Stripe,
                self.stripes.len(),
            ) as *const [Stripe] as *const Self as *mut Self)
        }
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
    pub device_uuid: PackedUuid,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
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
    pub uuid: PackedUuid,
    pub parent_uuid: PackedUuid,
    pub received_uuid: PackedUuid,

    pub c_xid: u64_le,
    pub o_xid: u64_le,
    pub s_xid: u64_le,
    pub r_xid: u64_le,

    pub ctime: Timespec,
    pub otime: Timespec,
    pub stime: Timespec,
    pub rtime: Timespec,

    pub _rsvd: [u64_le; 8],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
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
        const ROOT_ITEM_INIT = 0x8000_0000;
    }
}
bitflags! {
    pub struct RootItemFlags: u64 {
        const SUBVOLUME_READONLY = 0x1;
    }
}

#[repr(packed)]
#[derive(Eq, Hash, PartialEq)]
pub struct InodeRef {
    pub index: u64_le,
    pub name_len: u16_le,
    pub name: [u8],
}
impl fmt::Debug for InodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let zero_pos = self
            .name
            .iter()
            .copied()
            .position(|c| c == 0)
            .unwrap_or(self.name.len());
        let name = String::from_utf8_lossy(&self.name[..zero_pos]).into_owned();

        f.debug_struct("InodeRef")
            .field("index", &self.index.get())
            .field("name_len", &self.name_len.get())
            .field("name", &name)
            .finish()
    }
}
impl InodeRef {
    pub const BASE_LEN: usize = 10;

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let name_len = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[8..10]).ok()?);

        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }

            if Self::BASE_LEN + name_len as usize > bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr();
            let len = name_len as usize;

            Some(&*(slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
    pub fn size_in_bytes(&self) -> usize {
        Self::BASE_LEN + self.name.len()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.size_in_bytes()) }
    }
}
impl ToOwned for InodeRef {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let mut b = vec![0u8; self.size_in_bytes()].into_boxed_slice();
        b.copy_from_slice(self.as_bytes());

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(
                slice::from_raw_parts(ptr as *const u8, self.name.len()) as *const [u8]
                    as *const Self as *mut Self,
            )
        }
    }
}

#[derive(Eq, PartialEq)]
#[repr(packed)]
pub struct RootRef {
    pub subtree_id: u64_le,
    pub sequence: u64_le,
    pub name_len: u16_le,
    pub name: [u8],
}
impl fmt::Debug for RootRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let zero_pos = self
            .name
            .iter()
            .copied()
            .position(|c| c == 0)
            .unwrap_or(self.name.len());
        let name = String::from_utf8_lossy(&self.name[..zero_pos]).into_owned();

        f.debug_struct("RootRef")
            .field("subtree_id", &self.subtree_id.get())
            .field("sequence", &self.sequence.get())
            .field("name_len", &self.name_len.get())
            .field("name", &name)
            .finish()
    }
}
impl RootRef {
    pub const BASE_LEN: usize = 18;

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let name_len = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[16..18]).ok()?);

        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }

            if Self::BASE_LEN + name_len as usize > bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr();
            let len = name_len as usize;

            Some(&*(slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
    pub fn size_in_bytes(&self) -> usize {
        Self::BASE_LEN + self.name.len()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.size_in_bytes()) }
    }
}
impl ToOwned for RootRef {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let mut b = vec![0u8; self.size_in_bytes()].into_boxed_slice();
        b.copy_from_slice(self.as_bytes());

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(
                slice::from_raw_parts(ptr as *const u8, self.name.len()) as *const [u8]
                    as *const Self as *mut Self,
            )
        }
    }
}

#[derive(Eq, PartialEq)]
#[repr(packed)]
pub struct DirItem {
    pub location: DiskKey,
    pub xid: u64_le,
    pub data_len: u16_le,
    pub name_len: u16_le,
    pub ty: u8,
    pub rest: [u8],
}
impl fmt::Debug for DirItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let zero_pos = self
            .name()
            .iter()
            .copied()
            .position(|c| c == 0)
            .unwrap_or(self.name().len());
        let name = String::from_utf8_lossy(&self.name()[..zero_pos]).into_owned();

        f.debug_struct("DirItem")
            .field("location", &self.location)
            .field("xid", &self.xid.get())
            .field("name_len", &self.name_len.get())
            .field("data_len", &self.data_len.get())
            .field("ty", &self.file_type())
            .field("name", &name)
            .field("data", &self.data())
            .finish()
    }
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
    pub const BASE_LEN: usize = 30;

    pub fn struct_size(bytes: &[u8]) -> Option<usize> {
        if bytes.len() < Self::BASE_LEN {
            return None;
        }
        let data_len = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[25..27]).ok()?);
        let name_len = u16::from_le_bytes(<[u8; 2]>::try_from(&bytes[27..29]).ok()?);

        Some(Self::BASE_LEN + data_len as usize + name_len as usize)
    }
    pub fn file_type(&self) -> Option<Filetype> {
        Filetype::from_u8(self.ty)
    }
    pub fn size_in_bytes(&self) -> usize {
        Self::BASE_LEN + self.rest.len()
    }
    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }
            if Self::struct_size(bytes)? > bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr();
            let len = Self::struct_size(bytes)? - Self::BASE_LEN;

            Some(&*(std::slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.size_in_bytes()) }
    }
    pub fn name(&self) -> &[u8] {
        &self.rest[..self.name_len.get() as usize]
    }
    pub fn data(&self) -> &[u8] {
        &self.rest[self.name_len.get() as usize
            ..self.name_len.get() as usize + self.data_len.get() as usize]
    }
}
impl ToOwned for DirItem {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let mut b = vec![0u8; self.size_in_bytes()].into_boxed_slice();
        b.copy_from_slice(self.as_bytes());

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(
                slice::from_raw_parts(ptr as *const u8, self.rest.len()) as *const [u8]
                    as *const Self as *mut Self,
            )
        }
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

impl ToOwned for FileExtentItem {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let mut b = vec![0u8; self.size_in_bytes()].into_boxed_slice();
        b.copy_from_slice(self.as_bytes());

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(
                slice::from_raw_parts(ptr as *const u8, self.rest.len()) as *const [u8]
                    as *const Self as *mut Self,
            )
        }
    }
}

#[derive(Clone, Copy, Debug, FromPrimitive)]
pub enum FileExtentItemType {
    Inline = 0,
    Reg = 1,
    Prealloc = 2,
}

#[derive(Debug)]
pub enum FileExtentItemExt<'a> {
    Inline(&'a [u8]),
    OnDisk(LayoutVerified<&'a [u8], FileExtentItemExtOnDisk>),
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
        FileExtentItemType::from_u8(self.ty)
    }
    pub const BASE_LEN: usize = 21;

    pub fn size_in_bytes(&self) -> usize {
        Self::BASE_LEN + self.rest.len()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.size_in_bytes()) }
    }

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let len = match FileExtentItemType::from_u8(bytes[20])? {
            FileExtentItemType::Inline => {
                let device_size = u64::from_le_bytes(<[u8; 8]>::try_from(&bytes[8..16]).ok()?);
                usize::try_from(device_size).ok()?
            }
            FileExtentItemType::Prealloc | FileExtentItemType::Reg => {
                mem::size_of::<FileExtentItemExtOnDisk>()
            }
        };

        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }

            if Self::BASE_LEN + len > bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr();

            Some(&*(slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
    pub fn ext<'a>(&'a self) -> Option<FileExtentItemExt<'a>> {
        Some(match self.ty()? {
            FileExtentItemType::Inline => FileExtentItemExt::Inline(&self.rest),
            FileExtentItemType::Prealloc | FileExtentItemType::Reg => {
                FileExtentItemExt::OnDisk(LayoutVerified::new_unaligned(&self.rest)?)
            }
        })
    }
}

impl fmt::Debug for FileExtentItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileExtentItem")
            .field("generation", &{ self.generation })
            .field("device_size", &{ self.device_size })
            .field("compression", &{ self.compression })
            .field("encryption", &{ self.encryption })
            .field("other_encoding", &{ self.other_encoding })
            .field("ty", &self.ty())
            .field("ext", &self.ext())
            .finish()
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
        BlockGroupType::from_bits(self.flags.get())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct ExtentItem {
    pub reference_count: u64_le,
    pub allocation_xid: u64_le,
    pub flags: u64_le,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct ExtentInlineRef {
    pub ty: u8,
    pub offset: u64_le,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct TreeBlockInfo {
    pub key: DiskKey,
    pub level: u8,
}

#[repr(packed)]
pub struct ExtentItemFull {
    pub base: ExtentItem,
    pub rest: [u8],
}
impl ExtentItemFull {
    pub const BASE_LEN: usize = mem::size_of::<ExtentItem>();

    pub fn struct_size(bytes: &[u8]) -> Option<NonZeroUsize> {
        if bytes.len() < Self::BASE_LEN {
            return None;
        }
        let count = bytes.len() - Self::BASE_LEN;

        Some(NonZeroUsize::new(Self::BASE_LEN + count).unwrap())
    }

    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        let base =
            LayoutVerified::<_, ExtentItem>::new_unaligned(&bytes[..mem::size_of::<ExtentItem>()])?
                .into_ref();

        if base.flags()?.contains(ExtentFlags::DATA)
            && base.flags()?.contains(ExtentFlags::TREE_BLOCK)
        {
            todo!("handle invalid flag combination")
        }
        if !base.flags()?.contains(ExtentFlags::DATA)
            && !base.flags()?.contains(ExtentFlags::TREE_BLOCK)
        {
            todo!("handle invalid flag combination")
        }
        if base.flags()?.contains(ExtentFlags::DATA)
            && base.flags()?.contains(ExtentFlags::FULL_BACKREF)
        {
            todo!("handle invalid flag combination")
        }
        unsafe {
            if bytes.len() < Self::BASE_LEN {
                return None;
            }
            let size = Self::struct_size(bytes)?.get();

            if size > bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr();
            let len = size - Self::BASE_LEN;

            Some(&*(slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
    pub fn size_in_bytes(&self) -> usize {
        Self::BASE_LEN + self.rest.len()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.size_in_bytes()) }
    }
}
impl ToOwned for ExtentItemFull {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let mut b = vec![0u8; self.size_in_bytes()].into_boxed_slice();
        b.copy_from_slice(self.as_bytes());

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(
                slice::from_raw_parts(ptr as *const u8, self.rest.len()) as *const [u8]
                    as *const Self as *mut Self,
            )
        }
    }
}

#[derive(Clone, Copy)]
pub struct ExtentItemFullWrapperRef<'a>(pub &'a ExtentItemFull, pub IncompatFlags);

#[derive(Clone)]
pub struct ExtentItemFullWrapperCow<'a>(pub Cow<'a, ExtentItemFull>, pub IncompatFlags);

impl<'a> ExtentItemFullWrapperRef<'a> {
    pub fn tree_block_info(&'a self) -> Option<&'a TreeBlockInfo> {
        if self.1.contains(IncompatFlags::SKINNY_METADATA)
            || self.0.base.flags()?.contains(ExtentFlags::DATA)
        {
            return None;
        }
        let reference: LayoutVerified<&'a [u8], TreeBlockInfo> =
            LayoutVerified::new_unaligned(&self.0.rest[..mem::size_of::<TreeBlockInfo>()])?;
        Some(reference.into_ref())
    }
    pub fn refs(&'a self) -> Option<&'a [ExtentInlineRef]> {
        let slice = if self.0.base.flags()?.contains(ExtentFlags::DATA)
            || self.1.contains(IncompatFlags::SKINNY_METADATA)
        {
            &self.0.rest
        } else {
            &self.0.rest[mem::size_of::<TreeBlockInfo>()..]
        };
        Some(LayoutVerified::new_slice_unaligned(&self.0.rest)?.into_slice())
    }
}
impl<'a> ExtentItemFullWrapperCow<'a> {
    pub fn as_borrowed(&self) -> Option<ExtentItemFullWrapperRef<'a>> {
        if let Cow::Borrowed(reference) = self.0 {
            Some(ExtentItemFullWrapperRef(reference, self.1))
        } else {
            None
        }
    }
    pub fn owned_as_borrowed<'b>(&'b self) -> Option<ExtentItemFullWrapperRef<'b>> {
        if let Cow::Owned(ref owned) = self.0 {
            Some(ExtentItemFullWrapperRef(owned, self.1))
        } else {
            None
        }
    }
    pub fn to_static(self) -> ExtentItemFullWrapperCow<'static> {
        match self.0 {
            Cow::Owned(owned) => ExtentItemFullWrapperCow(Cow::Owned(owned), self.1),
            Cow::Borrowed(borrowed) => ExtentItemFullWrapperCow(Cow::Owned(borrowed.to_owned()), self.1),
        }
    }
    pub fn as_ref<'b: 'a>(&'b self) -> ExtentItemFullWrapperRef<'b> {
        self.owned_as_borrowed().or(self.as_borrowed()).unwrap()
    }
}

impl fmt::Debug for ExtentItemFullWrapperRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtentItemFull")
            .field("base", &self.0.base)
            .field("tree_block_info", &self.tree_block_info())
            .field("refs", &self.refs())
            .finish()
    }
}
impl fmt::Debug for ExtentItemFullWrapperCow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_ref(), f)
    }
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
        ExtentFlags::from_bits(self.flags.get())
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
    pub chunk_tree_uuid: PackedUuid,
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
impl ToOwned for CsumItem {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let b = self.data.to_vec().into_boxed_slice();

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(
                slice::from_raw_parts(ptr as *const u8, self.data.len()) as *const [u8]
                    as *const Self as *mut Self,
            )
        }
    }
}

#[repr(packed)]
pub struct NoAlign<T>(T);

#[repr(packed)]
pub struct UuidItem {
    // Based on print-io.c from the btrfs source code. It seems like UUID items are simply an array
    // of subvolume ids.
    pub subvolumes: [NoAlign<u64_le>],
}

impl UuidItem {
    pub fn parse<'a>(bytes: &'a [u8]) -> &'a Self {
        unsafe {
            let begin = bytes.as_ptr() as *const NoAlign<u64_le>;
            let len = bytes.len() / mem::size_of::<NoAlign<u64_le>>();

            &*(std::slice::from_raw_parts(begin, len) as *const [NoAlign<u64_le>] as *const Self)
        }
    }
    pub fn size_in_bytes(&self) -> usize {
        self.subvolumes.len() * mem::size_of::<NoAlign<u64_le>>()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.size_in_bytes()) }
    }
}
impl fmt::Debug for UuidItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct List<'a>(&'a [NoAlign<u64_le>]);

        impl fmt::Debug for List<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list()
                    .entries(self.0.iter().map(|NoAlign(val)| val.get()))
                    .finish()
            }
        }

        f.debug_struct("UuidItem")
            .field("submodules", &List(&self.subvolumes))
            .finish()
    }
}
impl PartialEq for UuidItem {
    fn eq(&self, other: &Self) -> bool {
        self.subvolumes
            .iter()
            .map(|NoAlign(val)| val.get())
            .eq(other.subvolumes.iter().map(|NoAlign(val)| val.get()))
    }
}
impl ToOwned for UuidItem {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let mut b = vec![0u8; self.size_in_bytes()].into_boxed_slice();
        b.copy_from_slice(self.as_bytes());

        let ptr: *mut [u8] = Box::into_raw(b);
        unsafe {
            Box::from_raw(slice::from_raw_parts(
                ptr as *const u8 as *const NoAlign<u64_le>,
                self.subvolumes.len(),
            ) as *const [NoAlign<u64_le>] as *const Self
                as *mut Self)
        }
    }
}

#[derive(Clone, Copy, Debug, AsBytes, FromBytes, Unaligned)]
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
    pub uuid: PackedUuid,
    pub fs_uuid: PackedUuid,
}

#[cfg(test)]
mod tests {
    macro_rules! check_parsing(
        ( $ty:ident, $bytes:expr ) => {{
            let value: &$ty = $ty::parse($bytes).expect("failed to parse");
            assert_eq!(&$bytes[..], value.as_bytes());
            let boxed: Box<$ty> = value.to_owned();
            assert_eq!(&($bytes)[..], boxed.as_bytes());
            assert_eq!(&*boxed, value);
        }}
    );
    #[test]
    fn chunk_item_parsing() {
        use super::ChunkItem;

        let bytes = [
            0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,
            0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0xbd, 0x87, 0x14, 0x30, 0xc6, 0x33,
            0x45, 0xea, 0xa6, 0x53, 0xe8, 0x07, 0x28, 0x08, 0x2d, 0xf8,
        ];
        check_parsing!(ChunkItem, &bytes)
    }
    #[test]
    fn inode_ref_parsing() {
        use super::InodeRef;

        let bytes = [
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x73, 0x75, 0x62, 0x73,
            0x75, 0x62, 0x66, 0x69, 0x6C, 0x65,
        ];
        check_parsing!(InodeRef, &bytes)
    }
    #[test]
    fn dir_item_parsing() {
        use super::DirItem;

        let bytes = [
            0x07, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A,
            0x00, 0x01, 0x73, 0x75, 0x62, 0x73, 0x75, 0x62, 0x66, 0x69, 0x6C, 0x65,
        ];
        check_parsing!(DirItem, &bytes)
    }
    /*#[test]
    fn root_ref_parsing() {
        use super::RootRef;

        let bytes = [
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x0A, 0x00, 0x66, 0x61, 0x6C, 0x2D, 0x73, 0x75, 0x6C,
        ];
        check_parsing!(RootRef, &bytes);
    }*/
}
