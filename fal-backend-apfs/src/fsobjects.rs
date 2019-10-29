use crate::{
    btree::{BTreeKey, BTreeValue},
    crypto::ProtectionClass,
    ObjectIdentifier, TransactionIdentifier,
};

use std::cmp::Ordering;

use fal::parsing::{read_u16, read_u32, read_u64, read_u8};

use bitflags::bitflags;
use enum_primitive::*;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JKey {
    pub oid: ObjectIdentifier,
    pub ty: JObjType,
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub enum JObjType {
        Any = 0,
        SnapshotMetadata = 1,
        Extent = 2,
        Inode = 3,
        Xattr = 4,
        SiblingLink = 5,
        DataStreamId = 6,
        CryptoState = 7,
        FileExtent = 8,
        DirRecord = 9,
        DirStats = 10,
        SnapshotName = 11,
        SiblingMap = 12,
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum JAnyKey {
    InodeKey(JInodeKey),
    DrecKey(JDrecKey),
    DrecHashedKey(JDrecHashedKey),
    DirStatsKey(JDirStatsKey),
    XattrKey(JXattrKey),
    DatastreamIdKey(JDatastreamIdKey),
    FileExtentKey(JFileExtentKey),
}

impl JAnyKey {
    fn header(&self) -> &JKey {
        match self {
            Self::InodeKey(key) => &key.header,
            Self::DrecKey(key) => &key.header,
            Self::DrecHashedKey(key) => &key.header,
            Self::DirStatsKey(key) => &key.header,
            Self::XattrKey(key) => &key.header,
            Self::DatastreamIdKey(key) => &key.header,
            Self::FileExtentKey(key) => &key.header,
        }
    }
    /// A partial comparison functions for filesystem-layer objects. Only compares the object ID
    /// and the type, and ignores filenames or virtual addresses (for extents).
    pub fn partial_compare(k1: &BTreeKey, k2: &BTreeKey) -> Ordering {
        Ord::cmp(
            k1.as_fs_layer_key().unwrap().header(),
            k2.as_fs_layer_key().unwrap().header(),
        )
    }
    pub fn into_drec_hashed_key(self) -> Option<JDrecHashedKey> {
        match self {
            Self::DrecHashedKey(k) => Some(k),
            _ => None,
        }
    }
    pub fn into_inode_key(self) -> Option<JInodeKey> {
        match self {
            Self::InodeKey(k) => Some(k),
            _ => None,
        }
    }
    pub fn into_file_extent_key(self) -> Option<JFileExtentKey> {
        match self {
            Self::FileExtentKey(k) => Some(k),
            _ => None,
        }
    }
}

impl Ord for JAnyKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.header()
            .cmp(other.header())
            // Types are equal
            .then(match (self, other) {
                (Self::DrecKey(k1), Self::DrecKey(k2)) => Ord::cmp(k1, k2),
                (Self::DrecHashedKey(k1), Self::DrecHashedKey(k2)) => Ord::cmp(k1, k2),
                (Self::XattrKey(k1), Self::XattrKey(k2)) => Ord::cmp(k1, k2),
                (Self::FileExtentKey(k1), Self::FileExtentKey(k2)) => Ord::cmp(k1, k2),
                _ => Ordering::Equal,
            })
    }
}
impl PartialOrd for JAnyKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl JKey {
    const OBJECT_ID_MASK: u64 = 0x0FFF_FFFF_FFFF_FFFF;
    const OBJECT_TYPE_MASK: u64 = 0xF000_0000_0000_0000;
    const OBJECT_TYPE_SHIFT: u64 = 60;

    const LEN: usize = 8;

    pub fn parse(bytes: &[u8]) -> Self {
        let object_id_and_type = fal::read_u64(bytes, 0);

        let oid = ObjectIdentifier::from(object_id_and_type & Self::OBJECT_ID_MASK);
        let ty = JObjType::from_u8(
            ((object_id_and_type & Self::OBJECT_TYPE_MASK) >> Self::OBJECT_TYPE_SHIFT) as u8,
        )
        .unwrap();

        Self { oid, ty }
    }
}

impl Ord for JKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.oid, &with.oid).then(Ord::cmp(&self.ty, &with.ty))
    }
}
impl PartialOrd for JKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JInodeKey {
    pub header: JKey,
}

impl JInodeKey {
    pub fn new(oid: ObjectIdentifier) -> Self {
        Self {
            header: JKey {
                oid,
                ty: JObjType::Inode,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JInodeVal {
    pub parent_id: u64,
    pub private_id: u64,

    pub create_time: u64,
    pub modification_time: u64,
    pub change_time: u64,
    pub access_time: u64,

    pub internal_flags: InodeInternalFlags,

    pub children_or_hardlink_count: ChildrenOrHardlinkCount,

    pub default_protection_class: ProtectionClass,
    pub write_generation_counter: u32,
    pub bsd_flags: u32,

    pub uid: u32,
    pub gid: u32,

    pub ty: InodeType,
    pub permissions: u16,

    pub pad1: u16,
    pub pad2: u64,

    pub extended_fields: Option<Xblob<InodeXfieldType>>,
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum InodeType {
        Fifo = 0o10000,
        CharDev = 0o20000,
        Dir = 0o40000,
        BlockDev = 0o60000,
        File = 0o100000,
        Symlink = 0o120000,
        Socket = 0o140000,
        Whiteout = 0o160000,
    }
}
impl InodeType {
    pub const BITMASK: u16 = 0o170000;
}
bitflags! {
    pub struct InodeInternalFlags: u64 {
        const IS_APFS_PRIVATE = 0x1;
        const MAINTAINS_DIR_STATS = 0x2;
        const EXPLICIT_MAINTAINS_DIR_STATS = 0x4;
        const PROTOCOL_CLASS_EXPLICIT = 0x8;
        const WAS_CLONED = 0x10;
        const UNUSED = 0x20;
        const HAS_ACL = 0x40;
        const BEING_TRUNCATED = 0x80;
        const HAS_FINDER_INFO = 0x100; // LOL
        const IS_SPARSE = 0x200;
        const WAS_EVER_CLONED = 0x400;
        const ACTIVE_FILE_TRIMMED = 0x800;
        const PINNED_TO_MAIN = 0x1000;
        const PINNED_TO_TIER2 = 0x2000;
        const HAS_RSRC_FORK = 0x4000;
        const HAS_NO_RSRC_FORK = 0x8000;
        const ALLOCATION_SPILLEDOVER = 0x10000;
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildrenOrHardlinkCount {
    ChildrenCount(i32),
    HardlinkCount(i32),
}

impl InodeInternalFlags {
    pub fn inherited() -> Self {
        Self::MAINTAINS_DIR_STATS
            | Self::HAS_RSRC_FORK
            | Self::HAS_NO_RSRC_FORK
            | Self::HAS_FINDER_INFO
    }
    pub fn cloned() -> Self {
        Self::HAS_RSRC_FORK | Self::HAS_NO_RSRC_FORK | Self::HAS_FINDER_INFO
    }
}

impl JInodeVal {
    const LEN: usize = 92;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let parent_id = read_u64(bytes, &mut offset);
        let private_id = read_u64(bytes, &mut offset);

        let create_time = read_u64(bytes, &mut offset);
        let modification_time = read_u64(bytes, &mut offset);
        let change_time = read_u64(bytes, &mut offset);
        let access_time = read_u64(bytes, &mut offset);

        let internal_flags = InodeInternalFlags::from_bits(read_u64(bytes, &mut offset)).unwrap();
        let children_or_hardlink_count = read_u32(bytes, &mut offset) as i32;
        let default_protection_class =
            ProtectionClass::from_u32(read_u32(bytes, &mut offset)).unwrap();
        let write_generation_counter = read_u32(bytes, &mut offset);
        let bsd_flags = read_u32(bytes, &mut offset);

        let uid = read_u32(bytes, &mut offset);
        let gid = read_u32(bytes, &mut offset);
        let mode = read_u16(bytes, &mut offset);

        let pad1 = read_u16(bytes, &mut offset);
        let pad2 = read_u64(bytes, &mut offset);

        assert_eq!(offset, Self::LEN);

        let ty = InodeType::from_u16(mode & InodeType::BITMASK).unwrap();
        let permissions = mode & !InodeType::BITMASK;

        let extended_fields = if bytes.len() > Self::LEN {
            Some(Xblob::parse(&bytes[Self::LEN..]))
        } else {
            None
        };

        Self {
            parent_id,
            private_id,

            create_time,
            modification_time,
            change_time,
            access_time,

            internal_flags,
            children_or_hardlink_count: match ty {
                InodeType::Dir => {
                    ChildrenOrHardlinkCount::ChildrenCount(children_or_hardlink_count)
                }
                _ => ChildrenOrHardlinkCount::HardlinkCount(children_or_hardlink_count),
            },
            default_protection_class,
            write_generation_counter,
            bsd_flags,

            uid,
            gid,

            ty,
            permissions,

            pad1,
            pad2,

            extended_fields,
        }
    }
    fn datastream(&self) -> Option<&JDatastream> {
        if let Some(ext) = self.extended_fields.as_ref() {
            ext.fields
                .iter()
                .find(|(k, _)| k.ty == InodeXfieldType::Dstream)
                .map(|(_, v)| v.as_dstream().unwrap())
        } else {
            None
        }
    }
    pub fn rdev(&self) -> u32 {
        self.extended_fields
            .as_ref()
            .map(|ext| {
                ext.fields
                    .iter()
                    .find(|(k, _)| k.ty == InodeXfieldType::Rdev)
                    .map(|(_, v)| v.as_rdev().unwrap())
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }
    pub fn size(&self) -> u64 {
        self.datastream().map(|dstream| dstream.size).unwrap_or(0)
    }
    pub fn block_count(&self, block_size: u32) -> u64 {
        self.datastream()
            .map(|dstream| dstream.allocated_size / u64::from(block_size))
            .unwrap_or(0)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct JDrecKey {
    pub header: JKey,
    pub name: String,
}

impl JDrecKey {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        let header = read_jkey(bytes, &mut offset);
        let name_len = read_u16(bytes, &mut offset) as usize;

        let name = String::from_utf8(bytes[offset..offset + (name_len - 1)].to_owned()).unwrap();

        Self { header, name }
    }
    pub fn new(oid: ObjectIdentifier, name: String) -> Self {
        Self {
            header: JKey {
                oid,
                ty: JObjType::DirRecord,
            },
            name,
        }
    }
    pub fn partial(oid: ObjectIdentifier) -> Self {
        Self::new(oid, String::new())
    }
}

impl Ord for JDrecKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.header, &with.header).then(Ord::cmp(&self.name, &with.name))
    }
}
impl PartialOrd for JDrecKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct JDrecHashedKey {
    pub header: JKey,
    pub hash: u32,
    pub name: String,
}
impl JDrecHashedKey {
    pub fn new(oid: ObjectIdentifier, name: String) -> Self {
        Self {
            header: JKey {
                oid,
                ty: JObjType::DirRecord,
            },
            hash: 0,
            name,
        }
    }
    pub fn partial(oid: ObjectIdentifier) -> Self {
        Self::new(oid, String::new())
    }
}

impl Ord for JDrecHashedKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.header, &with.header).then(Ord::cmp(&self.name, &with.name))
    }
}
impl PartialOrd for JDrecHashedKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

impl JDrecHashedKey {
    const LEN_MASK: u32 = 0x0000_03FF;
    const HASH_MASK: u32 = 0xFFFF_F400;
    const HASH_SHIFT: u32 = 10;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        let header = read_jkey(bytes, &mut offset);
        let name_len_and_hash = read_u32(bytes, &mut offset);

        let name_len = (name_len_and_hash & Self::LEN_MASK) as usize;
        let hash = (name_len_and_hash & Self::HASH_MASK) >> Self::HASH_SHIFT;

        let name = String::from_utf8(bytes[offset..offset + (name_len - 1)].to_owned()).unwrap();

        Self { header, name, hash }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JDrecVal {
    pub file_id: u64,
    pub date_added: u64,
    pub flags: JDrecTypeAndFlags,

    extended_fields: Option<Xblob<DrecXfieldType>>,
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum JDrecType {
        Unknown = 0,
        Fifo = 1,
        CharDev = 2,
        Dir = 4,
        BlockDev = 6,
        File = 8,
        Symlink = 10,
        Socket = 12,
        Whiteout = 14,
    }
}

bitflags! {
    pub struct JDrecFlags: u16 {
        const RESERVED = 0x10;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JDrecTypeAndFlags {
    pub ty: JDrecType,
    pub flags: JDrecFlags,
}
impl JDrecTypeAndFlags {
    const TYPE_MASK: u16 = 0x000F;

    pub fn from_raw(raw: u16) -> Self {
        Self {
            ty: JDrecType::from_u16(raw & Self::TYPE_MASK).unwrap(),
            flags: JDrecFlags::from_bits(raw & !Self::TYPE_MASK).unwrap(),
        }
    }
}

impl JDrecVal {
    const LEN: usize = 18;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let extended_fields = if bytes.len() > Self::LEN {
            Some(Xblob::parse(&bytes[Self::LEN..]))
        } else {
            None
        };

        Self {
            file_id: read_u64(bytes, &mut offset),
            date_added: read_u64(bytes, &mut offset),
            flags: JDrecTypeAndFlags::from_raw(read_u16(bytes, &mut offset)),
            extended_fields,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JDirStatsKey {
    pub header: JKey,
}

#[derive(Debug)]
pub struct JDirStatsVal {
    pub children_count: u64,
    pub total_size: u64,
    pub chained_key: u64,
    pub gen_count: u64,
}

impl JDirStatsVal {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            children_count: read_u64(bytes, &mut offset),
            total_size: read_u64(bytes, &mut offset),
            chained_key: read_u64(bytes, &mut offset),
            gen_count: read_u64(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct JXattrKey {
    pub header: JKey,
    pub name: String,
}

impl JXattrKey {
    pub const SYMLINK_XATTR_NAME: &'static str = "com.apple.fs.symlink";

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let header = read_jkey(bytes, &mut offset);
        let name_length = read_u16(bytes, &mut offset) as usize;
        let name = String::from_utf8(bytes[offset..offset + (name_length - 1)].to_owned()).unwrap();

        Self { header, name }
    }
    pub fn new(oid: ObjectIdentifier, name: String) -> Self {
        Self {
            header: JKey {
                oid,
                ty: JObjType::Xattr,
            },
            name,
        }
    }
    pub fn partial(oid: ObjectIdentifier) -> Self {
        Self::new(oid, String::new())
    }
}

impl Ord for JXattrKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.header, &with.header).then(Ord::cmp(&self.name, &with.name))
    }
}
impl PartialOrd for JXattrKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JXattrVal {
    pub flags: XattrFlags,
    pub len: u16,
    pub embedded: Option<Vec<u8>>,
    // TODO: streams
}

bitflags! {
    pub struct XattrFlags: u16 {
        const DATA_STREAM = 0x1;
        const DATA_EMBEDDED = 0x2;
        const FILE_SYSTEM_OWNED = 0x4;
        const RESERVED = 0x8;
    }
}

impl JXattrVal {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let flags = XattrFlags::from_bits(read_u16(bytes, &mut offset)).unwrap();
        let len = read_u16(bytes, &mut offset);

        let embedded = if flags.contains(XattrFlags::DATA_EMBEDDED) {
            Some(bytes[offset..offset + len as usize].to_owned())
        } else {
            None
        };

        Self {
            flags,
            len,
            embedded,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JDatastreamIdKey {
    pub header: JKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JDatastreamIdVal {
    pub reference_count: u32,
}
impl JDatastreamIdVal {
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            reference_count: read_u32(bytes, &mut 0),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JFileExtentKey {
    pub header: JKey,
    pub logical_addr: u64,
}

impl JFileExtentKey {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            header: read_jkey(bytes, &mut offset),
            logical_addr: read_u64(bytes, &mut offset),
        }
    }
    pub fn new(oid: ObjectIdentifier, logical_addr: u64) -> Self {
        Self {
            header: JKey {
                oid,
                ty: JObjType::FileExtent,
            },
            logical_addr,
        }
    }
    pub fn partial(oid: ObjectIdentifier) -> Self {
        Self::new(oid, 0)
    }
}

impl Ord for JFileExtentKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.header, &with.header).then(Ord::cmp(&self.logical_addr, &with.logical_addr))
    }
}
impl PartialOrd for JFileExtentKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JFileExtentVal {
    pub length: u64,
    pub flags: (),
    pub physical_block_num: u64,
    pub crypto_id: u64,
}

impl JFileExtentVal {
    const LEN_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let length_and_flags = read_u64(bytes, &mut offset);
        let physical_block_num = read_u64(bytes, &mut offset);
        let crypto_id = read_u64(bytes, &mut offset);

        Self {
            length: length_and_flags & Self::LEN_MASK,
            flags: (), // Currently no defined flags.
            physical_block_num,
            crypto_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JDatastream {
    pub size: u64,
    pub allocated_size: u64,
    pub default_crypto_id: u64,
    pub total_bytes_written: u64,
    pub total_bytes_read: u64,
}

impl JDatastream {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            size: read_u64(bytes, &mut offset),
            allocated_size: read_u64(bytes, &mut offset),
            default_crypto_id: read_u64(bytes, &mut offset),
            total_bytes_read: read_u64(bytes, &mut offset),
            total_bytes_written: read_u64(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Xblob<T> {
    pub ext_count: u16,
    pub ext_bytes_used: u16,
    pub fields: Vec<(Xfield<T>, XfieldValue)>,
}

impl<T: XfieldType> Xblob<T> {
    fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        let xfield_len = Xfield::<()>::LEN;

        let ext_count = read_u16(bytes, &mut offset);
        let ext_bytes_used = read_u16(bytes, &mut offset);

        let metadata_bytes = ext_count as usize * xfield_len;

        let metadata_offset = offset;
        let mut values_offset = offset + metadata_bytes;

        let fields = (0..ext_count as usize)
            .map(|i| Xfield::parse(&bytes, metadata_offset + i * xfield_len, &mut values_offset))
            .collect();

        Self {
            ext_count,
            ext_bytes_used,
            fields,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Xfield<T> {
    pub ty: T,
    pub flags: XfieldFlags,
    pub size: u16,
}

bitflags! {
    pub struct XfieldFlags: u8 {
        const DATA_DEPENDENT = 0x01;
        const DONT_COPY = 0x02;
        const RESERVED4 = 0x04;
        const CHILDREN_INHERIT = 0x08;
        const USER_FIELD = 0x10;
        const SYSTEM_FIELD = 0x20;
        const RESERVED_40 = 0x40;
        const RESERVED_80 = 0x80;
    }
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum DrecXfieldType {
        SiblingId = 1,
    }
}
enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum InodeXfieldType {
        SnapshotXid = 1,
        DeltaTreeOid = 2,
        DocumentId = 3,
        Name = 4,
        PreviousFilesize = 5,
        Reserved6 = 6,
        FinderInfo = 7, // lol
        Dstream = 8,
        Reserved9 = 9,
        DirStatsKey = 10,
        FsUuid = 11,
        Reserved12 = 12,
        SparseBytes = 13,
        Rdev = 14,
    }
}

impl XfieldType for InodeXfieldType {
    fn parse(bytes: &[u8], ty: Self) -> XfieldValue {
        match ty {
            InodeXfieldType::SnapshotXid => XfieldValue::SnapshotXid(fal::read_u64(bytes, 0)),
            InodeXfieldType::DeltaTreeOid => {
                XfieldValue::DeltaTreeOid(fal::read_u64(bytes, 0).into())
            }
            InodeXfieldType::Name => {
                XfieldValue::Name(String::from_utf8(bytes[..bytes.len() - 1].to_owned()).unwrap())
            }
            InodeXfieldType::Dstream => XfieldValue::Dstream(JDatastream::parse(bytes)),
            InodeXfieldType::Rdev => XfieldValue::Rdev(fal::read_u32(bytes, 0)),
            other => unimplemented!("Xattr type {:?}", other),
        }
    }
}

impl XfieldType for DrecXfieldType {
    fn parse(bytes: &[u8], ty: Self) -> XfieldValue {
        match ty {
            DrecXfieldType::SiblingId => XfieldValue::SiblingId(fal::read_u64(bytes, 0)),
        }
    }
}

pub trait XfieldType: FromPrimitive + std::fmt::Debug + Copy {
    fn parse(bytes: &[u8], ty: Self) -> XfieldValue;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XfieldValue {
    SiblingId(u64),
    SnapshotXid(TransactionIdentifier),
    Name(String),
    DeltaTreeOid(ObjectIdentifier),
    Dstream(JDatastream),
    Rdev(u32),
}

impl XfieldValue {
    pub fn as_dstream(&self) -> Option<&JDatastream> {
        match self {
            Self::Dstream(d) => Some(d),
            _ => None,
        }
    }
    pub fn as_rdev(&self) -> Option<u32> {
        match self {
            &Self::Rdev(d) => Some(d),
            _ => None,
        }
    }
}

impl<T> Xfield<T> {
    pub const LEN: usize = 4;
}
impl<T: XfieldType> Xfield<T> {
    pub fn parse(bytes: &[u8], key_offset: usize, value_offset: &mut usize) -> (Self, XfieldValue) {
        let mut offset = key_offset;

        let key = Self {
            ty: T::from_u8(read_u8(bytes, &mut offset)).unwrap(),
            flags: XfieldFlags::from_bits(read_u8(bytes, &mut offset)).unwrap(),
            size: read_u16(bytes, &mut offset),
        };
        let value = XfieldType::parse(
            &bytes[*value_offset..*value_offset + key.size as usize],
            key.ty,
        );
        *value_offset += fal::align(key.size as usize, 8);
        (key, value)
    }
}

pub fn read_jkey(bytes: &[u8], offset: &mut usize) -> JKey {
    let jkey = JKey::parse(&bytes[..JKey::LEN]);
    *offset += JKey::LEN;
    jkey
}

impl BTreeKey {
    pub fn parse_jkey(bytes: &[u8]) -> Self {
        let base = JKey::parse(bytes);

        Self::FsLayerKey(match base.ty {
            JObjType::Inode => JAnyKey::InodeKey(JInodeKey { header: base }),
            JObjType::DirRecord => {
                let base_name_len = fal::read_u16(bytes, 8); // after the 8-byte header
                if 10 + base_name_len as usize == bytes.len() {
                    JAnyKey::DrecKey(JDrecKey::parse(bytes))
                } else {
                    JAnyKey::DrecHashedKey(JDrecHashedKey::parse(bytes))
                }
            }
            JObjType::DataStreamId => JAnyKey::DatastreamIdKey(JDatastreamIdKey { header: base }),
            JObjType::FileExtent => JAnyKey::FileExtentKey(JFileExtentKey::parse(bytes)),
            JObjType::Xattr => JAnyKey::XattrKey(JXattrKey::parse(bytes)),
            other => unimplemented!("{:?}", other),
        })
    }
}
impl BTreeValue {
    pub fn parse_from_jkey(jkey: &JKey, bytes: &[u8]) -> Self {
        match jkey.ty {
            JObjType::Inode => Self::Inode(JInodeVal::parse(bytes)),
            JObjType::DirRecord => Self::DirRecord(JDrecVal::parse(bytes)),
            JObjType::DataStreamId => Self::DatastreamId(JDatastreamIdVal::parse(bytes)),
            JObjType::FileExtent => Self::FileExtent(JFileExtentVal::parse(bytes)),
            JObjType::Xattr => Self::Xattr(JXattrVal::parse(bytes)),
            other => unimplemented!("value for key {:?}", other),
        }
    }
}
