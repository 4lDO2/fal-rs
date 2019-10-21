use crate::{superblock::{ChecksumType, Superblock}, DiskKey, read_timespec, Timespec};
use fal::parsing::{read_u8, read_u16, read_u32, read_u64, read_uuid, skip};

use bitflags::bitflags;
use enum_primitive::*;
use uuid::Uuid;

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ChunkItem {
    pub len: u64,
    pub owner: u64,

    pub stripe_length: u64,
    pub ty: BlockGroupType,

    pub io_alignment: u32,
    pub io_width: u32,

    pub sector_size: u32,
    pub stripe_count: u16,
    pub sub_stripe_count: u16,
    pub stripes: Box<[Stripe]>,
}

impl ChunkItem {
    pub const LEN: usize = 80;
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let len = read_u64(bytes, &mut offset);
        let owner = read_u64(bytes, &mut offset);

        let stripe_length = read_u64(bytes, &mut offset);
        let ty = BlockGroupType::from_bits(read_u64(bytes, &mut offset)).unwrap();

        let io_alignment = read_u32(bytes, &mut offset);
        let io_width = read_u32(bytes, &mut offset);

        let sector_size = read_u32(bytes, &mut offset);

        let stripe_count = read_u16(bytes, &mut offset);
        let sub_stripe_count = read_u16(bytes, &mut offset);

        let stripes = (0..stripe_count as usize).map(|i| Stripe::parse(&bytes[offset + i * Stripe::LEN..offset + (i + 1) * Stripe::LEN])).collect::<Vec<_>>().into_boxed_slice(); // FIXME: stripe_count & sub_stripe_count length.

        Self { len, owner, stripe_length, ty, io_alignment, io_width, sector_size, stripe_count, sub_stripe_count, stripes }
    }
    pub fn stripe(&self, superblock: &Superblock) -> &Stripe {
        self.stripes.iter().find(|stripe| &stripe.device_uuid == &superblock.device_properties.uuid).expect("Using the superblock of a different filesystem")
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Stripe {
    pub device_id: u64,
    pub offset: u64,
    pub device_uuid: uuid::Uuid,
}

impl Stripe {
    const LEN: usize = 32;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            device_id: read_u64(bytes, &mut offset),
            offset: read_u64(bytes, &mut offset),
            device_uuid: read_uuid(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DevItem {
    device_id: u64,
    total_bytes: u64,
    bytes_used: u64,
    io_alignment: u32,
    io_width: u32,
    sector_size: u32,
    ty: u64,
    generation: u64,
    start_offset: u64,
    device_group: u32,
    seek_speed: u8,
    bandwidth: u8,
    device_uuid: Uuid,
    fsid: Uuid,
}

impl DevItem {
    pub const LEN: usize = 98;
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            device_id: read_u64(bytes, &mut offset),
            total_bytes: read_u64(bytes, &mut offset),
            bytes_used: read_u64(bytes, &mut offset),
            io_alignment: read_u32(bytes, &mut offset),
            io_width: read_u32(bytes, &mut offset),
            sector_size: read_u32(bytes, &mut offset),
            ty: read_u64(bytes, &mut offset),
            generation: read_u64(bytes, &mut offset),
            start_offset: read_u64(bytes, &mut offset),
            device_group: read_u32(bytes, &mut offset),
            seek_speed: read_u8(bytes, &mut offset),
            bandwidth: read_u8(bytes, &mut offset),
            device_uuid: read_uuid(bytes, &mut offset),
            fsid: read_uuid(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RootItem {
    pub inode_item: InodeItem,
    pub generation: u64,
    pub root_directory_id: u64,
    pub addr: u64,
    pub byte_limit: u64,
    pub bytes_used: u64,
    pub last_snapshot: u64,
    pub flags: RootItemFlags,
    pub refs: u32,

    pub drop_progress: DiskKey,
    pub drop_level: u8,
    pub level: u8,

    pub generation_v2: u64,
    pub uuid: Uuid,
    pub parent_uuid: Uuid,
    pub received_uuid: Uuid,

    pub c_xid: u64,
    pub o_xid: u64,
    pub s_xid: u64,
    pub r_xid: u64,

    pub ctime: Timespec,
    pub otime: Timespec,
    pub stime: Timespec,
    pub rtime: Timespec,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InodeItem {
    pub generation: u64,
    pub transaction_id: u64,
    pub size: u64,
    pub byte_count: u64,
    pub block_group: u64,
    pub hardlink_count: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub rdev: u64,
    pub flags: InodeFlags,
    pub sequence: u64,

    // 4 reserved u64s.

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

impl InodeItem {
    pub const LEN: usize = 160;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            generation: read_u64(bytes, &mut offset),
            transaction_id: read_u64(bytes, &mut offset),
            size: read_u64(bytes, &mut offset),
            byte_count: read_u64(bytes, &mut offset),
            block_group: read_u64(bytes, &mut offset),
            hardlink_count: read_u32(bytes, &mut offset),
            uid: read_u32(bytes, &mut offset),
            gid: read_u32(bytes, &mut offset),
            mode: read_u32(bytes, &mut offset),
            rdev: read_u64(bytes, &mut offset),
            flags: InodeFlags::from_bits_truncate(read_u64(bytes, &mut offset)),
            sequence: read_u64(bytes, &mut offset),

            atime: read_timespec(bytes, skip(&mut offset, 32)),
            ctime: read_timespec(bytes, &mut offset),
            mtime: read_timespec(bytes, &mut offset),
            otime: read_timespec(bytes, &mut offset),
        }
    }
}

impl RootItem {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = InodeItem::LEN;

        Self {
            inode_item: InodeItem::parse(&bytes[..InodeItem::LEN]),

            generation: read_u64(bytes, &mut offset),
            root_directory_id: read_u64(bytes, &mut offset),
            addr: read_u64(bytes, &mut offset),
            byte_limit: read_u64(bytes, &mut offset),
            bytes_used: read_u64(bytes, &mut offset),
            last_snapshot: read_u64(bytes, &mut offset),
            flags: RootItemFlags::from_bits(read_u64(bytes, &mut offset)).unwrap(),
            refs: read_u32(bytes, &mut offset),

            drop_progress: DiskKey::parse(&bytes[offset..offset + 17]),
            drop_level: read_u8(bytes, skip(&mut offset, 17)),
            level: read_u8(bytes, &mut offset),

            generation_v2: read_u64(bytes, &mut offset),
            uuid: read_uuid(bytes, &mut offset),
            parent_uuid: read_uuid(bytes, &mut offset),
            received_uuid: read_uuid(bytes, &mut offset),

            c_xid: read_u64(bytes, &mut offset),
            o_xid: read_u64(bytes, &mut offset),
            s_xid: read_u64(bytes, &mut offset),
            r_xid: read_u64(bytes, &mut offset),

            ctime: read_timespec(bytes, &mut offset),
            otime: read_timespec(bytes, &mut offset),
            stime: read_timespec(bytes, &mut offset),
            rtime: read_timespec(bytes, &mut offset),

            // 8 reserved u64s
        }
    }
}

#[derive(Clone, Debug)]
pub struct InodeRef {
    pub index: u64,
    pub name_len: u16,
    pub name: Vec<u8>, // TODO: Find a good cross-platform unix OsString lib.
}
impl InodeRef {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let index = read_u64(bytes, &mut offset);
        let name_len = read_u16(bytes, &mut offset);

        Self {
            index,
            name_len,
            name: bytes[offset..offset + name_len as usize].to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RootRef {
    pub subtree_id: u64,
    pub sequence: u64,
    pub name_len: u16,
}

impl RootRef {
    pub const LEN: usize = 18;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            subtree_id: read_u64(bytes, &mut 0),
            sequence: read_u64(bytes, &mut 8),
            name_len: read_u16(bytes, &mut 16),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirItem {
    pub location: DiskKey,
    pub xid: u64,
    pub data_len: u16,
    pub name_len: u16,
    pub ty: Filetype,
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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
}

impl DirItem {
    pub const LEN: usize = DiskKey::LEN + 13;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = DiskKey::LEN;
        Self {
            location: DiskKey::parse(&bytes[..DiskKey::LEN]),
            xid: read_u64(bytes, &mut offset),
            data_len: read_u16(bytes, &mut offset),
            name_len: read_u16(bytes, &mut offset),
            ty: Filetype::from_u8(read_u8(bytes, &mut offset)).unwrap(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileExtentItem {
    pub generation: u64,
    pub device_size: u64,
    pub compression: u8,
    pub encryption: u8,
    pub other_encoding: u16,
    pub ty: FileExtentItemType,

    pub extension: FileExtentItemExtension,
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug)]
    pub enum FileExtentItemType {
        Inline = 0,
        Reg = 1,
        Prealloc = 2,
    }
}

#[derive(Clone, Debug)]
pub enum FileExtentItemExtension {
    OnDisk {
        disk_bytenr: u64,
        disk_byte_count: u64,
        disk_offset: u64,
        byte_count: u64,
    },
    Inline(Vec<u8>),
}

impl FileExtentItem {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let generation = read_u64(bytes, &mut offset);
        let device_size = read_u64(bytes, &mut offset);
        let compression = read_u8(bytes, &mut offset);
        let encryption = read_u8(bytes, &mut offset);
        let other_encoding = read_u16(bytes, &mut offset);
        let ty = FileExtentItemType::from_u8(read_u8(bytes, &mut offset)).unwrap();

        Self {
            generation,
            device_size,
            compression,
            encryption,
            other_encoding,
            ty,

            extension: match ty {
                FileExtentItemType::Reg | FileExtentItemType::Prealloc => FileExtentItemExtension::OnDisk {
                    disk_bytenr: read_u64(bytes, &mut offset),
                    disk_byte_count: read_u64(bytes, &mut offset),
                    disk_offset: read_u64(bytes, &mut offset),
                    byte_count: read_u64(bytes, &mut offset),
                },
                FileExtentItemType::Inline => FileExtentItemExtension::Inline(bytes[offset..offset + device_size as usize].to_owned()),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BlockGroupItem {
    pub bytes_used: u64,
    pub chunk_oid: u64,
    pub flags: BlockGroupType,
}

impl BlockGroupItem {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            bytes_used: read_u64(bytes, &mut offset),
            chunk_oid: read_u64(bytes, &mut offset),
            flags: BlockGroupType::from_bits(read_u64(bytes, &mut offset)).unwrap(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExtentItem {
    pub reference_count: u64,
    pub allocation_xid: u64,
    pub flags: ExtentFlags,
}

bitflags! {
    pub struct ExtentFlags: u64 {
        const DATA = 0x1;
        const TREE_BLOCK = 0x2;
        const FULL_BACKREF = 0x80;
    }
}

impl ExtentItem {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            reference_count: read_u64(bytes, &mut offset),
            allocation_xid: read_u64(bytes, &mut offset),
            flags: ExtentFlags::from_bits(read_u64(bytes, &mut offset)).unwrap(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DevStatsItem {
    pub write_errs: u64,
    pub read_errs: u64,
    pub flush_errs: u64,

    pub corruption_errs: u64,
    pub generation_errs: u64,
}

impl DevStatsItem {
    pub const LEN: usize = 48;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            write_errs: read_u64(bytes, &mut offset),
            read_errs: read_u64(bytes, &mut offset),
            flush_errs: read_u64(bytes, &mut offset),

            corruption_errs: read_u64(bytes, &mut offset),
            generation_errs: read_u64(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DevExtent {
    pub chunk_tree: u64,
    pub chunk_oid: u64,
    pub chunk_offset: u64,
    pub len: u64,
    pub chunk_tree_uuid: Uuid,
}

impl DevExtent {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            chunk_tree: read_u64(bytes, &mut offset),
            chunk_oid: read_u64(bytes, &mut offset),
            chunk_offset: read_u64(bytes, &mut offset),
            len: read_u64(bytes, &mut offset),
            chunk_tree_uuid: read_uuid(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CsumItem {
    checksums: Checksums,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Checksums {
    Crc32(Vec<u32>),
}

impl CsumItem {
    pub fn parse(checksum_type: ChecksumType, bytes: &[u8]) -> Self {
        Self {
            checksums: match checksum_type {
                ChecksumType::Crc32 => Checksums::Crc32({
                    assert_eq!(bytes.len() % std::mem::size_of::<u32>(), 0);
                    (0..bytes.len() / 4).map(|i| fal::read_u32(bytes, i * 4)).collect()
                })
            },
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct UuidItem {
    // Based on print-io.c from the btrfs source code. It seems like UUID items are simply an array
    // of subvolume ids.
    pub subvolumes: Vec<u64>,
}

impl UuidItem {
    pub fn parse(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len() % std::mem::size_of::<u64>(), 0);
        Self {
            subvolumes: (0..bytes.len() / 8).map(|i| fal::read_u64(bytes, i)).collect(),
        }
    }
}
