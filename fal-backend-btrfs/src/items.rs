use crate::{DiskKey, read_timespec, Timespec};
use fal::{read_u8, read_u16, read_u32, read_u64, read_uuid};

use bitflags::bitflags;
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
        let stripe_count = read_u16(bytes, 44);
        let sub_stripe_count = read_u16(bytes, 46);

        Self {
            len: read_u64(bytes, 0),
            owner: read_u64(bytes, 8),

            stripe_length: read_u64(bytes, 16),
            ty: BlockGroupType::from_bits(read_u64(bytes, 24)).unwrap(),

            io_alignment: read_u32(bytes, 32),
            io_width: read_u32(bytes, 36),

            sector_size: read_u32(bytes, 40),

            stripe_count,
            sub_stripe_count,

            stripes: (0..stripe_count as usize).map(|i| Stripe::parse(&bytes[48 + i * Stripe::LEN..48 + (i + 1) * Stripe::LEN])).collect::<Vec<_>>().into_boxed_slice(), // FIXME: stripe_count & sub_stripe_count length.
        }
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
        Self {
            device_id: read_u64(bytes, 0),
            offset: read_u64(bytes, 8),
            device_uuid: read_uuid(bytes, 16),
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
        Self {
            device_id: read_u64(bytes, 0),
            total_bytes: read_u64(bytes, 8),
            bytes_used: read_u64(bytes, 16),
            io_alignment: read_u32(bytes, 24),
            io_width: read_u32(bytes, 28),
            sector_size: read_u32(bytes, 32),
            ty: read_u64(bytes, 36),
            generation: read_u64(bytes, 44),
            start_offset: read_u64(bytes, 52),
            device_group: read_u32(bytes, 60),
            seek_speed: read_u8(bytes, 64),
            bandwidth: read_u8(bytes, 65),
            device_uuid: read_uuid(bytes, 66),
            fsid: read_uuid(bytes, 82),
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
    pub flags: u64,
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
    pub flags: u64,
    pub sequence: u64,

    // 4 reserved u64s.

    pub atime: Timespec,
    pub ctime: Timespec,
    pub mtime: Timespec,
    pub otime: Timespec,
}

impl InodeItem {
    pub const LEN: usize = 152;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            generation: read_u64(bytes, 0),
            transaction_id: read_u64(bytes, 8),
            size: read_u64(bytes, 16),
            byte_count: read_u64(bytes, 24),
            block_group: read_u64(bytes, 32),
            hardlink_count: read_u32(bytes, 40),
            uid: read_u32(bytes, 44),
            gid: read_u32(bytes, 48),
            mode: read_u32(bytes, 52),
            rdev: read_u64(bytes, 56),
            flags: read_u64(bytes, 64),
            sequence: read_u64(bytes, 72),

            // 4 reserved u64s, new offset is 72 + 32 = 104.

            atime: read_timespec(bytes, 104),
            ctime: read_timespec(bytes, 116),
            mtime: read_timespec(bytes, 128),
            otime: read_timespec(bytes, 140),
        }
    }
}

impl RootItem {
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            inode_item: InodeItem::parse(&bytes[..152]),

            generation: read_u64(bytes, 152),
            root_directory_id: read_u64(bytes, 160),
            addr: read_u64(bytes, 168),
            byte_limit: read_u64(bytes, 176),
            bytes_used: read_u64(bytes, 184),
            last_snapshot: read_u64(bytes, 192),
            flags: read_u64(bytes, 200),
            refs: read_u32(bytes, 208),

            drop_progress: DiskKey::parse(&bytes[212..229]),
            drop_level: read_u8(bytes, 229),
            level: read_u8(bytes, 230),

            generation_v2: read_u64(bytes, 231),
            uuid: read_uuid(bytes, 239),
            parent_uuid: read_uuid(bytes, 271),
            received_uuid: read_uuid(bytes, 287),

            c_xid: read_u64(bytes, 303),
            o_xid: read_u64(bytes, 311),
            s_xid: read_u64(bytes, 319),
            r_xid: read_u64(bytes, 327),

            ctime: read_timespec(bytes, 335),
            otime: read_timespec(bytes, 347),
            stime: read_timespec(bytes, 359),
            rtime: read_timespec(bytes, 371),

            // 8 reserved u64s
        }
    }
}
