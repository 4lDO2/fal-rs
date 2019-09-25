use std::ops::Range;

use fal::{read_u16, read_u32, read_u64};

use crate::superblock::{BlockAddr, ObjectIdentifier, ObjPhys, TransactionIdentifier};

#[derive(Debug)]
pub struct SpacemanagerDevice {
    block_count: u64,
    chunk_count: u64,
    cib_count: u32,
    cab_count: u32,
    free_count: u64,
    addr_offset: u32,
    reserved: u32,
    reserved2: u64,
}
impl SpacemanagerDevice {
    pub const LEN: usize = 48;
    pub const DEVICES_PER_SPACEMAN: usize = 2;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            block_count: read_u64(bytes, 0),
            chunk_count: read_u64(bytes, 8),
            cib_count: read_u32(bytes, 16),
            cab_count: read_u32(bytes, 20),
            free_count: read_u64(bytes, 24),
            addr_offset: read_u32(bytes, 32),
            reserved: read_u32(bytes, 36),
            reserved2: read_u64(bytes, 40),
        }
    }
}

#[derive(Debug)]
pub struct SpacemanagerFreeQueue {
    count: u64,
    tree_oid: ObjectIdentifier,
    oldest_xid: TransactionIdentifier,
    tree_node_limit: u16,
    pad16: u16,
    pad32: u32,
    reserved: u64,
}

impl SpacemanagerFreeQueue {
    pub const LEN: usize = 40;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            count: read_u64(bytes, 0),
            tree_oid: read_u64(bytes, 8).into(),
            oldest_xid: read_u64(bytes, 16),
            tree_node_limit: read_u16(bytes, 24),
            pad16: read_u16(bytes, 26),
            pad32: read_u32(bytes, 28),
            reserved: read_u64(bytes, 32),
        }
    }
}

pub type SpacemanagerAllocZoneBoundaries = Range<u64>;

#[derive(Debug)]
pub struct SpacemanagerAllocZoneInfoPhys {
    current_boundaries: SpacemanagerAllocZoneBoundaries,
    previous_boundaries: Vec<SpacemanagerAllocZoneBoundaries>,
    zone_id: u16,
    previous_boundary_idx: u16,
    reserved: u32,
}

impl SpacemanagerAllocZoneInfoPhys {
    pub const LEN: usize = 136;
    pub fn parse(bytes: &[u8]) -> Self {
        const PREVIOUS_BOUNDARIES_COUNT: usize = 7;

        // This collection starts at 16, and ends at 16 + 7 * 16 = 128.
        let previous_boundaries = (0..PREVIOUS_BOUNDARIES_COUNT).map(|i| read_u64(bytes, 16 + i * 16 + 0)..read_u64(bytes, 16 + i * 16 + 8)).collect();

        Self {
            current_boundaries: read_u64(bytes, 0)..read_u64(bytes, 8),
            previous_boundaries,
            zone_id: read_u16(bytes, 128),
            previous_boundary_idx: read_u16(bytes, 130),
            reserved: read_u32(bytes, 132),
        }
    }
}

#[derive(Debug)]
pub struct SpacemanagerDatazoneInfoPhys {
    allocation_zones: Vec<SpacemanagerAllocZoneInfoPhys>,
}

impl SpacemanagerDatazoneInfoPhys {
    pub const ALLOCATION_ZONE_COUNT: usize = 8;
    pub const LEN: usize = Self::ALLOCATION_ZONE_COUNT * SpacemanagerAllocZoneInfoPhys::LEN;
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            allocation_zones: (0..SpacemanagerPhys::DEVICE_COUNT * Self::ALLOCATION_ZONE_COUNT).map(|i| SpacemanagerAllocZoneInfoPhys::parse(&bytes[i * SpacemanagerAllocZoneInfoPhys::LEN .. (i + 1) * SpacemanagerAllocZoneInfoPhys::LEN])).collect(),
        }
    }
}

#[derive(Debug)]
pub struct SpacemanagerPhys {
    pub header: ObjPhys,
    pub block_size: u32,
    pub blocks_per_chunk: u32,
    pub chunks_per_cib: u32,
    pub cibs_per_cab: u32,
    pub devices: Vec<SpacemanagerDevice>,
    pub flags: u32,
    pub ip_bm_tx_multiplier: u32,
    pub ip_block_count: u64,
    pub ip_bm_size_in_blocks: u32,
    pub ip_bm_block_count: u32,
    pub ip_bm_base: BlockAddr,
    pub ip_base: BlockAddr,
    pub fs_reserve_block_count: u64,
    pub fs_reserve_alloc_count: u64,
    pub free_queues: Vec<SpacemanagerFreeQueue>,
    pub ip_bm_free_head: u16,
    pub ip_bm_free_tail: u16,
    pub ip_bm_xid_offset: u32,
    pub ip_bitmap_offset: u32,
    pub ip_bm_free_next_offset: u32,
    pub version: u32,
    pub struct_size: u32,
    pub datazone: SpacemanagerDatazoneInfoPhys,
}

impl SpacemanagerPhys {
    pub const DEVICE_COUNT: usize = 2;
    pub fn parse(bytes: &[u8]) -> Self {
        let header = ObjPhys::parse(bytes);

        // The devices field starts at 48, and since SD_COUNT = 2 and LEN = 48, it stops at 144.
        let devices = (0..Self::DEVICE_COUNT).map(|i| SpacemanagerDevice::parse(&bytes[48 + i * 48..48 + (i + 1) * 48])).collect();

        // The free queues field starts at 200, and since SFQ_COUNT = 3 and LEN = 40, it stops at
        // 320.
        let free_queues = (0..3).map(|i| SpacemanagerFreeQueue::parse(&bytes[200 + i * 40 .. 200 + (i + 1) * 40])).collect();

        Self {
            header,
            block_size: read_u32(bytes, 32),
            blocks_per_chunk: read_u32(bytes, 36),
            chunks_per_cib: read_u32(bytes, 40),
            cibs_per_cab: read_u32(bytes, 44),
            devices,
            flags: read_u32(bytes, 144),
            ip_bm_tx_multiplier: read_u32(bytes, 148),
            ip_block_count: read_u64(bytes, 152),
            ip_bm_size_in_blocks: read_u32(bytes, 160),
            ip_bm_block_count: read_u32(bytes, 164),
            ip_bm_base: read_u64(bytes, 168) as i64,
            ip_base: read_u64(bytes, 176) as i64,
            fs_reserve_block_count: read_u64(bytes, 184),
            fs_reserve_alloc_count: read_u64(bytes, 192),
            free_queues,
            ip_bm_free_head: read_u16(bytes, 320),
            ip_bm_free_tail: read_u16(bytes, 322),
            ip_bm_xid_offset: read_u32(bytes, 324),
            ip_bitmap_offset: read_u32(bytes, 328),
            ip_bm_free_next_offset: read_u32(bytes, 332),
            version: read_u32(bytes, 336),
            struct_size: read_u32(bytes, 340),
            datazone: SpacemanagerDatazoneInfoPhys::parse(&bytes[344..]),
        }
    }
}
