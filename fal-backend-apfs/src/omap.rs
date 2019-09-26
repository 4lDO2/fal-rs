use std::cmp::Ordering;
use fal::{read_u32, read_u64};
use crate::{BlockAddr, ObjectIdentifier, ObjPhys, TransactionIdentifier};

#[derive(Debug)]
pub struct OmapPhys {
    pub header: ObjPhys,
    pub flags: u32,
    pub snapshot_count: u32,
    pub tree_type: u32,
    pub snapshot_tree_type: u32,
    pub tree_oid: ObjectIdentifier,
    pub snapshot_tree_oid: ObjectIdentifier,
    pub most_recent_snapshot: TransactionIdentifier,
    pub pending_revert_min: TransactionIdentifier,
    pub pending_revert_max: TransactionIdentifier,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OmapKey {
    pub oid: ObjectIdentifier,
    pub xid: TransactionIdentifier,
}

impl OmapKey {
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            oid: read_u64(bytes, 0).into(),
            xid: read_u64(bytes, 8),
        }
    }
}

impl PartialOrd for OmapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.oid.cmp(&other.oid) {
            Ordering::Equal => Some(self.xid.cmp(&other.xid)),
            other_ordering => Some(other_ordering),
        }
    }
}
impl Ord for OmapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OmapValue {
    pub flags: u32,
    pub size: u32,
    pub paddr: BlockAddr,
}

impl OmapValue {
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            flags: read_u32(bytes, 0),
            size: read_u32(bytes, 4),
            paddr: read_u64(bytes, 8) as i64,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OmapSnapshot {
    pub flags: u32,
    pub padding: u32,
    pub oid: ObjectIdentifier,
}

impl OmapPhys {
    pub fn parse(bytes: &[u8]) -> Self {
        let header = ObjPhys::parse(&bytes[..32]);

        Self {
            header,
            flags: read_u32(bytes, 32),
            snapshot_count: read_u32(bytes, 36),
            tree_type: read_u32(bytes, 40),
            snapshot_tree_type: read_u32(bytes, 44),
            tree_oid: read_u64(bytes, 48).into(),
            snapshot_tree_oid: read_u64(bytes, 56).into(),
            most_recent_snapshot: read_u64(bytes, 64),
            pending_revert_min: read_u64(bytes, 72),
            pending_revert_max: read_u64(bytes, 80),
        }
    }
}
