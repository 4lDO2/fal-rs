use fal::{read_u32, read_u64};

use crate::superblock::{ObjectIdentifier, ObjPhys, TransactionIdentifier};

#[derive(Debug)]
pub struct ReaperPhys {
    header: ObjPhys,
    next_reaper_id: u64,
    completed_id: u64,
    head: ObjectIdentifier,
    tail: ObjectIdentifier,
    flags: u32,
    rlcount: u32,
    ty: u32,
    size: u32,
    fs_oid: ObjectIdentifier,
    oid: ObjectIdentifier,
    xid: TransactionIdentifier,
    nrle_flags: u32,
    state_buffer_size: u32,
    state_buffer: Vec<u8>,
}

impl ReaperPhys {
    pub fn parse(bytes: &[u8]) -> Self {
        let state_buffer_size = read_u32(bytes, 108);
        Self {
            header: ObjPhys::parse(&bytes[..32]),
            next_reaper_id: read_u64(bytes, 32),
            completed_id: read_u64(bytes, 40),
            head: read_u64(bytes, 48).into(),
            tail: read_u64(bytes, 56).into(),
            flags: read_u32(bytes, 64),
            rlcount: read_u32(bytes, 68),
            ty: read_u32(bytes, 72),
            size: read_u32(bytes, 76),
            fs_oid: read_u64(bytes, 80).into(),
            oid: read_u64(bytes, 88).into(),
            xid: read_u64(bytes, 96),
            nrle_flags: read_u32(bytes, 104),
            state_buffer_size,
            state_buffer: bytes[112..112 + state_buffer_size as usize].to_owned(),
        }
    }
}
