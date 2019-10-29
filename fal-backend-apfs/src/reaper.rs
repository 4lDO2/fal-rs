use fal::{read_u32, read_u64};

use crate::{ObjPhys, ObjectIdentifier, TransactionIdentifier};

#[derive(Debug)]
pub struct ReaperPhys {
    pub header: ObjPhys,
    pub next_reaper_id: u64,
    pub completed_id: u64,
    pub head: ObjectIdentifier,
    pub tail: ObjectIdentifier,
    pub flags: u32,
    pub rlcount: u32,
    pub ty: u32,
    pub size: u32,
    pub fs_oid: ObjectIdentifier,
    pub oid: ObjectIdentifier,
    pub xid: TransactionIdentifier,
    pub nrle_flags: u32,
    pub state_buffer_size: u32,
    pub state_buffer: Vec<u8>,
}

impl ReaperPhys {
    pub fn parse(bytes: &[u8]) -> Self {
        let state_buffer_size = read_u32(bytes, 108);
        Self {
            header: ObjPhys::parse(&bytes),
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
