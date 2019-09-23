use fal::{read_u32, read_u64};

use crate::{filesystem::Filesystem, superblock::{BlockAddr, NxSuperblock, ObjectIdentifier, ObjPhys, ObjectType}};

#[derive(Debug)]
pub struct CheckpointMapping {
    ty: u32,
    subtype: u32,
    size: u32,
    padding: u32,
    fs_oid: ObjectIdentifier,
    oid: ObjectIdentifier,
    paddr: ObjectIdentifier,
}

#[derive(Debug)]
pub struct CheckpointMappingPhys {
    header: ObjPhys,
    flags: u32,
    count: u32,
    mappings: Box<[CheckpointMapping]>,
}

impl CheckpointMapping {
    pub const LEN: usize = 40;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            ty: read_u32(bytes, 0),
            subtype: read_u32(bytes, 4),
            size: read_u32(bytes, 8),
            padding: read_u32(bytes, 12),
            fs_oid: read_u64(bytes, 16).into(),
            oid: read_u64(bytes, 24).into(),
            paddr: read_u64(bytes, 32).into(),
        }
    }
}

impl CheckpointMappingPhys {
    pub const BASE_LEN: usize = ObjPhys::LEN + 8;

    pub fn parse(bytes: &[u8]) -> Self {
        let header = ObjPhys::parse(&bytes[..32]);
        let flags = read_u32(bytes, 32);
        let count = read_u32(bytes, 36);

        let mut mappings = vec! [];

        for i in 0..count as usize {
            mappings.push(CheckpointMapping::parse(&bytes[40 + i * 40 .. 40 + (i + 1) * 40]));
        }

        let len = Self::BASE_LEN + count as usize * CheckpointMapping::LEN;

        Self {
            header,
            flags,
            count,
            mappings: mappings.into_boxed_slice(),
        }
    }
}

#[derive(Debug)]
pub enum CheckpointDescAreaEntry {
    Superblock(NxSuperblock),
    Mapping(CheckpointMappingPhys),
}

impl CheckpointDescAreaEntry {
    pub fn into_superblock(self) -> Option<NxSuperblock> {
        match self {
            Self::Superblock(superblock) => Some(superblock),
            Self::Mapping(_) => None,
        }
    }
}

pub fn read_from_desc_area<D: fal::Device>(device: &mut D, superblock: &NxSuperblock, index: u32) -> CheckpointDescAreaEntry {
    let block_bytes = Filesystem::read_block(superblock, device, superblock.chkpnt_desc_base + i64::from(index));

    let obj_phys = ObjPhys::parse(&block_bytes[..32]);
    match obj_phys.object_type.ty {
        ObjectType::NxSuperblock => CheckpointDescAreaEntry::Superblock(NxSuperblock::parse(&block_bytes)),
        ObjectType::CheckpointMap => CheckpointDescAreaEntry::Mapping(CheckpointMappingPhys::parse(&block_bytes)),

        other => panic!("Unexpected checkpoint desc area entry type: {:?}.", other),
    }
}
