use enum_primitive::FromPrimitive;

use fal::{read_u32, read_u64};

use crate::{
    btree::BTreeNode,
    filesystem::Filesystem,
    reaper::ReaperPhys,
    spacemanager::SpacemanagerPhys,
    superblock::{
        BlockAddr, NxSuperblock, ObjPhys, ObjectIdentifier, ObjectType, ObjectTypeAndFlags,
    },
};

#[derive(Debug)]
pub struct CheckpointMapping {
    pub ty: ObjectTypeAndFlags,
    pub subtype: ObjectType,
    pub size: u32,
    pub padding: u32,
    pub fs_oid: ObjectIdentifier,
    pub oid: ObjectIdentifier,
    pub paddr: ObjectIdentifier,
}

#[derive(Debug)]
pub struct CheckpointMappingPhys {
    pub header: ObjPhys,
    pub flags: u32,
    pub count: u32,
    pub mappings: Box<[CheckpointMapping]>,
}

impl CheckpointMapping {
    pub const LEN: usize = 40;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            ty: ObjectTypeAndFlags::from_raw(read_u32(bytes, 0)),

            subtype: ObjectType::from_u32(read_u32(bytes, 4)).unwrap(),
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

        let mut mappings = vec![];

        for i in 0..count as usize {
            mappings.push(CheckpointMapping::parse(
                &bytes[40 + i * 40..40 + (i + 1) * 40],
            ));
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

#[derive(Debug)]
pub enum GenericObject {
    SpaceManager(SpacemanagerPhys),
    Reaper(ReaperPhys),
    BTreeNode(BTreeNode),
    Null,
}

impl CheckpointDescAreaEntry {
    pub fn into_superblock(self) -> Option<NxSuperblock> {
        match self {
            Self::Superblock(superblock) => Some(superblock),
            Self::Mapping(_) => None,
        }
    }
    pub fn into_mapping(self) -> Option<CheckpointMappingPhys> {
        match self {
            Self::Mapping(mapping) => Some(mapping),
            Self::Superblock(_) => None,
        }
    }
}

pub fn read_from_desc_area<D: fal::Device>(
    device: &mut D,
    superblock: &NxSuperblock,
    index: u32,
) -> CheckpointDescAreaEntry {
    let block_bytes = Filesystem::read_block(
        superblock,
        device,
        superblock.chkpnt_desc_base + i64::from(index),
    );

    let obj_phys = ObjPhys::parse(&block_bytes[..32]);
    match obj_phys.object_type.ty {
        ObjectType::NxSuperblock => {
            CheckpointDescAreaEntry::Superblock(NxSuperblock::parse(&block_bytes))
        }
        ObjectType::CheckpointMap => {
            CheckpointDescAreaEntry::Mapping(CheckpointMappingPhys::parse(&block_bytes))
        }

        other => panic!("Unexpected checkpoint desc area entry type: {:?}.", other),
    }
}

pub fn read_from_data_area<D: fal::Device>(
    device: &mut D,
    superblock: &NxSuperblock,
    index: u32,
) -> Option<GenericObject> {
    let block_bytes = Filesystem::read_block(
        superblock,
        device,
        superblock.chkpnt_data_base + i64::from(index),
    );

    let obj_phys = ObjPhys::parse(&block_bytes[..32]);
    match obj_phys.object_type.ty {
        ObjectType::SpaceManager => Some(GenericObject::SpaceManager(SpacemanagerPhys::parse(
            &block_bytes,
        ))),
        ObjectType::NxReaper => Some(GenericObject::Reaper(ReaperPhys::parse(&block_bytes))),
        ObjectType::Btree | ObjectType::BtreeNode => Some(GenericObject::BTreeNode(
            BTreeNode::parse(&block_bytes)
        )),
        _ => {
            dbg!(index);
            dbg!(obj_phys.object_type.ty, obj_phys.object_subtype);
            Some(GenericObject::Null)
        }
    }
}
