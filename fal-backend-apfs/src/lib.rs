pub mod btree;
pub mod checkpoint;
pub mod filesystem;
pub mod omap;
pub mod reaper;
pub mod spacemanager;
pub mod superblock;

use std::io::SeekFrom;
use fal::parsing::{read_u32, read_u64, write_u32, write_u64};

use bitflags::bitflags;
use enum_primitive::{
    enum_from_primitive, enum_from_primitive_impl, enum_from_primitive_impl_ty, FromPrimitive,
};

use superblock::NxSuperblock;

/// A block address (paddr_t). Never negative.
pub type BlockAddr = i64;

/// A range of physical blocks (prange_t).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRange {
    pub start: BlockAddr,
    pub count: u64,
}

/// (oid_t).
#[derive(Clone, Copy, Debug, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct ObjectIdentifier(pub u64);

impl ObjectIdentifier {
    const NX_SUPERBLOCK: Self = Self(1);
    const INVALID: Self = Self(0);
    const RESERVED_COUNT: u64 = 1024;

    pub fn is_valid(&self) -> bool {
        *self != Self::INVALID
    }
}

impl From<u64> for ObjectIdentifier {
    fn from(int: u64) -> Self {
        Self(int)
    }
}
impl From<ObjectIdentifier> for u64 {
    fn from(ObjectIdentifier(oid): ObjectIdentifier) -> Self {
        oid
    }
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ObjectType {
        NxSuperblock = 0x1,
        Btree = 0x2,
        BtreeNode = 0x3,

        SpaceManager = 0x5,
        SpaceManagerCab = 0x6,
        SpaceManagerCib = 0x7,
        SpaceManagerBitmap = 0x8,
        SpaceManagerFreeQueue = 0x9,

        ExtentListTree = 0xA,
        ObjectMap = 0xB,
        CheckpointMap = 0xC,
        Filesystem = 0xD,
        FsTree = 0xE,
        BlockRefTree = 0xF,
        SnapshotMetadataTree = 0x10,
        NxReaper = 0x11,
        NxReaperList = 0x12,
        ObjectManagerSnapshot = 0x13,
        EfiJumpstart = 0x14,
        FusionMiddleTree = 0x15,
        NxFusionWbc = 0x16,
        NxFusionWbcList = 0x17,
        ErState = 0x18,
        GpBitmap = 0x19,
        GpBitmapTree = 0x1A,
        GpBitmapBlock = 0x1B,
        Invalid = 0,
        Test = 0xFF,

        // FIXME: How should the bitmask-overflowing ContainerKeybag and VolumeKeybag be handled?
        ContainerKeybag = 0x6b657973, // 'keys'
        VolumeKeybag = 0x72656373, // 'recs'
    }
}

bitflags! {
    pub struct ObjectTypeFlags: u16 {
        const VIRTUAL = 0x0000;
        const EPHEMERAL = 0x8000;
        const PHYSICAL = 0x4000;
        const NOHEADER = 0x2000;
        const ENCRYPTED = 0x1000;
        const NONPERSISTENT = 0x0800;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectTypeAndFlags {
    pub ty: ObjectType,
    pub flags: ObjectTypeFlags,
}

impl ObjectTypeAndFlags {
    pub const TYPE_MASK: u32 = 0x0000FFFF;
    pub const FLAGS_MASK: u32 = 0xFFFF0000;

    pub fn from_raw(raw: u32) -> Self {
        let ty = raw & Self::TYPE_MASK;
        let flags = ((raw & Self::FLAGS_MASK) >> 16) as u16;
        Self {
            ty: ObjectType::from_u32(ty).unwrap(),
            flags: ObjectTypeFlags::from_bits(flags).unwrap(),
        }
    }
    pub fn into_raw(this: Self) -> u32 {
        (this.ty as u32 & Self::TYPE_MASK) | (((this.flags.bits() as u32) << 16) & Self::FLAGS_MASK)
    }
    pub fn is_ephemeral(&self) -> bool {
        self.flags.contains(ObjectTypeFlags::EPHEMERAL)
    }
    pub fn is_physical(&self) -> bool {
        self.flags.contains(ObjectTypeFlags::PHYSICAL)
    }
    pub fn is_virtual(&self) -> bool {
        !self.is_ephemeral() && !self.is_physical()
    }
}

/// (xid_t).
pub type TransactionIdentifier = u64;

// TODO: Write a fletcher crate.
type Fletcher64 = u64;

#[derive(Clone, Copy, Debug)]
pub struct ObjPhys {
    pub checksum: Fletcher64,
    pub object_id: ObjectIdentifier,
    pub transaction_id: TransactionIdentifier,
    pub object_type: ObjectTypeAndFlags,
    pub object_subtype: ObjectType,
}

impl ObjPhys {
    pub const LEN: usize = 32;
    pub fn parse(bytes: &[u8]) -> Self {

        let mut offset = 0;
        Self {
            checksum: read_u64(bytes, &mut offset),
            object_id: ObjectIdentifier::from(read_u64(bytes, &mut offset)),
            transaction_id: read_u64(bytes, &mut offset),
            object_type: ObjectTypeAndFlags::from_raw(read_u32(bytes, &mut offset)),
            object_subtype: ObjectType::from_u32(read_u32(bytes, &mut offset)).unwrap(),
        }
    }
    pub fn serialize(this: &Self, bytes: &mut [u8]) {
        let mut offset = 0;
        write_u64(bytes, &mut offset, this.checksum);
        write_u64(bytes, &mut offset, this.object_id.into());
        write_u64(bytes, &mut offset, this.transaction_id.into());
        write_u32(bytes, &mut offset, ObjectTypeAndFlags::into_raw(this.object_type));
        write_u32(bytes, &mut offset, this.object_subtype as u32);
    }
    pub fn is_ephemeral(&self) -> bool {
        self.object_type.is_ephemeral()
    }
    pub fn is_physical(&self) -> bool {
        self.object_type.is_physical()
    }
    pub fn is_virtual(&self) -> bool {
        self.object_type.is_virtual()
    }
}

pub fn read_obj_phys(bytes: &[u8], offset: &mut usize) -> ObjPhys {
    let obj_phys = ObjPhys::parse(&bytes[*offset..*offset + ObjPhys::LEN]);
    *offset += ObjPhys::LEN;
    obj_phys
}

pub fn read_block_to<D: fal::Device>(
    superblock: &NxSuperblock,
    device: &mut D,
    block: &mut [u8],
    address: crate::BlockAddr,
) {
    debug_assert!(address >= 0);
    debug_assert_eq!(block.len(), superblock.block_size as usize);

    device
        .seek(SeekFrom::Start(
            address as u64 * u64::from(superblock.block_size),
        ))
        .unwrap();
    device.read_exact(block).unwrap();
}
pub fn read_block<D: fal::Device>(
    superblock: &NxSuperblock,
    device: &mut D,
    address: crate::BlockAddr,
) -> Box<[u8]> {
    let mut block_bytes = vec![0u8; superblock.block_size as usize].into_boxed_slice();
    read_block_to(superblock, device, &mut block_bytes, address);
    block_bytes
}
