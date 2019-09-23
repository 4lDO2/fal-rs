use std::{
    io::{SeekFrom, prelude::*},
    ops::BitOr, ops::BitAnd,
};

use enum_primitive::{enum_from_primitive, enum_from_primitive_impl, enum_from_primitive_impl_ty, FromPrimitive};
use uuid::Uuid;

use fal::{read_u8, read_u16, read_u32, read_u64, read_uuid, write_u8, write_u16, write_u32, write_u64, write_uuid};

/// A block address (paddr_t). Never negative.
pub type BlockAddr = i64;

/// A range of physical blocks (prange_t).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRange {
    pub start: BlockAddr,
    pub count: u64,
}

/// (oid_t).
#[derive(Clone, Copy, Debug)]
pub struct ObjectIdentifier(pub u64);

impl ObjectIdentifier {
    const NX_SUPERBLOCK: Self = Self(1);
    const INVALID: Self = Self(0);
    const RESERVED_COUNT: u64 = 1024;
}

impl From<u64> for ObjectIdentifier {
    fn from(int: u64) -> Self {
        Self(int)
    }
}
impl From<ObjectIdentifier> for u64 {
    fn from(ObjectIdentifier(oid): ObjectIdentifier) -> Self{
        oid
    }
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
pub struct ObjectTypeAndFlags {
    ty: ObjectType,
    flags: u16,
}

impl ObjectTypeAndFlags {
    pub const TYPE_MASK: u32 = 0x0000FFFF;
    pub const FLAGS_MASK: u32 = 0xFFFF0000;

    pub const FLAG_VIRTUAL: u16 = 0x0000;
    pub const FLAG_EPHEMERAL: u16 = 0x8000;
    pub const FLAG_PHYSICAL: u16 = 0x4000;
    pub const FLAG_NOHEADER: u16 = 0x2000;
    pub const FLAG_ENCRYPTED: u16 = 0x1000;
    pub const FLAG_NONPERSISTENT: u16 = 0x0800;

    pub fn from_raw(raw: u32) -> Self {
        let ty = raw & Self::TYPE_MASK;
        let flags = ((raw & Self::FLAGS_MASK) >> 16) as u16;
        Self {
            ty: ObjectType::from_u32(ty).unwrap(),
            flags,
        }
    }
    pub fn into_raw(this: Self) -> u32 {
        (this.ty as u32 & Self::TYPE_MASK) | (((this.flags as u32) << 16) & Self::FLAGS_MASK)
    }
}

/// (xid_t).
pub type TransactionIdenfifier = u64;

// TODO: Write a fletcher crate.
type Fletcher64 = u64;

#[derive(Clone, Copy, Debug)]
pub struct ObjPhys {
    pub checksum: Fletcher64,
    pub object_id: ObjectIdentifier,
    pub transaction_id: TransactionIdenfifier,
    pub object_type: ObjectTypeAndFlags,
    pub object_subtype: ObjectType,
}

impl ObjPhys {
    const LEN: usize = 32;
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            checksum: read_u64(bytes, 0),
            object_id: ObjectIdentifier::from(read_u64(bytes, 8)),
            transaction_id: read_u64(bytes, 16),
            object_type: ObjectTypeAndFlags::from_raw(read_u32(bytes, 24)),
            object_subtype: ObjectType::from_u32(read_u32(bytes, 28)).unwrap(),
        }
    }
    pub fn serialize(this: &Self, bytes: &mut [u8]) {
        write_u64(bytes, 0, this.checksum);
        write_u64(bytes, 8, this.object_id.into());
        write_u64(bytes, 16, this.transaction_id.into());
        write_u32(bytes, 24, ObjectTypeAndFlags::into_raw(this.object_type));
        write_u32(bytes, 28, this.object_subtype as u32);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Features(u64);

impl Features {
    pub fn defrag() -> Self {
        Self(1)
    }
    pub fn lcfd() -> Self {
        Self(2)
    }
    pub fn all() -> Self {
        Self::defrag() | Self::lcfd()
    }
    pub fn none() -> Self {
        Self(0)
    }
}

impl BitOr for Features {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl BitAnd for Features {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

impl std::fmt::Debug for Features {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut features = vec![];

        if *self & Self::defrag() != Self::none() {
            features.push("DEFRAGMENT");
        }

        if *self & Self::lcfd() != Self::none() {
            features.push("LOW_CAPACITY_FUSION_DRIVE");
        }

        if *self == Self::none() {
            features.push("(none)");
        }

        write!(f, "{}", features.join(" | "))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct IncompatFeatures(u64);

impl IncompatFeatures {
    pub fn version_1() -> Self {
        Self(1)
    }
    pub fn version_2() -> Self {
        Self(2)
    }
    pub fn fusion() -> Self {
        Self(0x100)
    }
    pub fn none() -> Self {
        Self(0)
    }
    pub fn all() -> Self {
        Self::version_1() | Self::version_1() | Self::version_2()
    }
}

impl BitOr for IncompatFeatures {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl BitAnd for IncompatFeatures {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

impl std::fmt::Debug for IncompatFeatures {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut features = vec![];

        if *self & Self::version_1() != Self::none() {
            features.push("VERSION_1");
        }

        if *self & Self::version_2() != Self::none() {
            features.push("VERSION_2");
        }

        if *self & Self::fusion() != Self::none() {
            features.push("FUSION");
        }

        if *self == Self::none() {
            features.push("(none)");
        }

        write!(f, "{}", features.join(" | "))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoCompatFeatures(u64);

impl RoCompatFeatures {
    pub fn none() -> Self {
        Self(0)
    }
    pub fn all() -> Self {
        Self::none()
    }
}

impl BitOr for RoCompatFeatures {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl BitAnd for RoCompatFeatures {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

#[derive(Debug)]
pub struct NxSuperblock {
    header: ObjPhys,
    block_size: u32,
    block_count: u64,

    features: Features,
    incompat_features: IncompatFeatures,
    ro_compat_features: RoCompatFeatures,

    uuid: Uuid,

    next_oid: ObjectIdentifier,
    next_xid: TransactionIdenfifier,

    chkpnt_desc_blkcnt: u32,
    chkpnt_data_blkcnt: u32,
    chkpnt_desc_base: BlockAddr,
    chkpnt_data_base: BlockAddr,
    chkpnt_desc_next_idx: u32,
    chkpnt_data_next_idx: u32,
    chkpnt_desc_first: u32,
    chkpnt_desc_len: u32,
    chkpnt_data_first: u32,
    chkpnt_data_len: u32,

    spacemanager_oid: ObjectIdentifier,
    object_map_oid: ObjectIdentifier,
    reaper_oid: ObjectIdentifier,

    // test_type: u32

    max_volume_count: u32,
    volumes_oids: Box<[ObjectIdentifier]>, 
    counters: Box<[u64]>,

    blocked_out_blocks: BlockRange,
    evict_mapping_tre_oid: ObjectIdentifier,

    flags: u64,

    efi_jumpstart: BlockAddr,
    fusion_uuid: Uuid,
    keylocker: BlockRange,
    ephemeral_info: Box<[u64]>,

    // test_oid: u32

    fusion_mt_oid: ObjectIdentifier,
    fusion_wbc_oid: ObjectIdentifier,
    fusion_wbc: BlockRange,
}

impl NxSuperblock {
    const MAGIC: u32 = 0x4253584E; // 'BSXN'
    const MAX_VOLUME_COUNT: u32 = 100;
    const NUM_COUNTERS: u32 = 32;
    const EPH_INFO_COUNT: u32 = 4;
    const EPH_MIN_BLOCK_COUNT: u32 = 4;

    pub fn load<D: fal::Device>(device: &mut D) -> Self {
        let mut block_bytes = [0; 4096];
        device.seek(SeekFrom::Start(0)).unwrap();
        device.read_exact(&mut block_bytes).unwrap();
        Self::parse(&block_bytes)
    }
    pub fn parse(block_bytes: &[u8]) -> Self {
        let header = ObjPhys::parse(&block_bytes[..ObjPhys::LEN]);

        let magic = read_u32(block_bytes, 32);
        assert_eq!(magic, Self::MAGIC);

        let block_size = read_u32(block_bytes, 36);
        let block_count = read_u64(block_bytes, 40);

        let features = Features(read_u64(block_bytes, 48));
        let ro_compat_features = RoCompatFeatures(read_u64(block_bytes, 56));
        let incompat_features = IncompatFeatures(read_u64(block_bytes, 64));

        let uuid = read_uuid(block_bytes, 72);

        let next_oid = read_u64(block_bytes, 88).into();
        let next_xid = read_u64(block_bytes, 96);

        let chkpnt_desc_blkcnt = read_u32(block_bytes, 104);
        let chkpnt_data_blkcnt = read_u32(block_bytes, 108);
        let chkpnt_desc_base = read_u64(block_bytes, 112) as i64;
        let chkpnt_data_base = read_u64(block_bytes, 120) as i64;
        let chkpnt_desc_next_idx = read_u32(block_bytes, 128);
        let chkpnt_data_next_idx = read_u32(block_bytes, 132);
        let chkpnt_desc_first = read_u32(block_bytes, 136);
        let chkpnt_desc_len = read_u32(block_bytes, 140);
        let chkpnt_data_first = read_u32(block_bytes, 144);
        let chkpnt_data_len = read_u32(block_bytes, 148);

        let spacemanager_oid = read_u64(block_bytes, 152).into();
        let object_map_oid = read_u64(block_bytes, 160).into();
        let reaper_oid = read_u64(block_bytes, 168).into();

        // nx_test_type
        assert_eq!(read_u32(block_bytes, 176), 0);

        let max_volume_count = read_u32(block_bytes, 180);
        assert!(max_volume_count <= 100);

        let mut volumes_oids = vec! [];

        for i in 0..max_volume_count {
            volumes_oids.push(read_u64(block_bytes, 180 + i as usize * 8).into());
        }

        let volumes_oids = volumes_oids.into_boxed_slice();

        // The new offset should be 8 * 100 + 180 = 980.
        // As the counters are only used for debugging, we can ignore them for now.

        // The counters take up 32 * sizeof(u64) bytes, which sets the new offset to 980 + 256 =
        // 1236.

        let blocked_out_blocks = BlockRange {
            start: read_u64(block_bytes, 1236) as i64,
            count: read_u64(block_bytes, 1244),
        };

        let evict_mapping_tre_oid = read_u64(block_bytes, 1252).into();
        let flags = read_u64(block_bytes, 1260);
        let efi_jumpstart = read_u64(block_bytes, 1268) as i64;
        let fusion_uuid = read_uuid(block_bytes, 1276);

        let keylocker = BlockRange {
            start: read_u64(block_bytes, 1292) as i64,
            count: read_u64(block_bytes, 1300),
        };

        let mut ephemeral_info = vec! [];

        for i in 0..Self::EPH_INFO_COUNT {
            ephemeral_info.push(read_u64(block_bytes, 1308 + i as usize * 8));
        }

        let ephemeral_info = ephemeral_info.into_boxed_slice();

        // Offset is now 1308 + 8 * 4 = 1340.
        // test_oid, offset 1348.

        let fusion_mt_oid = read_u64(block_bytes, 1348).into();
        let fusion_wbc_oid = read_u64(block_bytes, 1356).into();

        let fusion_wbc = BlockRange {
            start: read_u64(block_bytes, 1364) as i64,
            count: read_u64(block_bytes, 1372),
        };

        Self {
            header,
            block_size,
            block_count,

            features,
            ro_compat_features,
            incompat_features,

            uuid,

            next_oid,
            next_xid,

            chkpnt_desc_blkcnt,
            chkpnt_data_blkcnt,
            chkpnt_desc_base,
            chkpnt_data_base,
            chkpnt_desc_next_idx,
            chkpnt_data_next_idx,
            chkpnt_desc_first,
            chkpnt_desc_len,
            chkpnt_data_first,
            chkpnt_data_len,

            spacemanager_oid,
            object_map_oid,
            reaper_oid,

            max_volume_count,
            volumes_oids,

            counters: vec! [].into_boxed_slice(),
            blocked_out_blocks,

            evict_mapping_tre_oid,
            flags,
            efi_jumpstart,
            fusion_uuid,

            keylocker,
            ephemeral_info,

            fusion_mt_oid,
            fusion_wbc_oid,
            fusion_wbc,
        }
    }
    pub fn store<D: fal::DeviceMut>(this: &Self, device: &mut D) {
        let mut block_bytes = [0u8; 4096];
        Self::serialize(this, &mut block_bytes);
        device.seek(SeekFrom::Start(0)).unwrap();
        device.write_all(&block_bytes).unwrap();
    }
    pub fn serialize(this: &Self, block: &mut [u8]) {
        ObjPhys::serialize(&this.header, &mut block[..ObjPhys::LEN]);
        write_u32(block, 32, Self::MAGIC);
        write_u32(block, 36, this.block_size);
        write_u64(block, 40, this.block_count);
        write_u64(block, 48, this.features.0);
        write_u64(block, 56, this.ro_compat_features.0);
        write_u64(block, 64, this.incompat_features.0);
        write_uuid(block, 72, &this.uuid);
        write_u64(block, 88, this.next_oid.into());
        write_u64(block, 96, this.next_xid);
    }
}
