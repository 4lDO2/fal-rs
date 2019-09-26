use std::{
    io::SeekFrom,
    ops::BitAnd,
    ops::BitOr,
};

use uuid::Uuid;

use fal::{read_u32, read_u64, read_uuid, write_u32, write_u64, write_uuid};

use crate::{BlockAddr, BlockRange, ObjectIdentifier, TransactionIdentifier, ObjPhys};


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

/// Per-container (partition) superblock.
#[derive(Debug)]
pub struct NxSuperblock {
    pub header: ObjPhys,
    pub block_size: u32,
    pub block_count: u64,

    pub features: Features,
    pub incompat_features: IncompatFeatures,
    pub ro_compat_features: RoCompatFeatures,

    pub uuid: Uuid,

    pub next_oid: ObjectIdentifier,
    pub next_xid: TransactionIdentifier,

    pub chkpnt_desc_blkcnt: u32,
    pub chkpnt_data_blkcnt: u32,
    pub chkpnt_desc_base: BlockAddr,
    pub chkpnt_data_base: BlockAddr,
    pub chkpnt_desc_next_idx: u32,
    pub chkpnt_data_next_idx: u32,
    pub chkpnt_desc_first: u32,
    pub chkpnt_desc_len: u32,
    pub chkpnt_data_first: u32,
    pub chkpnt_data_len: u32,

    pub spacemanager_oid: ObjectIdentifier,
    pub object_map_oid: ObjectIdentifier,
    pub reaper_oid: ObjectIdentifier,

    // test_type: u32
    pub max_volume_count: u32,
    pub volumes_oids: Box<[ObjectIdentifier]>,
    pub counters: Box<[u64]>,

    pub blocked_out_blocks: BlockRange,
    pub evict_mapping_tre_oid: ObjectIdentifier,

    pub flags: u64,

    pub efi_jumpstart: BlockAddr,
    pub fusion_uuid: Uuid,
    pub keylocker: BlockRange,
    pub ephemeral_info: Box<[u64]>,

    // test_oid: u32
    pub fusion_mt_oid: ObjectIdentifier,
    pub fusion_wbc_oid: ObjectIdentifier,
    pub fusion_wbc: BlockRange,
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

        let mut volumes_oids = vec![];

        for i in 0..max_volume_count {
            volumes_oids.push(read_u64(block_bytes, 184 + i as usize * 8).into());
        }

        let volumes_oids = volumes_oids.into_boxed_slice();

        // The new offset should be 8 * 100 + 184 = 984.
        // As the counters are only used for debugging, we can ignore them for now.

        // The counters take up 32 * sizeof(u64) bytes, which sets the new offset to 984 + 256 =
        // 1240.

        let blocked_out_blocks = BlockRange {
            start: read_u64(block_bytes, 1240) as i64,
            count: read_u64(block_bytes, 1248),
        };

        let evict_mapping_tre_oid = read_u64(block_bytes, 1256).into();
        let flags = read_u64(block_bytes, 1264);
        let efi_jumpstart = read_u64(block_bytes, 1272) as i64;
        let fusion_uuid = read_uuid(block_bytes, 1280);

        let keylocker = BlockRange {
            start: read_u64(block_bytes, 1296) as i64,
            count: read_u64(block_bytes, 1304),
        };

        let mut ephemeral_info = vec![];

        for i in 0..Self::EPH_INFO_COUNT {
            ephemeral_info.push(read_u64(block_bytes, 1312 + i as usize * 8));
        }

        let ephemeral_info = ephemeral_info.into_boxed_slice();

        // Offset is now 1312 + 8 * 4 = 1344.
        // test_oid, offset 1352.

        let fusion_mt_oid = read_u64(block_bytes, 1352).into();
        let fusion_wbc_oid = read_u64(block_bytes, 1360).into();

        let fusion_wbc = BlockRange {
            start: read_u64(block_bytes, 1368) as i64,
            count: read_u64(block_bytes, 1376),
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

            counters: vec![].into_boxed_slice(),
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
    pub fn chkpnt_desc_blkcnt(&self) -> u32 {
        self.chkpnt_desc_blkcnt & !(1 << 31)
    }
    pub fn chkpnt_data_blkcnt(&self) -> u32 {
        self.chkpnt_data_blkcnt & !(1 << 31)
    }
    pub fn is_valid(&self) -> bool {
        // FIXME
        true
    }
}

/// Per-volume superblock.
#[derive(Debug)]
pub struct ApfsSuperblock {
    header: ObjPhys,

    // magic
    fs_index: u32,

    features: u64,
    ro_incompat_features: u64,
    incompat_features: u64,

    unmount_time: u64,

    fs_reserve_block_count: u64,
    fs_quota_block_count: u64,
    fs_alloc_count: u64,
}

impl ApfsSuperblock {
    pub const MAGIC: u32 = 0x42535041; // 'BSPA'

    pub fn parse(bytes: &[u8]) -> Self {
        let header = ObjPhys::parse(&bytes[..32]);

        assert_eq!(read_u32(bytes, 32), Self::MAGIC);

        Self {
            header,

            fs_index: read_u32(bytes, 36),

            features: read_u64(bytes, 40),
            ro_incompat_features: read_u64(bytes, 48),
            incompat_features: read_u64(bytes, 56),

            unmount_time: read_u64(bytes, 64),

            fs_reserve_block_count: read_u64(bytes, 72),
            fs_quota_block_count: read_u64(bytes, 80),
            fs_alloc_count: read_u64(bytes, 88),

        }
    }
}
