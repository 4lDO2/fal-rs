use std::{ffi::CString, io::SeekFrom};

use bitflags::bitflags;
use uuid::Uuid;

use fal::{read_u16, read_u32, read_u64, read_uuid, write_u32, write_u64, write_uuid};

use crate::{
    crypto::WrappedMetaCryptoState, BlockAddr, BlockRange, ObjPhys, ObjectIdentifier,
    ObjectTypeAndFlags, TransactionIdentifier,
};

bitflags! {
    pub struct ContainerFeatures: u64 {
        const DEFRAG = 0x1;
        const LCFD = 0x2;
    }
}

bitflags! {
    pub struct ContainerIncompatFeatures: u64 {
        const VERSION_1 = 0x1;
        const VERSION_2 = 0x2;
        const FUSION = 0x100;
    }
}

#[derive(Debug)]
pub struct ContainerRoCompatFeatures;

/// Per-container (partition) superblock.
#[derive(Debug)]
pub struct NxSuperblock {
    pub header: ObjPhys,
    pub block_size: u32,
    pub block_count: u64,

    pub features: ContainerFeatures,
    pub incompat_features: ContainerIncompatFeatures,
    pub ro_compat_features: ContainerRoCompatFeatures,

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
        let header = ObjPhys::parse(&block_bytes);

        let magic = read_u32(block_bytes, 32);
        assert_eq!(magic, Self::MAGIC);

        let block_size = read_u32(block_bytes, 36);
        let block_count = read_u64(block_bytes, 40);

        let features = ContainerFeatures::from_bits(read_u64(block_bytes, 48)).unwrap();
        let ro_compat_features = ContainerRoCompatFeatures;
        let incompat_features =
            ContainerIncompatFeatures::from_bits(read_u64(block_bytes, 64)).unwrap();

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
        assert!(max_volume_count <= Self::MAX_VOLUME_COUNT);

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
        write_u64(block, 48, this.features.bits());
        write_u64(block, 56, 0);
        write_u64(block, 64, this.incompat_features.bits());
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

bitflags! {
    pub struct VolumeOptFeatures: u64 {
        const DEFREAG_PRERELEASE = 0x1;
        const HARDLINK_MAP_RECORDS = 0x2;
        const DEFRAG = 0x4;
    }
}
#[derive(Debug)]
pub struct VolumeRoCompatFeatures;

bitflags! {
    pub struct VolumeIncompatFeatures: u64 {
        const CASE_INSENSITIVE = 0x1;
        const DATALESS_SNAPS = 0x2;
        const ENC_ROLLED = 04;
        const NORMALIZATION_INSENSITIVE = 0x8;
    }
}
bitflags! {
    pub struct VolumeRole: u16 {
        const NONE = 0x0;
        const SYSTEM = 0x1;
        const USER = 0x2;
        const RECOVERY = 0x4;
        const VM = 0x8;
        const PREBOOT = 0x10;
        const INSTALLER = 0x20;
        const DATA = 0x40;
        const BASEBAND = 0x80;
        const RESERVED_200 = 0x200;
    }
}
bitflags! {
    pub struct VolumeFlags: u64 {
        const UNENCRYPTED = 0x1;
        const RESERVED_2 = 0x2;
        const RESERVED_4 = 0x4;
        const ONEKEY = 0x8;
        const SPILLEDOVER = 0x10;
        const RUN_SPILLOVER_CLEANER = 0x20;
        const ALWAYS_CHECK_EXTENTREF = 0x40;
    }
}

/// Per-volume superblock.
#[derive(Debug)]
pub struct ApfsSuperblock {
    pub header: ObjPhys,

    // magic
    pub fs_index: u32,

    pub features: VolumeOptFeatures,
    pub ro_incompat_features: VolumeRoCompatFeatures,
    pub incompat_features: VolumeIncompatFeatures,

    pub unmount_time: u64,

    pub fs_reserve_block_count: u64,
    pub fs_quota_block_count: u64,
    pub fs_alloc_count: u64,

    pub meta_crypto_state: WrappedMetaCryptoState,

    pub root_tree_type: ObjectTypeAndFlags,
    pub extentref_tree_type: ObjectTypeAndFlags,
    pub snap_meta_tree_type: ObjectTypeAndFlags,

    pub omap_oid: ObjectIdentifier,
    pub root_tree_oid: ObjectIdentifier,
    pub extentref_tree_oid: ObjectIdentifier,
    pub snap_meta_tree_oid: ObjectIdentifier,

    pub revert_to_xid: TransactionIdentifier,
    pub revert_to_sblock_oid: ObjectIdentifier,

    pub next_obj_id: u64,

    pub file_count: u64,
    pub directory_count: u64,
    pub symlink_count: u64,
    pub other_fsobj_count: u64,
    pub snapshot_count: u64,

    pub total_blocks_allocated: u64,
    pub total_blocks_freed: u64,

    pub volume_uuid: Uuid,
    pub last_modification_time: u64,

    pub volume_flags: VolumeFlags,

    pub formatted_by: ApfsModifiedBy,
    pub modified_by: Vec<ApfsModifiedBy>,

    pub volume_name: String,
    pub next_document_id: u32,

    pub role: VolumeRole,
    pub reserved: u16,

    pub root_to_xid: TransactionIdentifier,
    pub er_state_oid: ObjectIdentifier,
}

#[derive(Debug)]
pub struct ApfsModifiedBy {
    id: CString,
    timestamp: u64,
    latest_xid: TransactionIdentifier,
}

impl ApfsModifiedBy {
    const NAME_LENGTH: usize = 32;
    const LEN: usize = 48;

    pub fn parse(bytes: &[u8]) -> Self {
        let id_bytes = &bytes[..Self::NAME_LENGTH];
        let nul_position = id_bytes
            .into_iter()
            .copied()
            .position(|byte| byte == 0)
            .unwrap_or(id_bytes.len());
        let id = CString::new(&id_bytes[..nul_position]).unwrap();

        Self {
            id,
            timestamp: read_u64(bytes, 32),
            latest_xid: read_u64(bytes, 40),
        }
    }
}

impl ApfsSuperblock {
    pub const MAGIC: u32 = 0x42535041; // 'BSPA'

    pub fn parse(bytes: &[u8]) -> Self {
        let header = ObjPhys::parse(&bytes);

        assert_eq!(read_u32(bytes, 32), Self::MAGIC);

        // The previous modification collection starts at 320, it consists of 8 entries, 48 bytes
        // each. The total size is hence 384, and the next offset is 704.
        let modified_by = (0..8)
            .map(|i| ApfsModifiedBy::parse(&bytes[320 + i * 48..320 + (i + 1) * 48]))
            .take_while(|modified_by| modified_by.latest_xid != 0)
            .collect();

        // After the volname, the offset is 704 + 256 = 960.
        let volume_name = {
            let volname_bytes = &bytes[704..704 + 256];
            let nul_position = volname_bytes
                .into_iter()
                .copied()
                .find(|b| *b == 0)
                .unwrap();
            String::from_utf8((&volname_bytes[..nul_position as usize]).to_owned()).unwrap()
        };

        Self {
            header,

            fs_index: read_u32(bytes, 36),

            features: VolumeOptFeatures::from_bits(read_u64(bytes, 40)).unwrap(),
            ro_incompat_features: VolumeRoCompatFeatures,
            incompat_features: VolumeIncompatFeatures::from_bits(read_u64(bytes, 56)).unwrap(),

            unmount_time: read_u64(bytes, 64),

            fs_reserve_block_count: read_u64(bytes, 72),
            fs_quota_block_count: read_u64(bytes, 80),
            fs_alloc_count: read_u64(bytes, 88),

            meta_crypto_state: WrappedMetaCryptoState::parse(&bytes[96..116]),

            root_tree_type: ObjectTypeAndFlags::from_raw(read_u32(bytes, 116)),
            extentref_tree_type: ObjectTypeAndFlags::from_raw(read_u32(bytes, 120)),
            snap_meta_tree_type: ObjectTypeAndFlags::from_raw(read_u32(bytes, 124)),

            omap_oid: read_u64(bytes, 128).into(),
            root_tree_oid: read_u64(bytes, 136).into(),
            extentref_tree_oid: read_u64(bytes, 144).into(),
            snap_meta_tree_oid: read_u64(bytes, 152).into(),

            revert_to_xid: read_u64(bytes, 160),
            revert_to_sblock_oid: read_u64(bytes, 168).into(),

            next_obj_id: read_u64(bytes, 176),

            file_count: read_u64(bytes, 184),
            directory_count: read_u64(bytes, 192),
            symlink_count: read_u64(bytes, 200),
            other_fsobj_count: read_u64(bytes, 208),
            snapshot_count: read_u64(bytes, 216),

            total_blocks_allocated: read_u64(bytes, 224),
            total_blocks_freed: read_u64(bytes, 232),

            volume_uuid: read_uuid(bytes, 240),
            last_modification_time: read_u64(bytes, 256),

            volume_flags: VolumeFlags::from_bits(read_u64(bytes, 264)).unwrap(),

            formatted_by: ApfsModifiedBy::parse(&bytes[272..320]),
            modified_by,

            volume_name,
            next_document_id: read_u32(bytes, 960),

            role: VolumeRole::from_bits(read_u16(bytes, 964)).unwrap(),
            reserved: read_u16(bytes, 966),

            root_to_xid: read_u64(bytes, 968),
            er_state_oid: read_u64(bytes, 976).into(),
        }
    }
}
