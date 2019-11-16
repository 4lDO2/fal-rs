use fal::parsing::{read_u8, read_u16, read_u32, read_u64, read_uuid, write_u8, write_u16, write_u32, write_u64, write_uuid};
use uuid::Uuid;

use std::{
    io::{self, SeekFrom},
    ops::{AddAssign, ShrAssign},
};

use arrayvec::ArrayVec;
use bitflags::bitflags;
use crc::{crc32, Hasher32};

pub const SUPERBLOCK_OFFSET: u64 = 1024;
pub const SUPERBLOCK_LEN: u64 = 1024;

pub const SIGNATURE: u16 = 0xEF53;

fn take<T>(bytes: &[u8], offset: &mut usize, amount: usize) -> ArrayVec<T>
where
    T: arrayvec::Array<Item = u8>
{
        let mut array = ::arrayvec::ArrayVec::new();
        array.try_extend_from_slice(&bytes[*offset..*offset + amount]).unwrap();
        *offset += amount;
        array
}

fn take_vec(bytes: &[u8], offset: &mut usize, amount: usize) -> Vec<u8> {
    let vector = bytes[*offset .. *offset + amount].to_owned();
    *offset += amount;
    vector
}

#[derive(Debug)]
pub struct Superblock {
    pub inode_count: u32,
    pub block_count: u32,
    pub reserved_block_count: u32,
    pub unalloc_block_count: u32,
    pub unalloc_inode_count: u32,
    pub superblock_block_num: u32,
    pub block_size: u32,
    pub fragment_size: u64,
    pub blocks_per_group: u32,
    pub fragments_per_group: u32,
    pub inodes_per_group: u32,
    pub last_mount_time: u32,
    pub last_write_time: u32,
    pub mounts_since_fsck: u16,
    pub mounts_left_before_fsck: u16,
    pub fs_state: FilesystemState,
    pub error_handling: ErrorHandlingMethod,
    pub minor_version: u16,
    pub last_fsck_time: u32,
    pub interval_between_forced_fscks: u32,
    pub os_id: OsId,
    pub major_version: u32,
    pub reserver_uid: u16,
    pub reserver_gid: u16,
    pub extended: Option<SuperblockExtension>,
}

#[derive(Debug)]
pub struct SuperblockExtension {
    pub first_nonreserved_inode: u32,
    pub inode_struct_size: u16,
    pub superblock_block_group: u16,
    pub opt_features_present: OptionalFeatureFlags,
    pub req_features_present: RequiredFeatureFlags,
    pub req_features_for_rw: RoFeatureFlags,
    pub fs_id: Uuid,
    pub vol_name: [u8; 16],
    pub last_mount_path: ArrayVec<[u8; 64]>,
    pub compression_algorithms: u32,
    pub file_prealloc_block_count: u8,
    pub dir_prealloc_block_count: u8,
    pub reserved_gdt_blocks: u16,
    pub journal_id: Uuid,
    pub journal_inode: u32,
    pub journal_device: u32,
    pub orphan_inode_head_list: u32,
    pub hash_seed: [u32; 4],
    pub default_hash_version: u8,
    pub jnl_backup_type: u8,
    pub bgdesc_size: u16,
    pub default_mounts_ops: u32,
    pub first_meta_bg: u32,
    pub mkfs_time: u32,
    pub jnl_blocks: [u32; 17],
    pub block_count_hi: u32,
    pub reserved_block_count_hi: u32,
    pub free_block_count_hi: u32,
    pub min_extra_isize: u16,
    pub want_extra_isize: u16,
    pub flags: u32,
    pub raid_stride: u16,
    pub mmp_interval: u16,
    pub mmp_block: u64,
    pub raid_stripe_width: u32,
    pub log_groups_per_flex: u8,
    pub checksum_type: u8,
    pub reserved_pad: u16,
    pub kbs_written: u64,
    pub snapshot_ino: u32,
    pub snapshot_id: u32,
    pub snapshot_rsv_blocks_count: u64,
    pub snapshot_list: u32,
    pub error_count: u32,

    pub first_error_time: u32,
    pub first_error_ino: u32,
    pub first_error_block: u64,
    pub first_error_func: [u8; 32],
    pub first_error_line: u32,

    pub last_error_time: u32,
    pub last_error_ino: u32,
    pub last_error_block: u64,
    pub last_error_func: [u8; 32],
    pub last_error_line: u32,

    pub mounts_opts: ArrayVec<[u8; 64]>,
    pub user_quota_ino: u32,
    pub group_quota_ino: u32,
    pub overhead_blocks: u32,
    pub backup_bgs: [u32; 2],
    pub encrypt_algos: [u8; 4],
    pub encrypt_pw_salt: [u8; 16],
    pub lost_found_ino: u32,
    pub prj_quota_ino: u32,
    pub checksum_seed: u32,

    pub wtime_hi: u8,
    pub mtime_hi: u8,
    pub mkfs_time_hi: u8,
    pub lastcheck_hi: u8,
    pub first_error_time_hi: u8,
    pub last_error_time_hi: u8,

    // 2 bytes of zero padding

    pub encoding: u16,
    pub encoding_flags: u16,

    pub reserved_bytes: Vec<u8>,
    pub superblock_checksum: u32,
}

bitflags! {
    pub struct OptionalFeatureFlags: u32 {
        const PREALLOCATE_DIR_BLOCKS = 0x0001;
        const AFS_SERVER_INODES = 0x0002;
        const HAS_JOURNAL = 0x0004;
        const INODE_EXTENDED_ATTRS = 0x0008;
        const GROWABLE_BIT = 0x0010;
        const DIR_HASH_INDEX = 0x0020;

        // ext4
        const SPARSE_SUPER2 = 0x0200;
    }
}

bitflags! {
    pub struct RequiredFeatureFlags: u32 {
        const COMPRESSION = 0x0001;
        const DIR_TYPE = 0x0002;
        const REPLAY_JOURNAL_MANDATORY = 0x0004;
        const JOURNAL_DEVICE = 0x0008;

        // ext4
        const META_BG = 0x0010;
        const EXTENTS = 0x0040;
        const _64_BIT = 0x0080;
        const MMP = 0x0100;
        const FLEX_BG = 0x0200;
        const EA_INODE = 0x0400;
        const DIRDATA = 0x1000;
        const CSUM_SEED = 0x2000;
        const LARGEDIR = 0x4000;
        const INLINE_DATA = 0x8000;
        const ENCRYPT = 0x10000;
        const CASEFOLD = 0x20000;
    }
}

bitflags! {
    pub struct RoFeatureFlags: u32 {
        const SPARSE_SUPER = 0x0001;
        const EXTENDED_FILE_SIZE = 0x0002;
        const BST_DIR_CONTENTS = 0x0004;

        // ext4
        const HUGE_FILE = 0x0008;
        const GDT_CSUM = 0x0010;
        const DIR_HARDLINK_COUNT = 0x0020;
        const EXTRA_ISIZE = 0x0040;
        const QUOTA = 0x0100;
        const BIGALLOC = 0x0200;
        const METADATA_CSUM = 0x0400; // Mutually exclusive with GDT_CSUM.
        const READONLY = 0x1000;
        const PROJECT = 0x2000;
    }
}

fn log2_round_up<T: From<u8> + AddAssign + ShrAssign + Eq>(mut t: T) -> T {
    if t == 1.into() {
        return 0.into();
    }

    let mut count = 0.into();

    while t != 1.into() {
        t >>= 1.into();
        count += 1.into();
    }

    count
}

impl Superblock {
    /// Load and parse the super block.
    pub fn load<R: fal::Device>(device: &mut R) -> io::Result<Self> {
        device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET))?;

        let mut block_bytes = [0u8; 1024];
        device.read_exact(&mut block_bytes)?;

        Ok(Self::parse(&block_bytes))
    }
    pub fn parse(block_bytes: &[u8]) -> Self {
        let mut offset = 0;
        let inode_count = read_u32(&block_bytes, &mut offset);
        let block_count = read_u32(&block_bytes, &mut offset);
        let reserved_block_count = read_u32(&block_bytes, &mut offset);
        let unalloc_block_count = read_u32(&block_bytes, &mut offset);
        let unalloc_inode_count = read_u32(&block_bytes, &mut offset);
        let superblock_block_num = read_u32(&block_bytes, &mut offset);
        let block_size = 1024 << read_u32(&block_bytes, &mut offset);
        let fragment_size = 1024 << read_u32(&block_bytes, &mut offset);
        let blocks_per_group = read_u32(&block_bytes, &mut offset);
        let fragments_per_group = read_u32(&block_bytes, &mut offset);
        let inodes_per_group = read_u32(&block_bytes, &mut offset);
        let last_mount_time = read_u32(&block_bytes, &mut offset);
        let last_write_time = read_u32(&block_bytes, &mut offset);
        let mounts_since_fsck = read_u16(&block_bytes, &mut offset);
        let mounts_left_before_fsck = read_u16(&block_bytes, &mut offset);
        let signature = read_u16(&block_bytes, &mut offset);
        assert_eq!(signature, SIGNATURE);
        let fs_state = FilesystemState::try_parse(read_u16(&block_bytes, &mut offset)).unwrap();
        let error_handling = ErrorHandlingMethod::try_parse(read_u16(&block_bytes, &mut offset)).unwrap();
        let minor_version = read_u16(&block_bytes, &mut offset);
        let last_fsck_time = read_u32(&block_bytes, &mut offset);
        let interval_between_forced_fscks = read_u32(&block_bytes, &mut offset);
        let os_id = OsId::try_parse(read_u32(&block_bytes, &mut offset)).unwrap();
        let major_version = read_u32(&block_bytes, &mut offset);
        let reserver_uid = read_u16(&block_bytes, &mut offset);
        let reserver_gid = read_u16(&block_bytes, &mut offset);

        let extended = if major_version >= 1 {
            let first_nonreserved_inode = read_u32(&block_bytes, &mut offset);
            let inode_struct_size = read_u16(&block_bytes, &mut offset);
            let superblock_block_group = read_u16(&block_bytes, &mut offset);
            let opt_features_present =
                OptionalFeatureFlags::from_bits(read_u32(&block_bytes, &mut offset)).unwrap();
            let req_features_present =
                RequiredFeatureFlags::from_bits(read_u32(&block_bytes, &mut offset)).unwrap();

            let req_features_for_rw =
                RoFeatureFlags::from_bits(read_u32(&block_bytes, &mut offset)).unwrap();
            let fs_id = read_uuid(&block_bytes, &mut offset);

            let vol_name = take(block_bytes, &mut offset, 16).into_inner().unwrap();
            let last_mount_path = take(block_bytes, &mut offset, 64);

            let compression_algorithms = read_u32(&block_bytes, &mut offset);
            let file_prealloc_block_count = read_u8(&block_bytes, &mut offset);
            let dir_prealloc_block_count = read_u8(&block_bytes, &mut offset);

            let reserved_gdt_blocks = read_u16(&block_bytes, &mut offset);

            let journal_id = read_uuid(&block_bytes, &mut offset);
            let journal_inode = read_u32(&block_bytes, &mut offset);
            let journal_device = read_u32(&block_bytes, &mut offset);
            let orphan_inode_head_list = read_u32(&block_bytes, &mut offset);

            let hash_seed = [read_u32(&block_bytes, &mut offset), read_u32(&block_bytes, &mut offset), read_u32(&block_bytes, &mut offset), read_u32(&block_bytes, &mut offset)];
            let default_hash_version = read_u8(&block_bytes, &mut offset);
            let jnl_backup_type = read_u8(&block_bytes, &mut offset);

            let bgdesc_size = read_u16(&block_bytes, &mut offset);
            let default_mounts_ops = read_u32(&block_bytes, &mut offset);
            let first_meta_bg = read_u32(&block_bytes, &mut offset);
            let mkfs_time = read_u32(&block_bytes, &mut offset);
            let jnl_blocks = (0..17).map(|_| read_u32(&block_bytes, &mut offset)).collect::<ArrayVec<_>>().into_inner().unwrap();

            let block_count_hi = read_u32(&block_bytes, &mut offset);
            let reserved_block_count_hi = read_u32(&block_bytes, &mut offset);
            let free_block_count_hi = read_u32(&block_bytes, &mut offset);

            let min_extra_isize =read_u16(&block_bytes, &mut offset);
            let want_extra_isize =read_u16(&block_bytes, &mut offset);
            let flags =read_u32(&block_bytes, &mut offset);

            let raid_stride = read_u16(&block_bytes, &mut offset);
            let mmp_interval = read_u16(&block_bytes, &mut offset);
            let mmp_block = read_u64(&block_bytes, &mut offset);
            let raid_stripe_width = read_u32(&block_bytes, &mut offset);

            let log_groups_per_flex = read_u8(&block_bytes, &mut offset);
            let checksum_type = read_u8(&block_bytes, &mut offset);
            let reserved_pad = read_u16(&block_bytes, &mut offset);

            let extended = SuperblockExtension {
                first_nonreserved_inode,
                inode_struct_size,
                superblock_block_group,
                opt_features_present,
                req_features_present,
                req_features_for_rw,
                fs_id,
                vol_name,
                last_mount_path,
                compression_algorithms,
                file_prealloc_block_count,
                dir_prealloc_block_count,
                reserved_gdt_blocks,
                journal_id,
                journal_inode,
                journal_device,
                orphan_inode_head_list,

                hash_seed,
                default_hash_version,
                jnl_backup_type,
                bgdesc_size,
                default_mounts_ops,
                first_meta_bg,
                mkfs_time,
                jnl_blocks,

                block_count_hi,
                reserved_block_count_hi,
                free_block_count_hi,

                min_extra_isize,
                want_extra_isize,
                flags,

                raid_stride,
                mmp_interval,
                mmp_block,
                raid_stripe_width,

                log_groups_per_flex,
                checksum_type,
                reserved_pad,

                kbs_written: read_u64(block_bytes, &mut offset),
                snapshot_ino: read_u32(block_bytes, &mut offset),
                snapshot_id: read_u32(block_bytes, &mut offset),
                snapshot_rsv_blocks_count: read_u64(block_bytes, &mut offset),
                snapshot_list: read_u32(block_bytes, &mut offset),
                error_count: read_u32(block_bytes, &mut offset),

                first_error_time: read_u32(block_bytes, &mut offset),
                first_error_ino: read_u32(block_bytes, &mut offset),
                first_error_block: read_u64(block_bytes, &mut offset),
                first_error_func: take(block_bytes, &mut offset, 32).into_inner().unwrap(),
                first_error_line: read_u32(block_bytes, &mut offset),

                last_error_time: read_u32(block_bytes, &mut offset),
                last_error_ino: read_u32(block_bytes, &mut offset),
                last_error_block: read_u64(block_bytes, &mut offset),
                last_error_func: take(block_bytes, &mut offset, 32).into_inner().unwrap(),
                last_error_line: read_u32(block_bytes, &mut offset),

                mounts_opts: take(block_bytes, &mut offset, 64),

                user_quota_ino: read_u32(block_bytes, &mut offset),
                group_quota_ino: read_u32(block_bytes, &mut offset),
                overhead_blocks: read_u32(block_bytes, &mut offset),
                backup_bgs: [read_u32(block_bytes, &mut offset), read_u32(block_bytes, &mut offset)],
                encrypt_algos: take(block_bytes, &mut offset, 4).into_inner().unwrap(),
                encrypt_pw_salt: take(block_bytes, &mut offset, 16).into_inner().unwrap(),
                lost_found_ino: read_u32(block_bytes, &mut offset),
                prj_quota_ino: read_u32(block_bytes, &mut offset),
                checksum_seed: read_u32(block_bytes, &mut offset),

                wtime_hi: read_u8(block_bytes, &mut offset),
                mtime_hi: read_u8(block_bytes, &mut offset),
                mkfs_time_hi: read_u8(block_bytes, &mut offset),
                lastcheck_hi: read_u8(block_bytes, &mut offset),
                first_error_time_hi: read_u8(block_bytes, &mut offset),
                last_error_time_hi: read_u8(block_bytes, &mut offset),

                // 2 bytes of zero padding
                encoding: {
                    offset += 2;
                    read_u16(block_bytes, &mut offset)
                },
                encoding_flags: read_u16(block_bytes, &mut offset),

                reserved_bytes: take_vec(block_bytes, &mut offset, 380),
                superblock_checksum: read_u32(block_bytes, &mut offset),
            };
            let _calculated_checksum = {
                let mut hasher = crc32::Digest::new_with_initial(crc32::CASTAGNOLI, !0);
                hasher.write(&block_bytes[..1020]);
                hasher.sum32()
            };
            //assert_eq!(calculated_checksum, extended.superblock_checksum);
            Some(extended)
        } else {
            None
        };

        if extended.is_some() {
            assert_eq!(offset, 1024);
        } else {
            assert_eq!(offset, 84);
        }

        Superblock {
            inode_count,
            block_count,
            reserved_block_count,
            unalloc_block_count,
            unalloc_inode_count,
            superblock_block_num,
            block_size,
            fragment_size,
            blocks_per_group,
            fragments_per_group,
            inodes_per_group,
            last_mount_time,
            last_write_time,
            mounts_since_fsck,
            mounts_left_before_fsck,
            fs_state,
            error_handling,
            minor_version,
            last_fsck_time,
            interval_between_forced_fscks,
            os_id,
            major_version,
            reserver_uid,
            reserver_gid,
            extended,
        }
    }
    /// Serialize the basic part of the superblock. The buffer must fit exactly one block.
    pub fn serialize_basic(&self, buffer: &mut [u8]) {
        let mut offset = 0;
        write_u32(buffer, &mut offset, self.inode_count);
        write_u32(buffer, &mut offset, self.block_count);
        write_u32(buffer, &mut offset, self.reserved_block_count);
        write_u32(buffer, &mut offset, self.unalloc_block_count);
        write_u32(buffer, &mut offset, self.unalloc_inode_count);
        write_u32(buffer, &mut offset, self.superblock_block_num);
        write_u32(
            buffer,
            &mut offset,
            log2_round_up(self.block_size as u32) - log2_round_up(1024),
        );
        write_u32(
            buffer,
            &mut offset,
            log2_round_up(self.fragment_size as u32) - log2_round_up(1024),
        );
        write_u32(buffer, &mut offset, self.blocks_per_group);
        write_u32(buffer, &mut offset, self.fragments_per_group);
        write_u32(buffer, &mut offset, self.inodes_per_group);
        write_u32(buffer, &mut offset, self.last_mount_time);
        write_u32(buffer, &mut offset, self.last_write_time);
        write_u16(buffer, &mut offset, self.mounts_since_fsck);
        write_u16(buffer, &mut offset, self.mounts_left_before_fsck);
        write_u16(buffer, &mut offset, SIGNATURE);
        write_u16(buffer, &mut offset, FilesystemState::serialize(self.fs_state));
        write_u16(
            buffer,
            &mut offset,
            ErrorHandlingMethod::serialize(self.error_handling),
        );
        write_u16(buffer, &mut offset, self.minor_version);
        write_u32(buffer, &mut offset, self.last_fsck_time);
        write_u32(buffer, &mut offset, self.interval_between_forced_fscks);
        write_u32(buffer, &mut offset, OsId::serialize(self.os_id));
        write_u32(buffer, &mut offset, self.major_version);
        write_u16(buffer, &mut offset, self.reserver_uid);
        write_u16(buffer, &mut offset, self.reserver_gid);
    }

    /// Serialize the extended part of the superblock. The same buffer used for serialize_basic
    /// should be used here, as this functions writes to buffer[84..].
    pub fn serialize_extended(&self, buffer: &mut [u8]) {
        let mut offset = 84;

        let extended: &SuperblockExtension = self.extended.as_ref().unwrap();

        write_u32(buffer, &mut offset, extended.first_nonreserved_inode);
        write_u16(buffer, &mut offset, extended.inode_struct_size);
        write_u16(buffer, &mut offset, extended.superblock_block_group);
        write_u32(buffer, &mut offset, extended.opt_features_present.bits());
        write_u32(buffer, &mut offset, extended.req_features_present.bits());

        write_u32(buffer, &mut offset, extended.req_features_for_rw.bits());
        write_uuid(buffer, &mut offset, &extended.fs_id);

        buffer[offset..offset + 16].copy_from_slice(&extended.vol_name);
        offset += 16;

        buffer[offset..offset + 64].copy_from_slice(&extended.last_mount_path);
        offset += 64;

        write_u32(buffer, &mut offset, extended.compression_algorithms);
        write_u8(buffer, &mut offset, extended.file_prealloc_block_count);
        write_u8(buffer, &mut offset, extended.dir_prealloc_block_count);
        write_u16(buffer, &mut offset, extended.reserved_gdt_blocks);

        write_uuid(buffer, &mut offset, &extended.journal_id);
        write_u32(buffer, &mut offset, extended.journal_inode);
        write_u32(buffer, &mut offset, extended.journal_device);
        write_u32(buffer, &mut offset, extended.orphan_inode_head_list);

        for n in extended.hash_seed.iter().copied() {
            write_u32(buffer, &mut offset, n);
        }
        write_u8(buffer, &mut offset, extended.default_hash_version);
        write_u8(buffer, &mut offset, extended.jnl_backup_type);
        write_u16(buffer, &mut offset, extended.bgdesc_size);

        write_u32(buffer, &mut offset, extended.default_mounts_ops);
        write_u32(buffer, &mut offset, extended.default_mounts_ops);
        write_u32(buffer, &mut offset, extended.first_meta_bg);

        for n in extended.jnl_blocks.iter().copied() {
            write_u32(buffer, &mut offset, n);
        }

        write_u32(buffer, &mut offset, extended.block_count_hi);
        write_u32(buffer, &mut offset, extended.reserved_block_count_hi);
        write_u32(buffer, &mut offset, extended.free_block_count_hi);

        write_u16(buffer, &mut offset, extended.min_extra_isize);
        write_u16(buffer, &mut offset, extended.want_extra_isize);
        write_u32(buffer, &mut offset, extended.flags);

        write_u16(buffer, &mut offset, extended.raid_stride);
        write_u64(buffer, &mut offset, extended.mmp_block);
        write_u32(buffer, &mut offset, extended.raid_stripe_width);
        write_u8(buffer, &mut offset, extended.log_groups_per_flex);
        write_u8(buffer, &mut offset, extended.checksum_type);
        write_u16(buffer, &mut offset, extended.reserved_pad);

        write_u64(buffer, &mut offset, extended.kbs_written);
        write_u32(buffer, &mut offset, extended.snapshot_ino);
        write_u32(buffer, &mut offset, extended.snapshot_id);
        write_u64(buffer, &mut offset, extended.snapshot_rsv_blocks_count);
        write_u32(buffer, &mut offset, extended.snapshot_list);
        write_u32(buffer, &mut offset, extended.error_count);

        write_u32(buffer, &mut offset, extended.first_error_time);
        write_u32(buffer, &mut offset, extended.first_error_ino);
        write_u64(buffer, &mut offset, extended.first_error_block);
        buffer[offset..offset + 32].copy_from_slice(&extended.first_error_func);
        offset += 32;
        write_u32(buffer, &mut offset, extended.first_error_line);

        write_u32(buffer, &mut offset, extended.last_error_time);
        write_u32(buffer, &mut offset, extended.last_error_ino);
        write_u64(buffer, &mut offset, extended.last_error_block);
        buffer[offset..offset + 32].copy_from_slice(&extended.last_error_func);
        offset += 32;
        write_u32(buffer, &mut offset, extended.last_error_line);

        buffer[offset..offset + 64].copy_from_slice(&extended.mounts_opts);
        offset += 64;

        write_u32(buffer, &mut offset, extended.user_quota_ino);
        write_u32(buffer, &mut offset, extended.group_quota_ino);
        write_u32(buffer, &mut offset, extended.overhead_blocks);

        for n in extended.backup_bgs.iter().copied() {
            write_u32(buffer, &mut offset, n);
        }

        buffer[offset..offset + 4].copy_from_slice(&extended.encrypt_algos);
        offset += 4;

        buffer[offset..offset + 16].copy_from_slice(&extended.encrypt_pw_salt);
        offset += 16;

        write_u32(buffer, &mut offset, extended.lost_found_ino);
        write_u32(buffer, &mut offset, extended.prj_quota_ino);
        write_u32(buffer, &mut offset, extended.checksum_seed);

        write_u8(buffer, &mut offset, extended.wtime_hi);
        write_u8(buffer, &mut offset, extended.mtime_hi);
        write_u8(buffer, &mut offset, extended.mkfs_time_hi);
        write_u8(buffer, &mut offset, extended.lastcheck_hi);
        write_u8(buffer, &mut offset, extended.first_error_time_hi);
        write_u8(buffer, &mut offset, extended.last_error_time_hi);

        // 2 bytes of zero padding
        offset += 2;

        write_u16(buffer, &mut offset, extended.encoding);
        write_u16(buffer, &mut offset, extended.encoding_flags);

        buffer[offset..offset + 380].copy_from_slice(&extended.reserved_bytes);
        offset += 380;

        // FIXME: Write the updated checksum.
        write_u32(buffer, &mut offset, extended.superblock_checksum);
    }

    pub fn ro_compat_features(&self) -> RoFeatureFlags {
        self.extended.as_ref().map(|ext| ext.req_features_for_rw).unwrap_or(RoFeatureFlags::empty())
    }

    fn serialize(&self, buffer: &mut [u8]) {
        self.serialize_basic(buffer);
        if self.major_version > 1 {
            self.serialize_extended(buffer);
        }
    }

    pub fn store<D: fal::DeviceMut>(&self, device: &mut D) -> io::Result<()> {
        device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET)).unwrap();

        let mut block_bytes = [0u8; 1024];
        self.serialize(&mut block_bytes);
        self.serialize_extended(&mut block_bytes);
        device.write_all(&block_bytes)?;

        Ok(())
    }

    pub fn block_group_count(&self) -> u32 {
        let from_block_count = fal::div_round_up(self.block_count, self.blocks_per_group);
        let from_inode_count = fal::div_round_up(self.inode_count, self.inodes_per_group);
        assert_eq!(from_block_count, from_inode_count);
        from_block_count
    }
    pub fn inode_size(&self) -> u16 {
        self.extended
            .as_ref()
            .map(|extended| extended.inode_struct_size)
            .unwrap_or(128)
    }
    pub fn is_64bit(&self) -> bool {
        self.extended.as_ref().map(|ext| ext.req_features_present.contains(RequiredFeatureFlags::_64_BIT)).unwrap_or(false)
    }
}
#[derive(Clone, Copy, Debug)]
pub enum FilesystemState {
    Clean = 1,
    HasErrors = 2,
}
impl FilesystemState {
    pub fn try_parse(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(FilesystemState::Clean),
            2 => Some(FilesystemState::HasErrors),
            _ => None,
        }
    }
    pub fn serialize(this: Self) -> u16 {
        match this {
            Self::Clean => 1,
            Self::HasErrors => 2,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum ErrorHandlingMethod {
    IgnoreError = 1,
    RemountAsRo = 2,
    KernelPanic = 3,
}
impl ErrorHandlingMethod {
    pub fn try_parse(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(ErrorHandlingMethod::IgnoreError),
            2 => Some(ErrorHandlingMethod::RemountAsRo),
            3 => Some(ErrorHandlingMethod::KernelPanic),
            _ => None,
        }
    }
    pub fn serialize(this: Self) -> u16 {
        match this {
            Self::IgnoreError => 1,
            Self::RemountAsRo => 2,
            Self::KernelPanic => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum OsId {
    Linux = 0,
    Hurd = 1,
    Masix = 2,
    FreeBsd = 3,
    OtherBsds = 4,
}
impl OsId {
    pub fn try_parse(raw: u32) -> Option<Self> {
        match raw {
            0 => Some(OsId::Linux),
            1 => Some(OsId::Hurd),
            2 => Some(OsId::Masix),
            3 => Some(OsId::FreeBsd),
            4 => Some(OsId::OtherBsds),
            _ => None,
        }
    }
    pub fn serialize(this: Self) -> u32 {
        match this {
            Self::Linux => 0,
            Self::Hurd => 1,
            Self::Masix => 2,
            Self::FreeBsd => 3,
            Self::OtherBsds => 4,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn log2_round_up() {
        assert_eq!(super::log2_round_up(1024), 10);
        assert_eq!(super::log2_round_up(1), 0);
        assert_eq!(super::log2_round_up(65536), 16);
    }
}
