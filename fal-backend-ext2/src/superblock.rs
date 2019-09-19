use crate::{div_round_up, read_u16, read_u32, read_u8, write_u8, write_u16, write_u32, write_uuid, read_uuid, Uuid};

use std::{
    ffi::CString,
    io::{self, prelude::*, SeekFrom},
    ops::{AddAssign, ShrAssign},
};

pub const SUPERBLOCK_OFFSET: u64 = 1024;
pub const SUPERBLOCK_LEN: u64 = 1024;

pub const SIGNATURE: u16 = 0xEF53;

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
    pub vol_name: Option<CString>,
    pub last_mount_path: Option<CString>,
    pub compression_algorithms: u32,
    pub file_prealloc_block_count: u8,
    pub dir_prealloc_block_count: u8,
    pub journal_id: Uuid,
    pub journal_inode: u32,
    pub journal_device: u32,
    pub orphan_inode_head_list: u32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct OptionalFeatureFlags {
    pub preallocate_dir_blocks: bool,
    pub afs_server_inodes: bool,
    pub has_journal: bool,
    pub inode_extended_attributes: bool,
    pub growable: bool,
    pub dir_hash_index: bool,
}
impl OptionalFeatureFlags {
    pub const PREALLOCATE_DIR_BLOCKS_BIT: u32 = 0x0001;
    pub const AFS_SERVER_INODES_BIT: u32 = 0x0002;
    pub const HAS_JOURNAL_BIT: u32 = 0x0004;
    pub const INODE_EXTENDED_ATTRIBUTES_BIT: u32 = 0x0008;
    pub const GROWABLE_BIT: u32 = 0x0010;
    pub const DIR_HASH_INDEX_BIT: u32 = 0x0020;

    pub fn from_raw(raw: u32) -> Self {
        Self {
            preallocate_dir_blocks: raw & Self::PREALLOCATE_DIR_BLOCKS_BIT != 0,
            afs_server_inodes: raw & Self::AFS_SERVER_INODES_BIT != 0,
            has_journal: raw & Self::HAS_JOURNAL_BIT != 0,
            inode_extended_attributes: raw & Self::INODE_EXTENDED_ATTRIBUTES_BIT != 0,
            growable: raw & Self::GROWABLE_BIT != 0,
            dir_hash_index: raw & Self::DIR_HASH_INDEX_BIT != 0,
        }
    }
    pub fn into_raw(this: Self) -> u32 {
        let mut raw = 0;
        if this.preallocate_dir_blocks {
            raw |= Self::PREALLOCATE_DIR_BLOCKS_BIT;
        }
        if this.afs_server_inodes {
            raw |= Self::AFS_SERVER_INODES_BIT;
        }
        if this.has_journal {
            raw |= Self::HAS_JOURNAL_BIT;
        }
        if this.inode_extended_attributes {
            raw |= Self::INODE_EXTENDED_ATTRIBUTES_BIT;
        }
        if this.growable {
            raw |= Self::GROWABLE_BIT;
        }
        if this.dir_hash_index {
            raw |= Self::DIR_HASH_INDEX_BIT
        }
        raw
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RequiredFeatureFlags {
    pub compression: bool,
    pub dir_type: bool,
    pub replay_journal_mandatory: bool,
    pub journal_device: bool,
}

impl RequiredFeatureFlags {
    pub const COMPRESSION_BIT: u32 = 0x0001;
    pub const DIR_TYPE_BIT: u32 = 0x0002;
    pub const REPLAY_JOURNAL_MANDATORY_BIT: u32 = 0x0004;
    pub const JOURNAL_DEVICE_BIT: u32 = 0x0008;

    pub fn from_raw(raw: u32) -> Self {
        Self {
            compression: raw & Self::COMPRESSION_BIT != 0,
            dir_type: raw & Self::DIR_TYPE_BIT != 0,
            replay_journal_mandatory: raw & Self::REPLAY_JOURNAL_MANDATORY_BIT != 0,
            journal_device: raw & Self::JOURNAL_DEVICE_BIT != 0,
        }
    }
    pub fn into_raw(this: Self) -> u32 {
        let mut raw = 0;
        if this.compression {
            raw |= Self::COMPRESSION_BIT;
        }
        if this.dir_type {
            raw |= Self::DIR_TYPE_BIT;
        }
        if this.replay_journal_mandatory {
            raw |= Self::REPLAY_JOURNAL_MANDATORY_BIT;
        }
        if this.journal_device {
            raw |= Self::JOURNAL_DEVICE_BIT
        }
        raw
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RoFeatureFlags {
    pub sparse_super: bool,
    pub extended_file_size: bool,
    pub bst_dir_contents: bool,
}

impl RoFeatureFlags {
    pub const SPARSE_SUPER_BIT: u32 = 0x0001;
    pub const EXTENDED_FILE_SIZE_BIT: u32 = 0x0002;
    pub const BST_DIR_CONTENTS_BIT: u32 = 0x0004;

    pub fn from_raw(raw: u32) -> Self {
        Self {
            sparse_super: raw & Self::SPARSE_SUPER_BIT != 0,
            extended_file_size: raw & Self::EXTENDED_FILE_SIZE_BIT != 0,
            bst_dir_contents: raw & Self::BST_DIR_CONTENTS_BIT != 0,
        }
    }
    pub fn into_raw(this: Self) -> u32 {
        let mut raw = 0;
        if this.sparse_super {
            raw |= Self::SPARSE_SUPER_BIT;
        }
        if this.extended_file_size {
            raw |= Self::EXTENDED_FILE_SIZE_BIT;
        }
        if this.bst_dir_contents {
            raw |= Self::BST_DIR_CONTENTS_BIT
        }
        raw
    }
}

fn log2_round_up<T: From<u8> + AddAssign + ShrAssign + Eq>(mut t: T) -> T {
    if t == 1.into() { return 0.into(); }

    let mut count = 0.into();

    while t != 1.into() {
        t >>= 1.into();
        count += 1.into();
    }

    count
}

impl Superblock {
    /// Load and parse the super block.
    pub fn load<R: Read + Seek>(mut device: R) -> io::Result<Self> {
        device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET)).unwrap();

        let mut block_bytes = [0u8; 1024];
        device.read_exact(&mut block_bytes)?;

        Ok(Self::parse(&block_bytes))
    }
    pub fn parse(block_bytes: &[u8]) -> Self {
        let inode_count = read_u32(&block_bytes, 0);
        let block_count = read_u32(&block_bytes, 4);
        let reserved_block_count = read_u32(&block_bytes, 8);
        let unalloc_block_count = read_u32(&block_bytes, 12);
        let unalloc_inode_count = read_u32(&block_bytes, 16);
        let superblock_block_num = read_u32(&block_bytes, 20);
        let block_size = 1024 << read_u32(&block_bytes, 24);
        let fragment_size = 1024 << read_u32(&block_bytes, 28);
        let blocks_per_group = read_u32(&block_bytes, 32);
        let fragments_per_group = read_u32(&block_bytes, 36);
        let inodes_per_group = read_u32(&block_bytes, 40);
        let last_mount_time = read_u32(&block_bytes, 44);
        let last_write_time = read_u32(&block_bytes, 48);
        let mounts_since_fsck = read_u16(&block_bytes, 52);
        let mounts_left_before_fsck = read_u16(&block_bytes, 54);
        let signature = read_u16(&block_bytes, 56);
        assert_eq!(signature, SIGNATURE);
        let fs_state = FilesystemState::try_parse(read_u16(&block_bytes, 58)).unwrap();
        let error_handling = ErrorHandlingMethod::try_parse(read_u16(&block_bytes, 60)).unwrap();
        let minor_version = read_u16(&block_bytes, 62);
        let last_fsck_time = read_u32(&block_bytes, 64);
        let interval_between_forced_fscks = read_u32(&block_bytes, 68);
        let os_id = OsId::try_parse(read_u32(&block_bytes, 72)).unwrap();
        let major_version = read_u32(&block_bytes, 76);
        let reserver_uid = read_u16(&block_bytes, 80);
        let reserver_gid = read_u16(&block_bytes, 82);

        let extended = if major_version >= 1 {
            let first_nonreserved_inode = read_u32(&block_bytes, 84);
            let inode_struct_size = read_u16(&block_bytes, 88);
            let superblock_block_group = read_u16(&block_bytes, 90);
            let opt_features_present = OptionalFeatureFlags::from_raw(read_u32(&block_bytes, 92));
            let req_features_present = RequiredFeatureFlags::from_raw(read_u32(&block_bytes, 96));

            let req_features_for_rw = RoFeatureFlags::from_raw(read_u32(&block_bytes, 100));
            let fs_id = read_uuid(&block_bytes, 104);

            let vol_name = {
                let mut vol_name_raw = [0u8; 16];
                vol_name_raw.copy_from_slice(&block_bytes[120..136]);

                let nul_position = vol_name_raw
                    .iter()
                    .copied()
                    .position(|byte| byte == 0)
                    .unwrap_or(vol_name_raw.len());
                if nul_position != 0 {
                    CString::new(&vol_name_raw[..nul_position]).ok()
                } else {
                    None
                }
            };

            let last_mount_path = {
                let mut last_mount_path_raw = [0u8; 64];
                last_mount_path_raw.copy_from_slice(&block_bytes[136..200]);

                let nul_position = last_mount_path_raw
                    .iter()
                    .copied()
                    .position(|byte| byte == 0)
                    .unwrap_or(last_mount_path_raw.len());
                if nul_position != 0 {
                    CString::new(&last_mount_path_raw[..nul_position]).ok()
                } else {
                    None
                }
            };

            let compression_algorithms = read_u32(&block_bytes, 200);
            let file_prealloc_block_count = read_u8(&block_bytes, 204);
            let dir_prealloc_block_count = read_u8(&block_bytes, 205);

            let journal_id = read_uuid(&block_bytes, 208);
            let journal_inode = read_u32(&block_bytes, 224);
            let journal_device = read_u32(&block_bytes, 228);
            let orphan_inode_head_list = read_u32(&block_bytes, 232);

            Some(SuperblockExtension {
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
                journal_id,
                journal_inode,
                journal_device,
                orphan_inode_head_list,
            })
        } else {
            None
        };

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
        write_u32(buffer, 0, self.inode_count);
        write_u32(buffer, 4, self.block_count);
        write_u32(buffer, 8, self.reserved_block_count);
        write_u32(buffer, 12, self.unalloc_block_count);
        write_u32(buffer, 16, self.unalloc_inode_count);
        write_u32(buffer, 20, self.superblock_block_num);
        write_u32(buffer, 24, log2_round_up(self.block_size as u32) - log2_round_up(1024));
        write_u32(buffer, 28, log2_round_up(self.fragment_size as u32) - log2_round_up(1024));
        write_u32(buffer, 32, self.blocks_per_group);
        write_u32(buffer, 36, self.fragments_per_group);
        write_u32(buffer, 40, self.inodes_per_group);
        write_u32(buffer, 44, self.last_mount_time);
        write_u32(buffer, 48, self.last_write_time);
        write_u16(buffer, 52, self.mounts_since_fsck);
        write_u16(buffer, 54, self.mounts_left_before_fsck);
        write_u16(buffer, 56, SIGNATURE);
        write_u16(buffer, 58, FilesystemState::serialize(self.fs_state));
        write_u16(buffer, 60, ErrorHandlingMethod::serialize(self.error_handling));
        write_u16(buffer, 62, self.minor_version);
        write_u32(buffer, 64, self.last_fsck_time);
        write_u32(buffer, 68, self.interval_between_forced_fscks);
        write_u32(buffer, 72, OsId::serialize(self.os_id));
        write_u32(buffer, 76, self.major_version);
        write_u16(buffer, 80, self.reserver_uid);
        write_u16(buffer, 82, self.reserver_gid);
    }

    /// Serialize the extended part of the superblock. The same buffer used for serialize_basic
    /// should be used here, as this functions writes to buffer[84..].
    pub fn serialize_extended(&self, buffer: &mut [u8]) {
        let extended: &SuperblockExtension = self.extended.as_ref().unwrap();

        write_u32(buffer, 84, extended.first_nonreserved_inode);
        write_u16(buffer, 88, extended.inode_struct_size);
        write_u16(buffer, 90, extended.superblock_block_group);
        write_u32(buffer, 92, OptionalFeatureFlags::into_raw(extended.opt_features_present));
        write_u32(buffer, 96, RequiredFeatureFlags::into_raw(extended.req_features_present));

        write_u32(buffer, 100, RoFeatureFlags::into_raw(extended.req_features_for_rw));
        write_uuid(buffer, 104, &extended.fs_id);

        {
            if let Some(vol_name) = extended.vol_name.as_ref() {
                let vol_name_bytes = vol_name.as_bytes();
                assert_eq!(vol_name_bytes[15], 0);
                buffer[120..136].copy_from_slice(&vol_name_bytes);
            } else {
                buffer[120] = 0;
            }
        }

        {
            if let Some(last_mount_path) = extended.last_mount_path.as_ref() {
                let path_bytes = last_mount_path.as_bytes();
                assert_eq!(path_bytes[63], 0);
                buffer[136..200].copy_from_slice(&path_bytes);
            } else {
                buffer[136] = 0;
            }
        }

        write_u32(buffer, 200, extended.compression_algorithms);
        write_u8(buffer, 204, extended.file_prealloc_block_count);
        write_u8(buffer, 205, extended.dir_prealloc_block_count);

        write_uuid(buffer, 208, &extended.journal_id);
        write_u32(buffer, 224, extended.journal_inode);
        write_u32(buffer, 228, extended.journal_device);
        write_u32(buffer, 232, extended.orphan_inode_head_list);
    }

    fn serialize(&self, buffer: &mut [u8]) {
        self.serialize_basic(buffer);
        if self.major_version > 1 {
            self.serialize_extended(buffer);
        }
    }

    pub fn store<D: fal::DeviceMut>(&self, mut device: D) -> io::Result<()> {
        device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET)).unwrap();

        let mut block_bytes = [0u8; 1024];
        self.serialize(&mut block_bytes);
        device.write_all(&block_bytes)?;

        Ok(())
    }

    pub fn block_group_count(&self) -> u32 {
        let from_block_count = div_round_up(self.block_count, self.blocks_per_group);
        let from_inode_count = div_round_up(self.inode_count, self.inodes_per_group);
        assert_eq!(from_block_count, from_inode_count);
        from_block_count
    }
    pub fn inode_size(&self) -> u16 {
        self.extended
            .as_ref()
            .map(|extended| extended.inode_struct_size)
            .unwrap_or(128)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
