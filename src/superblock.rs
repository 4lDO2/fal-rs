use super::{read_u8, read_u16, read_u32, read_uuid, Uuid};

use std::{
    ffi::CString,
    io::{self, prelude::*, SeekFrom},
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
    pub block_size: u64,
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
    pub dir_hash_index: bool
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
    pub fn _into_raw(self) -> u32 {
        let mut raw = 0;
        if self.preallocate_dir_blocks {
            raw |= Self::PREALLOCATE_DIR_BLOCKS_BIT;
        }
        if self.afs_server_inodes {
            raw |= Self::AFS_SERVER_INODES_BIT;
        }
        if self.has_journal {
            raw |= Self::HAS_JOURNAL_BIT;
        }
        if self.inode_extended_attributes {
            raw |= Self::INODE_EXTENDED_ATTRIBUTES_BIT;
        }
        if self.growable {
            raw |= Self::GROWABLE_BIT;
        }
        if self.dir_hash_index {
            raw |= Self::DIR_HASH_INDEX_BIT
        }
        raw
    }
}

#[derive(Debug)]
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
    pub fn _into_raw(self) -> u32 {
        let mut raw = 0;
        if self.compression {
            raw |= Self::COMPRESSION_BIT;
        }
        if self.dir_type {
            raw |= Self::DIR_TYPE_BIT;
        }
        if self.replay_journal_mandatory {
            raw |= Self::REPLAY_JOURNAL_MANDATORY_BIT;
        }
        if self.journal_device {
            raw |= Self::JOURNAL_DEVICE_BIT
        }
        raw
    }
}

#[derive(Debug)]
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
    pub fn _into_raw(self) -> u32 {
        let mut raw = 0;
        if self.sparse_super {
            raw |= Self::SPARSE_SUPER_BIT;
        }
        if self.extended_file_size {
            raw |= Self::EXTENDED_FILE_SIZE_BIT;
        }
        if self.bst_dir_contents {
            raw |= Self::BST_DIR_CONTENTS_BIT
        }
        raw
    }
}

impl Superblock {
    pub fn parse<R: Read + Seek>(mut device: R) -> io::Result<Self> {
        device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET)).unwrap();

        let mut block_bytes = [0u8; 1024];
        device.read_exact(&mut block_bytes)?;

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

                let nul_position = vol_name_raw.iter().copied().position(|byte| byte == 0).unwrap_or(vol_name_raw.len());
                if nul_position != 0 {
                    CString::new(&vol_name_raw[..nul_position]).ok()
                } else {
                    None
                }
            };

            let last_mount_path = {
                let mut last_mount_path_raw = [0u8; 64];
                last_mount_path_raw.copy_from_slice(&block_bytes[136..200]);

                let nul_position = last_mount_path_raw.iter().copied().position(|byte| byte == 0).unwrap_or(last_mount_path_raw.len());
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

        Ok(Superblock {
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
        })
    }
    pub fn block_group_count(&self) -> u32 {
        let from_block_count = {
            if self.block_count % self.blocks_per_group != 0 {
                self.block_count / self.blocks_per_group + 1
            } else {
                self.block_count / self.blocks_per_group
            }
        };
        let from_inode_count = {
            if self.inode_count % self.inodes_per_group != 0 {
                self.inode_count / self.inodes_per_group + 1
            } else {
                self.inode_count / self.inodes_per_group
            }
        };
        assert_eq!(from_block_count, from_inode_count);
        from_block_count
    }
    pub fn inode_size(&self) -> u16 {
        self.extended.as_ref().map(|extended| extended.inode_struct_size).unwrap_or(128)
    }
}
#[derive(Debug)]
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
}
#[derive(Debug)]
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
}

#[derive(Debug)]
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
}
