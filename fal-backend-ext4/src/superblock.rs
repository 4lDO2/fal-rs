use std::{
    io::{self, SeekFrom},
    ops::{AddAssign, ShrAssign},
};

use bitflags::bitflags;
use quick_error::quick_error;
use scroll::{Pread, Pwrite};

use crate::calculate_crc32c;

pub const SUPERBLOCK_OFFSET: u64 = 1024;
pub const SUPERBLOCK_LEN: u64 = 1024;

pub const SIGNATURE: u16 = 0xEF53;

#[derive(Debug, Pread, Pwrite)]
pub struct SuperblockBase {
    pub inode_count: u32,
    pub block_count: u32,
    pub reserved_block_count: u32,
    pub unalloc_block_count: u32,
    pub unalloc_inode_count: u32,
    pub superblock_block_num: u32,
    pub log_block_size: u32,
    pub log_fragment_size: u32,
    pub blocks_per_group: u32,
    pub fragments_per_group: u32,
    pub inodes_per_group: u32,
    pub last_mount_time: u32,
    pub last_write_time: u32,
    pub mounts_since_fsck: u16,
    pub mounts_left_before_fsck: u16,
    pub signature: u16,
    pub fs_state: u16,
    pub error_handling: u16,
    pub minor_version: u16,
    pub last_fsck_time: u32,
    pub interval_between_forced_fscks: u32,
    pub os_id: u32,
    pub major_version: u32,
    pub reserver_uid: u16,
    pub reserver_gid: u16,
}

#[derive(Pread, Pwrite)]
pub struct SuperblockExtension {
    pub first_nonreserved_inode: u32,
    pub inode_struct_size: u16,
    pub superblock_block_group: u16,
    pub opt_features_present: u32,
    pub req_features_present: u32,
    pub req_features_for_rw: u32,
    pub fs_id: [u8; 16],
    pub vol_name: [u8; 16],
    pub last_mount_path: [u8; 64],
    pub compression_algorithms: u32,
    pub file_prealloc_block_count: u8,
    pub dir_prealloc_block_count: u8,
    pub reserved_gdt_blocks: u16,
    pub journal_id: [u8; 16],
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

    pub mounts_opts: [u8; 64],
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

    pub zero_padding: u16,
    pub encoding: u16,
    pub encoding_flags: u16,

    pub reserved_bytes: [u8; 380],
    pub superblock_checksum: u32,
}

pub struct Superblock {
    pub base: SuperblockBase,
    pub extended: Option<SuperblockExtension>,
}

impl std::ops::Deref for Superblock {
    type Target = SuperblockBase;

    fn deref(&self) -> &SuperblockBase {
        &self.base
    }
}
impl std::ops::DerefMut for Superblock {
    fn deref_mut(&mut self) -> &mut SuperblockBase {
        &mut self.base
    }
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

// Not used yet, but it will be necessary when filesystem creation is implemented.
fn _log2_round_up<T: From<u8> + AddAssign + ShrAssign + Eq>(mut t: T) -> T {
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

quick_error! {
    #[derive(Debug)]
    pub enum LoadSuperblockError {
        ParseError(err: scroll::Error) {
            description("superblock parsing error")
            from()
        }
        ChecksumMismatch {
            description("the superblock checksum was incorrect")
        }
        IoError(err: io::Error) {
            description("i/o error")
            from()
        }
    }
}

impl Superblock {
    /// Load and parse the super block.
    pub fn load<R: fal::Device>(device: &mut R) -> Result<Self, LoadSuperblockError> {
        device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET))?;

        let mut block_bytes = [0u8; 1024];
        device.read_exact(&mut block_bytes)?;

        let this = Self::parse(&block_bytes)?;

        if this.has_metadata_checksums() {
            if this.extended.as_ref().unwrap().superblock_checksum != Self::calculate_crc32c(&block_bytes) {
                return Err(LoadSuperblockError::ChecksumMismatch)
            }
        }
        Ok(this)
    }
    pub fn calculate_crc32c(bytes: &[u8]) -> u32 {
        calculate_crc32c(!0, &bytes[..1020])
    }
    pub fn parse(block_bytes: &[u8]) -> Result<Self, scroll::Error> {
        let base: SuperblockBase = block_bytes.pread_with(0, scroll::LE)?;
        let extended = if base.major_version >= 1 {
            Some(block_bytes.pread_with(84, scroll::LE)?)
        } else { None };

        Ok(Self {
            base,
            extended,
        })
    }

    pub fn incompat_features(&self) -> RequiredFeatureFlags {
        self.extended
            .as_ref()
            .map(|ext| RequiredFeatureFlags::from_bits(ext.req_features_present).unwrap())
            .unwrap_or_else(RequiredFeatureFlags::empty)
    }
    pub fn compat_features(&self) -> OptionalFeatureFlags {
        self.extended
            .as_ref()
            .map(|ext| OptionalFeatureFlags::from_bits(ext.opt_features_present).unwrap())
            .unwrap_or_else(OptionalFeatureFlags::empty)
    }
    pub fn block_size(&self) -> u32 {
        1024 << self.log_block_size
    }
    pub fn fragment_size(&self) -> u32 {
        1024 << self.log_fragment_size
    }
    pub fn ro_compat_features(&self) -> RoFeatureFlags {
        self.extended
            .as_ref()
            .map(|ext| RoFeatureFlags::from_bits(ext.req_features_for_rw).unwrap())
            .unwrap_or_else(RoFeatureFlags::empty)
    }
    pub fn uuid(&self) -> [u8; 16] {
        self.extended.as_ref().map(|ext| ext.fs_id).unwrap_or([0u8; 16])
    }
    pub fn checksum_seed(&self) -> Option<u32> {
        if self.incompat_features().contains(RequiredFeatureFlags::CSUM_SEED) {
            self.extended.as_ref().map(|ext| ext.checksum_seed)
        } else {
            self.extended.as_ref().map(|ext| calculate_crc32c(!0, &ext.fs_id))
        }
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer.pwrite_with(&self.base, 0, scroll::LE).unwrap();
        if let Some(ref extended) = self.extended {
            buffer.pwrite_with(extended, 84, scroll::LE).unwrap();
        }
    }

    pub fn store<D: fal::DeviceMut>(&mut self, device: &mut D) -> io::Result<()> {
        let mut block_bytes = [0u8; 1024];
        self.serialize(&mut block_bytes);

        if let Some(ref mut ext) = self.extended {
            ext.superblock_checksum = Self::calculate_crc32c(&block_bytes[..1020]);
            fal::write_u32(&mut block_bytes, 1020, ext.superblock_checksum);
        }

        device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET))?;
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
    pub fn has_metadata_checksums(&self) -> bool {
        self.ro_compat_features().contains(RoFeatureFlags::METADATA_CSUM)
    }
    pub fn is_64bit(&self) -> bool {
        self.incompat_features()
            .contains(RequiredFeatureFlags::_64_BIT)
    }
    pub fn os_id(&self) -> OsId {
        OsId::try_parse(self.os_id).unwrap()
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
    /*#[test]
    fn log2_round_up() {
        assert_eq!(super::log2_round_up(1024), 10);
        assert_eq!(super::log2_round_up(1), 0);
        assert_eq!(super::log2_round_up(65536), 16);
    }*/
}
