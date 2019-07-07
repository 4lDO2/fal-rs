use std::{env, fs, io::{self, prelude::*}, mem};

use uuid::Uuid;

fn read_uuid<R: Read>(mut device: R) -> io::Result<Uuid> {
    let mut bytes = [0; mem::size_of::<Uuid>()];
    device.read_exact(&mut bytes)?;
    Ok(uuid::builder::Builder::from_bytes(bytes).build())
}
fn read_u32<R: Read>(mut device: R) -> io::Result<u32> {
    let mut bytes = [0; mem::size_of::<u32>()];
    device.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}
fn read_u16<R: Read>(mut device: R) -> io::Result<u16> {
    let mut bytes = [0; mem::size_of::<u16>()];
    device.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}
fn read_u8<R: Read>(mut device: R) -> io::Result<u8> {
    let mut bytes = [0; mem::size_of::<u8>()];
    device.read_exact(&mut bytes)?;
    Ok(u8::from_le_bytes(bytes))
}

mod ext2 {
    use super::{read_u8, read_u16, read_u32, read_uuid, Uuid};

    use std::{
        ffi::CString,
        io::{self, prelude::*, SeekFrom},
    };

    pub const SUPERBLOCK_OFFSET: u64 = 1024;
    pub const BASE_SUPERBLOCK_LEN: u64 = 84;
    pub const EXTENDED_SUPERBLOCK_LEN: u64 = 236;

    pub const SIGNATURE: u16 = 0xEF53;

    #[derive(Debug)]
    pub struct Superblock {
        inode_count: u32,
        block_count: u32,
        reserved_block_count: u32,
        unalloc_block_count: u32,
        unalloc_inode_count: u32,
        superblock_block_num: u32,
        block_size: u64,
        fragment_size: u64,
        blocks_per_group: u32,
        fragments_per_group: u32,
        inodes_per_group: u32,
        last_mount_time: u32,
        last_write_time: u32,
        mounts_since_fsck: u16,
        mounts_left_before_fsck: u16,
        fs_state: FilesystemState,
        error_handling: ErrorHandlingMethod,
        minor_version: u16,
        last_fsck_time: u32,
        interval_between_forced_fscks: u32,
        os_id: OsId,
        major_version: u32,
        reserver_uid: u16,
        reserver_gid: u16,
        extended: Option<SuperblockExtension>,
    }

    #[derive(Debug)]
    pub struct SuperblockExtension {
        first_nonreserved_inode: u32,
        inode_struct_size: u16,
        superblock_block_group: u16,
        opt_features_present_raw: u32,
        req_features_present_raw: u32,
        req_features_for_rw: u32,
        fs_id: Uuid,
        vol_name: Option<CString>,
        last_mount_path: Option<CString>,
        compression_algorithms: u32,
        file_prealloc_block_count: u8,
        dir_prealloc_block_count: u8,
        journal_id: Uuid,
        journal_inode: u32,
        journal_device: u32,
        orphan_inode_head_list: u32,
    }

    impl Superblock {
        pub fn parse<R: Read + Seek>(mut device: R) -> io::Result<Self> {
            device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET)).unwrap();

            let inode_count = read_u32(&mut device)?;
            let block_count = read_u32(&mut device)?;
            let reserved_block_count = read_u32(&mut device)?;
            let unalloc_block_count = read_u32(&mut device)?;
            let unalloc_inode_count = read_u32(&mut device)?;
            let superblock_block_num = read_u32(&mut device)?;
            let block_size = 1024 << read_u32(&mut device)?;
            let fragment_size = 1024 << read_u32(&mut device)?;
            let blocks_per_group = read_u32(&mut device)?;
            let fragments_per_group = read_u32(&mut device)?;
            let inodes_per_group = read_u32(&mut device)?;
            let last_mount_time = read_u32(&mut device)?;
            let last_write_time = read_u32(&mut device)?;
            let mounts_since_fsck = read_u16(&mut device)?;
            let mounts_left_before_fsck = read_u16(&mut device)?;
            let signature = read_u16(&mut device)?;
            assert_eq!(signature, SIGNATURE);
            let fs_state = FilesystemState::try_parse(read_u16(&mut device)?).unwrap();
            let error_handling = ErrorHandlingMethod::try_parse(read_u16(&mut device)?).unwrap();
            let minor_version = read_u16(&mut device)?;
            let last_fsck_time = read_u32(&mut device)?;
            let interval_between_forced_fscks = read_u32(&mut device)?;
            let os_id = OsId::try_parse(read_u32(&mut device)?).unwrap();
            let major_version = read_u32(&mut device)?;
            let reserver_uid = read_u16(&mut device)?;
            let reserver_gid = read_u16(&mut device)?;

            assert_eq!(device.seek(SeekFrom::Current(0))? - SUPERBLOCK_OFFSET, BASE_SUPERBLOCK_LEN);

            let extended = if major_version >= 1 {
                let first_nonreserved_inode = read_u32(&mut device)?;
                let inode_struct_size = read_u16(&mut device)?;
                let superblock_block_group = read_u16(&mut device)?;
                let opt_features_present_raw = read_u32(&mut device)?;
                let req_features_present_raw = read_u32(&mut device)?;
                let req_features_for_rw = read_u32(&mut device)?;
                let fs_id = read_uuid(&mut device)?;

                let vol_name = {
                    let mut vol_name_raw = [0u8; 16];
                    device.read_exact(&mut vol_name_raw)?;

                    let nul_position = vol_name_raw.iter().copied().position(|byte| byte == 0).unwrap_or(vol_name_raw.len());
                    if nul_position != 0 {
                        CString::new(&vol_name_raw[..nul_position]).ok()
                    } else {
                        None
                    }
                };

                let last_mount_path = {
                    let mut last_mount_path_raw = [0u8; 64];
                    device.read_exact(&mut last_mount_path_raw)?;

                    let nul_position = last_mount_path_raw.iter().copied().position(|byte| byte == 0).unwrap_or(last_mount_path_raw.len());
                    if nul_position != 0 {
                        CString::new(&last_mount_path_raw[..nul_position]).ok()
                    } else {
                        None
                    }
                };

                let compression_algorithms = read_u32(&mut device)?;
                let file_prealloc_block_count = read_u8(&mut device)?;
                let dir_prealloc_block_count = read_u8(&mut device)?;

                device.seek(SeekFrom::Current(2))?;

                let journal_id = read_uuid(&mut device)?;
                let journal_inode = read_u32(&mut device)?;
                let journal_device = read_u32(&mut device)?;
                let orphan_inode_head_list = read_u32(&mut device)?;

                assert_eq!(device.seek(SeekFrom::Current(0))? - SUPERBLOCK_OFFSET, EXTENDED_SUPERBLOCK_LEN);

                Some(SuperblockExtension {
                    first_nonreserved_inode,
                    inode_struct_size,
                    superblock_block_group,
                    opt_features_present_raw,
                    req_features_present_raw,
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
}

fn main() {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(env::args().nth(1).unwrap()).unwrap();

    let superblock = ext2::Superblock::parse(&mut file).unwrap();
    println!("{:?}", superblock);
}
