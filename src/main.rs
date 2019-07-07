use std::{env, fs, io::{self, prelude::*}, mem};

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

mod ext2 {
    use super::{read_u16, read_u32};
    use std::io::{self, prelude::*, SeekFrom};

    pub const SUPERBLOCK_OFFSET: u64 = 1024;
    pub const FS_SIGNATURE: u16 = 0xEF53;

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
        signature: u16,
        fs_state: FilesystemState,
        error_handling: ErrorHandlingMethod,
        minor_version: u16,
        last_fsck_time: u32,
        interval_between_forced_fscks: u32,
        os_id: OsId,
        major_version: u32,
        reserver_uid: u32,
        reserver_gid: u32,
    }
    impl Superblock {
        pub fn parse<R: Read + Seek>(mut device: R) -> io::Result<Self> {
            device.seek(SeekFrom::Start(SUPERBLOCK_OFFSET)).unwrap();

            Ok(Superblock {
                inode_count: read_u32(&mut device)?,
                block_count: read_u32(&mut device)?,
                reserved_block_count: read_u32(&mut device)?,
                unalloc_block_count: read_u32(&mut device)?,
                unalloc_inode_count: read_u32(&mut device)?,
                superblock_block_num: read_u32(&mut device)?,
                block_size: 1024 << read_u32(&mut device)?,
                fragment_size: 1024 << read_u32(&mut device)?,
                blocks_per_group: read_u32(&mut device)?,
                fragments_per_group: read_u32(&mut device)?,
                inodes_per_group: read_u32(&mut device)?,
                last_mount_time: read_u32(&mut device)?,
                last_write_time: read_u32(&mut device)?,
                mounts_since_fsck: read_u16(&mut device)?,
                mounts_left_before_fsck: read_u16(&mut device)?,
                signature: {
                    let signature = read_u16(&mut device)?;
                    assert_eq!(signature, FS_SIGNATURE);
                    signature
                },
                fs_state: FilesystemState::try_parse(read_u16(&mut device)?).unwrap(),
                error_handling: ErrorHandlingMethod::try_parse(read_u16(&mut device)?).unwrap(),
                minor_version: read_u16(&mut device)?,
                last_fsck_time: read_u32(&mut device)?,
                interval_between_forced_fscks: read_u32(&mut device)?,
                os_id: OsId::try_parse(read_u32(&mut device)?).unwrap(),
                major_version: read_u32(&mut device)?,
                reserver_uid: read_u32(&mut device)?,
                reserver_gid: read_u32(&mut device)?,
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
