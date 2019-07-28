use std::{
    ffi::CString,
    fmt,
    fs::File,
    io::{prelude::*, SeekFrom},
};

use fs_core::{read_u8, read_u16, read_u32, read_u64, read_uuid, write_u8, write_u64};

const SUPERBLOCK_OFFSET: u64 = 65536;
const CHECKSUM_SIZE: usize = 32;
const MAGIC: u64 = 0x4D5F53665248425F;

#[derive(Debug)]
pub struct Superblock {
    checksum: [u8; CHECKSUM_SIZE],
    fs_id: uuid::Uuid,
    block_number: u64,
    flags: u64,
    magic: u64,
    generation: u64,
    root: u64,
    chunk_root: u64,
    log_root: u64,

    log_root_transid: u64,
    total_byte_count: u64,
    total_bytes_used: u64,
    root_dir_objectid: u64,
    device_count: u64,

    sector_size: u32,
    node_size: u32,
    unused_leaf_size: u32,
    stripe_size: u32,
    system_chunk_array_size: u32,
    chunk_root_gen: u64,

    optional_flags: u64,
    flags_for_write_support: u64,
    required_flags: u64,

    checksum_type: ChecksumType,

    root_level: u8,
    chunk_root_level: u8,
    log_root_level: u8,

    device_properties: DeviceProperties,
    device_label: CString, // TODO: Is this really a C string?

    cache_generation: u8,
    uuid_tree_generation: u8,
    metadata_uuid: uuid::Uuid,
    system_chunk_array: SystemChunkArray,
    root_backups: [RootBackup; 4],
}
pub struct SystemChunkArray([u8; 2048]);

impl fmt::Debug for SystemChunkArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("SystemChunkArray").finish()
    }
}

#[derive(Debug)]
pub struct DeviceProperties {
    id: u64,
    size: u64,
    bytes_used: u64,
    io_alignment: u32,
    io_width: u32,
    sector_size: u32,
    type_and_info: u64,
    generation: u64,
    start_byte: u64,
    group: u32,
    seek_speed: u8,
    bandwidth: u8,
    uuid: uuid::Uuid,
    fs_uuid: uuid::Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChecksumType {
    Crc32,
}
impl ChecksumType {
    const CRC32_RAW: u16 = 0;

    pub fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            Self::CRC32_RAW => Some(ChecksumType::Crc32),
            _ => None,
        }
    }
    pub fn to_raw(this: Self) -> u16 {
        match this {
            ChecksumType::Crc32 => Self::CRC32_RAW,
        }
    }
}

impl Superblock {
    pub fn parse(mut file: File) -> Self {
        file.seek(SeekFrom::Start(SUPERBLOCK_OFFSET)).unwrap();

        let mut block = vec![0u8; 16384];
        file.read_exact(&mut block).unwrap();

        let mut checksum = [0u8; CHECKSUM_SIZE];
        checksum.copy_from_slice(&block[..=31]);

        let fs_id = read_uuid(&block, 32);

        let block_number = read_u64(&block, 48);
        let flags = read_u64(&block, 56);
        let magic = read_u64(&block, 64);
        assert_eq!(magic, MAGIC);
        let generation = read_u64(&block, 72);
        let root = read_u64(&block, 80);
        let chunk_root = read_u64(&block, 88);
        let log_root = read_u64(&block, 96);

        let log_root_transid = read_u64(&block, 104);
        let total_byte_count = read_u64(&block, 112);
        let total_bytes_used = read_u64(&block, 120);
        let root_dir_objectid = read_u64(&block, 128);
        let device_count = read_u64(&block, 136);

        let sector_size = read_u32(&block, 144);
        let node_size = read_u32(&block, 148);
        let unused_leaf_size = read_u32(&block, 152);
        let stripe_size = read_u32(&block, 156);
        let system_chunk_array_size = read_u32(&block, 160);
        let chunk_root_gen = read_u64(&block, 164);

        let optional_flags = read_u64(&block, 172);
        let flags_for_write_support = read_u64(&block, 180);
        let required_flags = read_u64(&block, 188);

        let checksum_type = ChecksumType::from_raw(read_u16(&block, 196)).unwrap();

        let root_level = read_u8(&block, 198);
        let chunk_root_level = read_u8(&block, 199);
        let log_root_level = read_u8(&block, 200);

        let device_properties = {
            let id = read_u64(&block, 201);
            let size = read_u64(&block, 209);
            let bytes_used = read_u64(&block, 217);
            let io_alignment = read_u32(&block, 225);
            let io_width = read_u32(&block, 229);
            let sector_size = read_u32(&block, 233);
            let type_and_info = read_u64(&block, 237);
            let generation = read_u64(&block, 245);
            let start_byte = read_u64(&block, 253);
            let group = read_u32(&block, 261);
            let seek_speed = read_u8(&block, 265);
            let bandwidth = read_u8(&block, 266);
            let device_uuid = read_uuid(&block, 267);
            let fs_uuid = read_uuid(&block, 283);

            assert_eq!(fs_uuid, fs_id);

            DeviceProperties {
                id,
                size,
                bytes_used,
                io_alignment,
                io_width,
                sector_size,
                type_and_info,
                generation,
                start_byte,
                group,
                seek_speed,
                bandwidth,
                fs_uuid,
                uuid: device_uuid,
            }
        };

        let device_label = {
            let label_bytes = &block[299..=554];
            let nul_position = label_bytes.iter().copied().position(|byte| byte == 0).unwrap();
            let label_bytes_to_nul = &label_bytes[..nul_position];
            CString::new(label_bytes_to_nul).unwrap()
        };

        let cache_generation = read_u8(&block, 555);
        let uuid_tree_generation = read_u8(&block, 556);
        let metadata_uuid = read_uuid(&block, 557);

        let mut system_chunk_array = [0u8; 2048];
        system_chunk_array.copy_from_slice(&block[811..=2858]);

        let mut root_backups = [Default::default(); 4];
        for (index, backup) in root_backups.iter_mut().enumerate() {
            *backup = RootBackup::from_raw(&block[2859 + index * RootBackup::RAW_SIZE .. 2859 + (index + 1) * RootBackup::RAW_SIZE]);
        }

        Self {
            checksum,
            fs_id,
            block_number,
            flags,
            magic,
            generation,
            root,
            chunk_root,
            log_root,
            log_root_transid,
            total_byte_count,
            total_bytes_used,
            root_dir_objectid,
            device_count,
            sector_size,
            node_size,
            unused_leaf_size,
            stripe_size,
            system_chunk_array_size,
            chunk_root_gen,
            optional_flags,
            flags_for_write_support,
            required_flags,
            checksum_type,
            root_level,
            chunk_root_level,
            log_root_level,
            device_properties,
            device_label,
            cache_generation,
            uuid_tree_generation,
            metadata_uuid,
            system_chunk_array: SystemChunkArray(system_chunk_array),
            root_backups,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RootBackup {
    tree_root: u64,
    tree_root_generation: u64,
    chunk_root: u64,
    chunk_root_generation: u64,
    extent_root: u64,
    extent_root_generation: u64,
    filesystem_root: u64,
    filesystem_root_generation: u64,
    device_root: u64,
    device_root_generation: u64,
    checksum_root: u64,
    checksum_root_generation: u64,
    total_bytes: u64,
    bytes_used: u64,
    device_count: u64,
    tree_root_level: u8,
    chunk_root_level: u8,
    extent_root_level: u8,
    filesystem_root_level: u8,
    device_root_level: u8,
    checksum_root_level: u8,
}

impl RootBackup {
    pub const RAW_SIZE: usize = 168;

    pub fn from_raw(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= Self::RAW_SIZE);

        Self {
            tree_root: read_u64(bytes, 0),
            tree_root_generation: read_u64(bytes, 8),
            chunk_root: read_u64(bytes, 16),
            chunk_root_generation: read_u64(bytes, 24),
            extent_root: read_u64(bytes, 32),
            extent_root_generation: read_u64(bytes, 40),
            filesystem_root: read_u64(bytes, 48),
            filesystem_root_generation: read_u64(bytes, 56),
            device_root: read_u64(bytes, 64),
            device_root_generation: read_u64(bytes, 72),
            checksum_root: read_u64(bytes, 80),
            checksum_root_generation: read_u64(bytes, 88),
            total_bytes: read_u64(bytes, 96),
            bytes_used: read_u64(bytes, 104),
            device_count: read_u64(bytes, 112),
            // 120..=151 unused
            tree_root_level: read_u8(bytes, 152),
            chunk_root_level: read_u8(bytes, 153),
            extent_root_level: read_u8(bytes, 154),
            filesystem_root_level: read_u8(bytes, 155),
            device_root_level: read_u8(bytes, 156),
            checksum_root_level: read_u8(bytes, 157),
            // 158..=167 unused
        }
    }
    pub fn to_raw(this: Self, bytes: &mut [u8]) {
        assert!(bytes.len() >= Self::RAW_SIZE);

        write_u64(bytes, 0, this.tree_root);
        write_u64(bytes, 8, this.tree_root_generation);
        write_u64(bytes, 16, this.chunk_root);
        write_u64(bytes, 24, this.chunk_root_generation);
        write_u64(bytes, 32, this.extent_root);
        write_u64(bytes, 40, this.extent_root_generation);
        write_u64(bytes, 48, this.filesystem_root);
        write_u64(bytes, 56, this.filesystem_root_generation);
        write_u64(bytes, 64, this.device_root);
        write_u64(bytes, 72, this.device_root_generation);
        write_u64(bytes, 80, this.checksum_root);
        write_u64(bytes, 88, this.checksum_root_generation);
        write_u64(bytes, 96, this.total_bytes);
        write_u64(bytes, 104, this.bytes_used);
        write_u64(bytes, 112, this.device_count);
        // 120..=151 unused
        write_u8(bytes, 152, this.tree_root_level);
        write_u8(bytes, 153, this.chunk_root_level);
        write_u8(bytes, 154, this.extent_root_level);
        write_u8(bytes, 155, this.filesystem_root_level);
        write_u8(bytes, 156, this.device_root_level);
        write_u8(bytes, 157, this.checksum_root_level);
        // 158..=167 unused
    }
}
