use std::{
    ffi::CString,
    io::SeekFrom,
};

use crate::{Checksum, DiskKey, DiskKeyType, items::{BlockGroupType, ChunkItem}, sizes};

use bitflags::bitflags;
use enum_primitive::*;
use fal::{read_u16, read_u32, read_u64, read_u8, read_uuid, write_u64, write_u8};

const SUPERBLOCK_OFFSETS: [u64; 4] = [64 * sizes::K, 64 * sizes::M, 256 * sizes::G, 1 * sizes::P];
const CHECKSUM_SIZE: usize = 32;
const MAGIC: u64 = 0x4D5F53665248425F; // ASCII for "_BHRfS_M"

#[derive(Debug)]
pub struct Superblock {
    pub checksum: Checksum,
    pub fs_id: uuid::Uuid,
    pub byte_number: u64,
    pub flags: SuperblockFlags,
    pub magic: u64,
    pub generation: u64,
    pub root: u64,
    pub chunk_root: u64,
    pub log_root: u64,

    pub log_root_transid: u64,
    pub total_byte_count: u64,
    pub total_bytes_used: u64,
    pub root_dir_objectid: u64,
    pub device_count: u64,

    pub sector_size: u32,
    pub node_size: u32,
    pub unused_leaf_size: u32,
    pub stripe_size: u32,
    pub system_chunk_array_size: u32,
    pub chunk_root_gen: u64,

    pub optional_flags: u64,
    pub flags_for_write_support: u64,
    pub required_flags: u64,

    pub checksum_type: ChecksumType,

    pub root_level: u8,
    pub chunk_root_level: u8,
    pub log_root_level: u8,

    pub device_properties: DeviceProperties,
    pub device_label: CString, // TODO: Is this really a C string?

    pub cache_generation: u8,
    pub uuid_tree_generation: u8,
    pub metadata_uuid: uuid::Uuid,
    pub system_chunk_array: SystemChunkArray,
    pub root_backups: [RootBackup; 4],
}
#[derive(Debug)]
pub struct SystemChunkArray(pub Vec<(DiskKey, ChunkItem)>);

#[derive(Debug)]
pub struct DeviceProperties {
    pub id: u64,
    pub size: u64,
    pub bytes_used: u64,
    pub io_alignment: u32,
    pub io_width: u32,
    pub sector_size: u32,
    pub type_and_info: u64,
    pub generation: u64,
    pub start_byte: u64,
    pub group: u32,
    pub seek_speed: u8,
    pub bandwidth: u8,
    pub uuid: uuid::Uuid,
    pub fs_uuid: uuid::Uuid,
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum ChecksumType {
        Crc32 = 0,
    }
}

impl Superblock {
    pub fn load<D: fal::Device>(device: &mut D) -> Self {
        let disk_size = device.seek(SeekFrom::End(0)).unwrap();

        let mut block = [0u8; 4096];

        SUPERBLOCK_OFFSETS.iter().copied().filter(|offset| offset + 4096 < disk_size).map(|offset| {
            device.seek(SeekFrom::Start(offset)).unwrap();
            device.read_exact(&mut block).unwrap();

            Self::parse(&block)
        }).max_by_key(|sb| sb.generation).unwrap()
    }
    pub fn parse(block: &[u8]) -> Self {
        let mut checksum = [0u8; CHECKSUM_SIZE];
        checksum.copy_from_slice(&block[..32]);

        let fs_id = read_uuid(&block, 32);

        let byte_number = read_u64(&block, 48);
        let flags = SuperblockFlags::from_bits(read_u64(&block, 56)).unwrap();
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

        let checksum_type = ChecksumType::from_u16(read_u16(&block, 196)).unwrap();

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
            let nul_position = label_bytes
                .iter()
                .copied()
                .position(|byte| byte == 0)
                .unwrap();
            let label_bytes_to_nul = &label_bytes[..nul_position];
            CString::new(label_bytes_to_nul).unwrap()
        };

        let cache_generation = read_u8(&block, 555);
        let uuid_tree_generation = read_u8(&block, 556);
        let metadata_uuid = read_uuid(&block, 557);

        let system_chunk_array = &block[811..=2858];

        let mut root_backups = [Default::default(); 4];
        for (index, backup) in root_backups.iter_mut().enumerate() {
            *backup = RootBackup::from_raw(
                &block[2859 + index * RootBackup::RAW_SIZE
                    ..2859 + (index + 1) * RootBackup::RAW_SIZE],
            );
        }

        Self {
            checksum: Checksum::new(checksum_type, &checksum),
            fs_id,
            byte_number,
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
            system_chunk_array: SystemChunkArray::parse(&system_chunk_array[..system_chunk_array_size as usize]),
            root_backups,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RootBackup {
    pub tree_root: u64,
    pub tree_root_generation: u64,
    pub chunk_root: u64,
    pub chunk_root_generation: u64,
    pub extent_root: u64,
    pub extent_root_generation: u64,
    pub filesystem_root: u64,
    pub filesystem_root_generation: u64,
    pub device_root: u64,
    pub device_root_generation: u64,
    pub checksum_root: u64,
    pub checksum_root_generation: u64,
    pub total_bytes: u64,
    pub bytes_used: u64,
    pub device_count: u64,
    pub tree_root_level: u8,
    pub chunk_root_level: u8,
    pub extent_root_level: u8,
    pub filesystem_root_level: u8,
    pub device_root_level: u8,
    pub checksum_root_level: u8,
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




impl SystemChunkArray {
    pub fn parse(bytes: &[u8]) -> Self {
        let stride = DiskKey::LEN + ChunkItem::LEN;

        let pairs = (0..bytes.len() / stride).map(|i| {
            let key_bytes = &bytes[i * stride .. i * stride + DiskKey::LEN];
            let chunk_bytes = &bytes[i * stride + DiskKey::LEN  .. (i + 1) * stride];

            let key = DiskKey::parse(key_bytes);
            assert_eq!(key.ty, DiskKeyType::ChunkItem);

            let chunk = ChunkItem::parse(chunk_bytes);
            assert!(chunk.ty.contains(BlockGroupType::SYSTEM));

            // Only RAID 0 is supported so far.
            assert_eq!(chunk.stripe_count, 1, "Unimplemented RAID configuration with stripe count {}", chunk.stripe_count);
            assert_eq!(chunk.sub_stripe_count, 0, "Unimplemented RAID configuration with sub stripe count (used for RAID 10) {}", chunk.sub_stripe_count);

            (key, chunk)
        }).collect();

        Self(pairs)
    }
}

bitflags! {
    pub struct SuperblockFlags: u64 {
        const WRITTEN = 1 << 0;
        const RELOC = 1 << 1;

        const ERROR = 1 << 2;
        const SEEDING = 1 << 32;
        const METADUMP = 1 << 33;
        const METADUMP_V2 = 1 << 34;
        const CHANGING_FSID = 1 << 35;
        const CHANGING_FSID_V2 = 1 << 36;
    }
}
