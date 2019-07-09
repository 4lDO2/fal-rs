use std::{
    convert::{TryFrom, TryInto},
    ffi::OsString,
    io::{self, prelude::*},
    mem,
};

use crate::{
    block_group, div_round_up, os_string_from_bytes, read_block, read_block_to, read_u16, read_u32, read_u8,
    superblock::Superblock, Filesystem,
};

pub const ROOT: u32 = 2;

#[derive(Debug)]
pub struct Inode {
    pub ty: InodeType,
    pub permissions: u16,
    pub uid: u16,
    pub size_low: u32,
    pub last_access_time: u32,
    pub creation_time: u32,
    pub last_modification_time: u32,
    pub deletion_time: u32,
    pub gid: u16,
    pub hard_link_count: u16,
    pub disk_sector_count: u32,
    pub flags: u32,
    pub os_specific_1: u32,
    pub direct_ptrs: [u32; 12],
    pub singly_indirect_ptr: u32,
    pub doubly_indirect_ptr: u32,
    pub triply_indirect_ptr: u32,
    pub generation_number: u32,

    pub extended_atribute_block: u32,
    pub size_high_or_acl: u32,
    pub fragment_baddr: u32,
    pub os_specific_2: [u8; 12],
}
#[derive(Debug)]
pub struct DirEntry {
    pub inode: u32,
    pub type_indicator: Option<InodeType>,
    pub name: OsString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InodeType {
    Unknown,
    Fifo,
    CharDev,
    Dir,
    BlockDev,
    File,
    Symlink,
    UnixSock,
}

impl InodeType {
    const TYPE_MASK: u16 = 0xF000;
    const PERM_MASK: u16 = 0x0FFF;
    pub fn from_type_and_perm(raw: u16) -> (Option<Self>, u16) {
        (
            match raw & Self::TYPE_MASK {
                0x1000 => Some(InodeType::Fifo),
                0x2000 => Some(InodeType::CharDev),
                0x4000 => Some(InodeType::Dir),
                0x6000 => Some(InodeType::BlockDev),
                0x8000 => Some(InodeType::File),
                0xA000 => Some(InodeType::Symlink),
                0xC000 => Some(InodeType::UnixSock),
                _ => None,
            },
            raw & Self::PERM_MASK,
        )
    }
    pub fn from_direntry_ty_indicator(indicator: u8) -> Option<Self> {
        match indicator {
            0 => Some(InodeType::Unknown),
            1 => Some(InodeType::File),
            2 => Some(InodeType::Dir),
            3 => Some(InodeType::CharDev),
            4 => Some(InodeType::BlockDev),
            5 => Some(InodeType::Fifo),
            6 => Some(InodeType::UnixSock),
            7 => Some(InodeType::Symlink),
            _ => None,
        }
    }
}

impl Inode {
    pub fn load<R: Read + Seek + Write>(
        filesystem: &mut Filesystem<R>,
        inode_address: u32,
    ) -> io::Result<Self> {
        if inode_address == 0 { return Err(io::Error::new(io::ErrorKind::NotFound, "no inode address (was 0)")) }

        debug_assert!(block_group::inode_exists(inode_address, filesystem)?);

        let block_group_index =
            block_group::inode_block_group_index(&filesystem.superblock, inode_address);
        let block_group_descriptor =
            block_group::load_block_group_descriptor(filesystem, block_group_index)?;
        let inode_index_in_group = block_group::inode_index_inside_group(&filesystem.superblock, inode_address);
        let inode_size = filesystem.superblock.inode_size();

        let containing_block_index = block_group_descriptor.inode_table_start_baddr
            + u32::try_from(
                u64::from(inode_index_in_group * u32::from(inode_size))
                    / filesystem.superblock.block_size,
            )
            .unwrap();

        let max_inodes_in_block =
            u32::try_from(filesystem.superblock.block_size / u64::from(inode_size)).unwrap();

        let inode_index_in_block =
            usize::try_from(inode_index_in_group % max_inodes_in_block).unwrap();
        let inode_size = usize::from(inode_size);

        let containing_block = read_block(filesystem, containing_block_index)?;
        let inode_bytes = &containing_block
            [inode_index_in_block * inode_size..inode_index_in_block * inode_size + inode_size];

        if inode_address == 344065 {
            dbg!();
        }
        Ok(Self::parse(inode_bytes))
    }
    pub fn parse(bytes: &[u8]) -> Self {
        let (ty, permissions) = InodeType::from_type_and_perm(read_u16(bytes, 0));

        Self {
            ty: ty.unwrap(),
            permissions,
            uid: read_u16(bytes, 2),
            size_low: read_u32(bytes, 4),
            last_access_time: read_u32(bytes, 8),
            creation_time: read_u32(bytes, 12),
            last_modification_time: read_u32(bytes, 16),
            deletion_time: read_u32(bytes, 20),
            gid: read_u16(bytes, 24),
            hard_link_count: read_u16(bytes, 26),
            disk_sector_count: read_u32(bytes, 28),
            flags: read_u32(bytes, 32),
            os_specific_1: read_u32(bytes, 36),
            direct_ptrs: {
                let mut direct_ptrs = [0u32; 12];
                for index in 0..12 {
                    let mut le_bytes = [0u8; mem::size_of::<u32>()];
                    le_bytes.copy_from_slice(&bytes[40 + index * 4..40 + index * 4 + 4]);
                    let direct_ptr = u32::from_le_bytes(le_bytes);
                    direct_ptrs[index] = direct_ptr;
                }
                direct_ptrs
            },
            singly_indirect_ptr: read_u32(bytes, 88),
            doubly_indirect_ptr: read_u32(bytes, 92),
            triply_indirect_ptr: read_u32(bytes, 96),
            generation_number: read_u32(bytes, 100),
            extended_atribute_block: read_u32(bytes, 104),
            size_high_or_acl: read_u32(bytes, 108),
            fragment_baddr: read_u32(bytes, 112),
            os_specific_2: {
                let mut os_specific_bytes = [0u8; 12];
                os_specific_bytes.copy_from_slice(&bytes[116..128]);
                os_specific_bytes
            },
        }
    }
    pub fn size(&self, superblock: &Superblock) -> u64 {
        u64::from(self.size_low)
            | if let Some(extended) = superblock.extended.as_ref() {
                if extended.req_features_for_rw.extended_file_size && self.ty == InodeType::File {
                    u64::from(self.size_high_or_acl) << 32
                } else {
                    0
                }
            } else {
                0
            }
    }
    pub fn ls<D: Read + Seek + Write>(
        &self,
        filesystem: &mut Filesystem<D>,
    ) -> io::Result<Vec<DirEntry>> {
        if self.ty != InodeType::Dir {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let size = self.size(&filesystem.superblock);

        if size == 0 {
            return Ok(vec! [])
        }

        let mut entries = vec! [];

        let mut current_entry_offset = 0;

        let mut entry_bytes = vec! [0; 6];

        while current_entry_offset < size {
            entry_bytes.clear();
            entry_bytes.resize(6, 0);

            self.read(filesystem, current_entry_offset, &mut entry_bytes[..6])?;
            let length = DirEntry::length(&entry_bytes[0..6]).try_into().unwrap();

            if length == 0 { break } // TODO: Assuming the entry with length 0 is the last one, is this correct?

            entry_bytes.resize(length, 0);
            self.read(filesystem, current_entry_offset, &mut entry_bytes[..length])?;

            entries.push(DirEntry::parse(&filesystem.superblock, &entry_bytes));
            current_entry_offset += u64::try_from(length).unwrap();
        }
        Ok(entries)
    }
    pub fn read_block_to<D: Read + Seek + Write>(&self, rel_baddr: u32, filesystem: &mut Filesystem<D>, buffer: &mut [u8]) -> io::Result<()> {
        if u64::from(rel_baddr) >= div_round_up(self.size(&filesystem.superblock), filesystem.superblock.block_size) {
            return Err(io::ErrorKind::UnexpectedEof.into())
        }

        if rel_baddr <= u32::try_from(filesystem.superblock.block_size * u64::try_from(self.direct_ptrs.len()).unwrap()).unwrap()  {
            read_block_to(filesystem, self.direct_ptrs[usize::try_from(rel_baddr).unwrap()], buffer)
        } else {
            unimplemented!()
        }
    }
    pub fn read<D: Read + Write + Seek>(&self, filesystem: &mut Filesystem<D>, offset: u64, mut buffer: &mut [u8]) -> io::Result<()> {
        let off_from_rel_block = offset % filesystem.superblock.block_size;
        let rel_baddr_start = offset / filesystem.superblock.block_size;

        let mut block_bytes = (vec! [0u8; usize::try_from(filesystem.superblock.block_size).unwrap()]).into_boxed_slice();

        if off_from_rel_block != 0 {
            self.read_block_to(rel_baddr_start.try_into().unwrap(), filesystem, &mut block_bytes)?;

            {
                let off_from_rel_block = usize::try_from(off_from_rel_block).unwrap();
                let end = std::cmp::min(buffer.len(), usize::try_from(filesystem.superblock.block_size).unwrap() - off_from_rel_block);
                buffer[..end].copy_from_slice(&block_bytes[off_from_rel_block..off_from_rel_block + end]);
            }

            if u64::try_from(buffer.len()).unwrap() >= off_from_rel_block {
                return self.read(filesystem, offset - off_from_rel_block, &mut buffer[off_from_rel_block.try_into().unwrap()..]);
            } else {
                return Ok(())
            }
        }

        let mut current_rel_baddr = 0;

        while buffer.len() >= usize::try_from(filesystem.superblock.block_size).unwrap() {

            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;
            buffer[..usize::try_from(filesystem.superblock.block_size).unwrap()].copy_from_slice(&block_bytes);

            if buffer.len() > usize::try_from(filesystem.superblock.block_size).unwrap() {
                buffer = &mut buffer[usize::try_from(filesystem.superblock.block_size).unwrap()..];
            }
            current_rel_baddr += 1;
        }

        if buffer.len() != 0 {
            self.read_block_to(current_rel_baddr.try_into().unwrap(), filesystem, &mut block_bytes)?;
            let buffer_len = buffer.len();
            buffer.copy_from_slice(&block_bytes[..buffer_len]);
        }

        Ok(())
    }
}

impl DirEntry {
    pub fn length(bytes: &[u8]) -> u16 {
        assert_eq!(bytes.len(), mem::size_of::<u32>() + mem::size_of::<u16>());
        if read_u32(bytes, 0) != 0 {
            read_u16(bytes, 4)
        } else {
            0
        }
    }
    pub fn parse(superblock: &Superblock, bytes: &[u8]) -> Self {
        let total_entry_size = read_u16(bytes, 4);

        let name_length = total_entry_size - 8;
        let name_bytes = &bytes[8..8 + usize::try_from(name_length).unwrap()];
        let name_bytes = &name_bytes[..name_bytes
            .iter()
            .copied()
            .position(|byte| byte == 0)
            .unwrap_or(name_bytes.len())];
        let name = os_string_from_bytes(name_bytes);
        Self {
            inode: read_u32(bytes, 0),
            type_indicator: if let Some(extended) = superblock.extended.as_ref() {
                if extended.opt_features_present.inode_extended_attributes {
                    InodeType::from_direntry_ty_indicator(read_u8(bytes, 7))
                } else {
                    None
                }
            } else {
                None
            },
            name,
        }
    }
}