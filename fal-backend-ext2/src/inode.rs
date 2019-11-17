use std::{
    convert::{TryFrom, TryInto},
    ffi::{OsStr, OsString},
    fmt, io, mem,
};

use crate::{
    block_group, os_str_to_bytes, os_string_from_bytes, read_block, read_block_to, read_u16,
    read_u32, read_u8,
    extents::ExtentTree,
    superblock::{OptionalFeatureFlags, OsId, RequiredFeatureFlags, RoFeatureFlags, Superblock},
    write_block, write_u16, write_u32, write_u8, Filesystem,
};

use bitflags::bitflags;
use fal::Timespec;
use scroll::{Pread, Pwrite};

pub const ROOT: u32 = 2;

pub const BASE_INODE_SIZE: u64 = 128;
pub const EXTENDED_INODE_SIZE: u64 = 160;

// XXX: Const generics.
#[derive(Clone, Copy, Pread, Pwrite)]
pub struct Blocks {
    pub inner: [u8; 60],
}

impl PartialEq for Blocks {
    fn eq(&self, other: &Blocks) -> bool {
        &self.inner[..] == &other.inner[..]
    }
}

impl Eq for Blocks {}

impl fmt::Debug for Blocks {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.inner[..] {
            write!(f, "{:x}", byte)?;
        }
        Ok(())
    }
}
impl std::hash::Hash for Blocks {
    fn hash<T: std::hash::Hasher>(&self, hasher: &mut T) {
        hasher.write(&self.inner)
    }
}

impl Blocks {
    pub fn block_ptrs(&self) -> &[u32] {
        bytemuck::cast_slice::<u8, u32>(&self.inner[..])
    }
    pub fn direct_ptrs(&self) -> &[u32] {
        &self.block_ptrs()[..12]
    }
    pub fn singly_indirect_ptr(&self) -> u32 {
        self.block_ptrs()[12]
    }
    pub fn doubly_indirect_ptr(&self) -> u32 {
        self.block_ptrs()[13]
    }
    pub fn triply_indirect_ptr(&self) -> u32 {
        self.block_ptrs()[14]
    }
    pub fn extent_tree(&self) -> ExtentTree {
        ExtentTree::from_inode_blocks_field(self).unwrap()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct InodeRawBase {
    pub mode: u16,
    pub uid: u16,
    pub size_lo: u32,
    
    pub a_time_lo: i32,
    pub c_time_lo: i32,
    pub m_time_lo: i32,
    pub d_time_lo: i32,

    pub gid: u16,
    pub hardlink_count: u16,

    pub block_count_lo: u32,
    pub flags: u32,
    pub os_specific_1: [u8; 4],
    pub blocks: Blocks,
    pub generation_lo: u32,

    pub file_acl_lo: u32,
    pub size_hi_or_dir_acl: u32,

    pub obsolete_faddr: u32,
    pub os_specific_2: [u8; 12],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct InodeRawExt {
    pub extra_isize: u16,
    pub checksum_hi: u16,
    pub c_time_extra: u32,
    pub m_time_extra: u32,
    pub a_time_extra: u32,
    pub cr_time_lo: i32,
    pub cr_time_extra: u32,
    pub generation_hi: u32,
    pub project_id: u32,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct InodeRaw {
    pub base: InodeRawBase,
    pub ext: Option<InodeRawExt>,
}

bitflags! {
    pub struct InodeFlags: u32 {
        const SECURE_DELETION = 0x1;
        const PRESERVED = 0x2;
        const COMPRESSED = 0x4;
        const SYNC = 0x8;
        const IMMUTABLE = 0x10;
        const APPEND_ONLY = 0x20;
        const NODUMP = 0x40;
        const NOATIME = 0x80;
        const DIRTY_COMPR = 0x100;
        const HAS_COMPR_CLUSTERS = 0x200;
        const DONT_COMPR = 0x400;
        const ENCRYPTED = 0x800;
        const DIR_HASH_IDX = 0x1000;
        const AFS_MAGIC = 0x2000;
        const ALWAYS_JOURNAL = 0x4000;
        const DONT_MERGE = 0x8000;
        const DIR_SYNC = 0x1_0000;
        const TOP_DIR = 0x2_0000;
        const HUGE_FILE = 0x4_0000;
        const EXTENTS = 0x8_0000;
        const VERIFY = 0x10_0000;
        const EA_INODE = 0x20_0000;
        const ALLOC_PAST_EOF = 0x40_0000;
        const IS_SNAPSHOT = 0x100_0000;
        const SNAP_DELETING = 0x400_0000;
        const SNAP_SHRUNK = 0x800_0000;
        const INLINE_DATA = 0x1000_0000;
        const PROJ_INHERIT = 0x2000_0000;
        const RESERVED = 0x8000_0000;
    }
}

impl InodeFlags {
    pub const AGGREGATE_FLAGS: Self = Self::empty();
    pub const USER_VISIBLE_FLAGS: Self = Self { bits: 0x705BDFFF };
    pub const USER_MODIFIABLE_FLAGS: Self = Self { bits: 0x604BC0FF };
}

impl InodeRaw {
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            base: bytes.pread_with(0, scroll::LE).unwrap(),
            ext: if bytes.len() >= EXTENDED_INODE_SIZE as usize {
                Some(bytes.pread_with(BASE_INODE_SIZE as usize, scroll::LE).unwrap())
            } else {
                None
            }
        }
    }
    pub fn serialize(this: &Self, bytes: &mut [u8]) {
        bytes.pwrite_with(&this.base, 0, scroll::LE).unwrap();

        if let Some(ref ext) = this.ext {
            bytes.pwrite_with(ext, EXTENDED_INODE_SIZE as usize, scroll::LE).unwrap();
        }
    }
    pub fn ty(&self) -> InodeType {
        InodeType::from_type_and_perm(self.base.mode).0.unwrap()
    }
    pub fn flags(&self) -> InodeFlags {
        InodeFlags::from_bits_truncate(self.flags)
    }
    pub fn permissions(&self) -> u16 {
        InodeType::from_type_and_perm(self.base.mode).1
    }
    pub fn set_permissions(&mut self, permissions: u16) {
        debug_assert_eq!(permissions & InodeType::PERM_MASK, permissions);
        let other = self.mode & !InodeType::PERM_MASK;
        self.mode = other | permissions;
    }
    pub fn set_uid(&mut self, uid: u32, os: OsId) {
        let lo = (uid & 0xFFFF) as u16;
        let hi = (uid >> 16) as u16;

        self.base.uid = lo;

        if os == OsId::Linux {
            fal::write_u16(&mut self.os_specific_2, 4, hi)
        } else if hi != 0 {
            panic!("Only Linux filesystems support 32-bit UIDs")
        }
    }
    pub fn set_gid(&mut self, gid: u32, os: OsId) {
        let lo = (gid & 0xFFFF) as u16;
        let hi = (gid >> 16) as u16;

        self.base.gid = lo;

        if os == OsId::Linux {
            fal::write_u16(&mut self.os_specific_2, 6, hi)
        } else if hi != 0 {
            panic!("Only Linux filesystems support 32-bit GIDs")
        }
    }
    pub fn size(&self, superblock: &Superblock) -> u64 {
        u64::from(self.base.size_lo) | if superblock.ro_compat_features().contains(RoFeatureFlags::EXTENDED_FILE_SIZE) && self.ty() != InodeType::Dir {
            u64::from(self.base.size_hi_or_dir_acl)
        } else { 0 } << 32
    }
    pub fn generation(&self) -> u64 {
        u64::from(self.base.generation_lo) | u64::from(self.ext.as_ref().map(|ext| ext.generation_hi).unwrap_or(0)) << 32
    }

    fn decode_timestamp(base: i32, extra: u32) -> Timespec {
        let epoch_ext = (extra & 0b11) as i32;

        Timespec {
            sec: i64::from(base) | i64::from(epoch_ext) << 32,
            nsec: (extra >> 2) as i32,
        }
    }

    pub fn a_time(&self) -> Timespec {
        Self::decode_timestamp(self.a_time_lo, self.ext.as_ref().map(|ext| ext.a_time_extra).unwrap_or(0))
    }
    pub fn c_time(&self) -> Timespec {
        Self::decode_timestamp(self.c_time_lo, self.ext.as_ref().map(|ext| ext.c_time_extra).unwrap_or(0))
    }
    pub fn m_time(&self) -> Timespec {
        Self::decode_timestamp(self.m_time_lo, self.ext.as_ref().map(|ext| ext.m_time_extra).unwrap_or(0))
    }
    pub fn d_time(&self) -> Timespec {
        Timespec {
            sec: self.d_time_lo.into(),
            nsec: 0,
        }
    }

    pub fn cr_time(&self) -> Option<Timespec> {
        self.ext.as_ref().map(|ext| Self::decode_timestamp(ext.cr_time_lo, ext.cr_time_extra))
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct Inode {
    pub block_size: u32,
    pub size: u64,
    pub addr: u32,
    pub os: OsId,
    pub raw: InodeRaw,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InodeType {
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
    pub fn to_type_and_perm((this, perm): (Self, u16)) -> u16 {
        assert_eq!(perm & Self::PERM_MASK, perm);

        (match this {
            InodeType::Fifo => 0x1000,
            InodeType::CharDev => 0x2000,
            InodeType::Dir => 0x4000,
            InodeType::BlockDev => 0x6000,
            InodeType::File => 0x8000,
            InodeType::Symlink => 0xA000,
            InodeType::UnixSock => 0xC000,
        }) | perm
    }
    pub fn from_direntry_ty_indicator(indicator: u8) -> Option<Self> {
        match indicator {
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
    pub fn to_direntry_ty_indicator(this: Self) -> u8 {
        match this {
            InodeType::File => 1,
            InodeType::Dir => 2,
            InodeType::CharDev => 3,
            InodeType::BlockDev => 4,
            InodeType::Fifo => 5,
            InodeType::UnixSock => 6,
            InodeType::Symlink => 7,
        }
    }
}

impl From<InodeType> for fal::FileType {
    fn from(inode_type: InodeType) -> Self {
        match inode_type {
            InodeType::File => Self::RegularFile,
            InodeType::Dir => Self::Directory,
            InodeType::Symlink => Self::Symlink,
            InodeType::BlockDev => Self::BlockDevice,
            InodeType::UnixSock => Self::Socket,
            InodeType::CharDev => Self::CharacterDevice,
            InodeType::Fifo => Self::NamedPipe,
        }
    }
}

impl Inode {
    const DIRECT_PTR_COUNT: usize = 12;

    pub fn load<R: fal::Device>(
        filesystem: &Filesystem<R>,
        inode_address: u32,
    ) -> fal::Result<Self> {
        if inode_address == 0 {
            return Err(fal::Error::Invalid);
        }

        if !block_group::inode_exists(inode_address, filesystem)? {
            return Err(fal::Error::NoEntity);
        }

        let block_group_index =
            block_group::inode_block_group_index(&filesystem.superblock, inode_address);
        let block_group_descriptor =
            block_group::load_block_group_descriptor(filesystem, block_group_index.into())?;
        let inode_index_in_group =
            block_group::inode_index_inside_group(&filesystem.superblock, inode_address);
        let inode_size = filesystem.superblock.inode_size();

        let containing_block_index = block_group_descriptor.inode_table_start_baddr()
            + u64::from(inode_index_in_group * u32::from(inode_size) / filesystem.superblock.block_size);

        let max_inodes_in_block = filesystem.superblock.block_size / u32::from(inode_size);

        let inode_index_in_block =
            usize::try_from(inode_index_in_group % max_inodes_in_block).unwrap();
        let inode_size = usize::from(inode_size);

        let containing_block = read_block(filesystem, containing_block_index)?;
        let inode_bytes = &containing_block
            [inode_index_in_block * inode_size..inode_index_in_block * inode_size + inode_size];
        Ok(Self::parse(
            &filesystem.superblock,
            inode_address,
            inode_bytes,
        ))
    }
    pub fn store<D: fal::DeviceMut>(
        this: &Self,
        filesystem: &mut Filesystem<D>,
    ) -> fal::Result<()> {
        let inode_address = this.addr;

        if inode_address == 0 {
            return Err(fal::Error::Invalid);
        }

        if !block_group::inode_exists(inode_address, filesystem)? {
            return Err(fal::Error::NoEntity);
        }

        let block_group_index =
            block_group::inode_block_group_index(&filesystem.superblock, inode_address);
        let block_group_descriptor =
            block_group::load_block_group_descriptor(filesystem, block_group_index.into())?;
        let inode_index_in_group =
            block_group::inode_index_inside_group(&filesystem.superblock, inode_address);
        let inode_size = filesystem.superblock.inode_size();

        let containing_block_index = block_group_descriptor.inode_table_start_baddr()
            + u64::from(inode_index_in_group * u32::from(inode_size) / filesystem.superblock.block_size);

        let max_inodes_in_block = filesystem.superblock.block_size / u32::from(inode_size);

        let inode_index_in_block =
            usize::try_from(inode_index_in_group % max_inodes_in_block).unwrap();
        let inode_size = usize::from(inode_size);

        let mut containing_block = read_block(filesystem, containing_block_index)?;
        let inode_bytes = &mut containing_block
            [inode_index_in_block * inode_size..inode_index_in_block * inode_size + inode_size];

        Self::serialize(this, inode_bytes);

        Ok(write_block(
            filesystem,
            containing_block_index,
            &containing_block,
        )?)
    }
    pub fn parse(superblock: &Superblock, addr: u32, bytes: &[u8]) -> Self {
        let raw = InodeRaw::parse(bytes);
        let size = raw.size(superblock);

        Self {
            block_size: superblock.block_size,
            addr,
            raw,
            size,
            os: superblock.os_id,
        }
    }
    pub fn serialize(this: &Inode, buffer: &mut [u8]) {
        InodeRaw::serialize(&this.raw, buffer)
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn size_in_blocks(&self) -> u64 {
        fal::div_round_up(self.size(), self.block_size.into())
    }
    fn entry_count(block_size: u32) -> usize {
        block_size as usize / mem::size_of::<u32>()
    }
    fn read_singly<D: fal::Device>(
        filesystem: &Filesystem<D>,
        singly_baddr: u32,
        rel_baddr: u32,
    ) -> io::Result<u32> {
        let singly_indirect_block_bytes = read_block(filesystem, singly_baddr.try_into().unwrap())?;
        let index = rel_baddr as usize - Self::DIRECT_PTR_COUNT;

        Ok(read_u32(
            &singly_indirect_block_bytes,
            index * mem::size_of::<u32>(),
        ))
    }
    fn read_doubly<D: fal::Device>(
        filesystem: &Filesystem<D>,
        doubly_baddr: u32,
        rel_baddr: u32,
    ) -> io::Result<u32> {
        let entry_count = Self::entry_count(filesystem.superblock.block_size);

        let doubly_indirect_block_bytes = read_block(filesystem, doubly_baddr.try_into().unwrap())?;
        let index = (rel_baddr as usize - Self::DIRECT_PTR_COUNT - entry_count) / entry_count;

        let singly = read_u32(&doubly_indirect_block_bytes, index * mem::size_of::<u32>());
        let singly_rel_baddr = rel_baddr - (index as u32 + 1) * entry_count as u32;

        Self::read_singly(filesystem, singly, singly_rel_baddr)
    }
    fn read_triply<D: fal::Device>(
        filesystem: &Filesystem<D>,
        triply_baddr: u32,
        rel_baddr: u32,
    ) -> io::Result<u32> {
        let entry_count = Self::entry_count(filesystem.superblock.block_size);

        let triply_indirect_block_bytes = read_block(filesystem, triply_baddr.try_into().unwrap())?;
        let index =
            (rel_baddr as usize - Self::DIRECT_PTR_COUNT - entry_count - entry_count * entry_count)
                / (entry_count * entry_count);

        let doubly = read_u32(&triply_indirect_block_bytes, index * mem::size_of::<u32>());
        let doubly_rel_baddr =
            rel_baddr - (index as u32 + 1) * entry_count as u32 * entry_count as u32;

        Self::read_doubly(filesystem, doubly, doubly_rel_baddr)
    }
    fn absolute_baddr<D: fal::Device>(
        &self,
        filesystem: &Filesystem<D>,
        rel_baddr: u32,
    ) -> io::Result<u64> {
        if self.flags().contains(InodeFlags::EXTENTS) {
            // TODO: Cache this tree.
            let tree = self.blocks.extent_tree();
            let leaf = tree.resolve(filesystem, rel_baddr).expect("Block not found");

            let offset_from_extent_start = rel_baddr - leaf.logical_block();
            return Ok(leaf.physical_start_block() + u64::from(offset_from_extent_start));
        }

        let entry_count = Self::entry_count(filesystem.superblock.block_size) as u32;

        let direct_size = Self::DIRECT_PTR_COUNT as u32;
        let singly_indir_size = direct_size + entry_count;
        let doubly_indir_size = singly_indir_size + entry_count * entry_count;
        let triply_indir_size = doubly_indir_size + entry_count * entry_count * entry_count;

        Ok(u64::from(if rel_baddr < direct_size {
            self.blocks.direct_ptrs()[usize::try_from(rel_baddr).unwrap()]
        } else if rel_baddr < singly_indir_size {
            Self::read_singly(filesystem, self.blocks.singly_indirect_ptr(), rel_baddr)?
        } else if rel_baddr < doubly_indir_size {
            Self::read_doubly(filesystem, self.blocks.doubly_indirect_ptr(), rel_baddr)?
        } else if rel_baddr < triply_indir_size {
            Self::read_triply(filesystem, self.blocks.triply_indirect_ptr(), rel_baddr)?
        } else {
            panic!("Read exceeding maximum ext2 file size.");
        }))
    }
    pub fn read_block_to<D: fal::Device>(
        &self,
        rel_baddr: u32,
        filesystem: &Filesystem<D>,
        buffer: &mut [u8],
    ) -> fal::Result<()> {
        if u64::from(rel_baddr) >= self.size_in_blocks() {
            return Err(fal::Error::Overflow);
        }
        let abs_baddr = self.absolute_baddr(filesystem, rel_baddr)?;
        Ok(read_block_to(filesystem, abs_baddr.try_into().unwrap(), buffer)?)
    }
    pub fn write_content_block<D: fal::DeviceMut>(
        &self,
        rel_baddr: u32,
        filesystem: &Filesystem<D>,
        buffer: &[u8],
    ) -> fal::Result<()> {
        if u64::from(rel_baddr) >= self.size_in_blocks() {
            return Err(fal::Error::Overflow);
        }
        let abs_baddr = self.absolute_baddr(filesystem, rel_baddr)?;
        Ok(write_block(filesystem, abs_baddr.try_into().unwrap(), buffer)?)
    }
    pub fn read<D: fal::Device>(
        &self,
        filesystem: &Filesystem<D>,
        offset: u64,
        mut buffer: &mut [u8],
    ) -> fal::Result<usize> {
        if self.flags().contains(InodeFlags::INLINE_DATA) {
            if self.flags().contains(InodeFlags::EXTENTS) {
                // TODO: Error handling
                panic!("Inode uses both INLINE and EXTENTS for data storage; this should not be possible, right?")
            }
            if self.size() > 60 {
                panic!("Inode too large to actually be INLINE ({} > 60)", self.size())
            }
            if offset >= self.size() {
                return Ok(0)
            }

            let readable_file_data = &self.blocks.inner[offset as usize..self.size() as usize];
            let bytes_to_read = std::cmp::min(buffer.len(), readable_file_data.len());
            buffer[..bytes_to_read].copy_from_slice(&readable_file_data[..bytes_to_read]);
            return Ok(bytes_to_read);
        }

        let mut bytes_read = 0;

        let off_from_rel_block = offset % u64::from(filesystem.superblock.block_size);
        let rel_baddr_start = offset / u64::from(filesystem.superblock.block_size);

        let mut block_bytes =
            (vec![0u8; usize::try_from(filesystem.superblock.block_size).unwrap()])
                .into_boxed_slice();

        if off_from_rel_block != 0 {
            self.read_block_to(
                rel_baddr_start.try_into().unwrap(),
                filesystem,
                &mut block_bytes,
            )?;

            let off_from_rel_block_usize = usize::try_from(off_from_rel_block).unwrap();
            let end = std::cmp::min(
                buffer.len(),
                usize::try_from(filesystem.superblock.block_size).unwrap()
                    - off_from_rel_block_usize,
            );
            buffer[..end].copy_from_slice(
                &block_bytes[off_from_rel_block_usize..off_from_rel_block_usize + end],
            );

            bytes_read += end;

            if u64::try_from(buffer.len()).unwrap() >= off_from_rel_block {
                return self
                    .read(
                        filesystem,
                        fal::round_up(offset, u64::from(filesystem.superblock.block_size)),
                        &mut buffer[end..],
                    )
                    .map(|b| b + bytes_read);
            } else {
                return Ok(bytes_read);
            }
        }

        let mut current_rel_baddr = u32::try_from(rel_baddr_start).unwrap();

        while buffer.len() >= usize::try_from(filesystem.superblock.block_size).unwrap() {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;

            buffer[..usize::try_from(filesystem.superblock.block_size).unwrap()]
                .copy_from_slice(&block_bytes);

            bytes_read += block_bytes.len();

            buffer = &mut buffer[usize::try_from(filesystem.superblock.block_size).unwrap()..];
            current_rel_baddr += 1;
        }

        if buffer.len() != 0 {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;
            let buffer_len = buffer.len();
            buffer.copy_from_slice(&block_bytes[..buffer_len]);
            bytes_read += buffer_len;
        }

        Ok(bytes_read)
    }
    pub fn write<D: fal::DeviceMut>(
        &self,
        filesystem: &mut Filesystem<D>,
        offset: u64,
        mut buffer: &[u8],
    ) -> fal::Result<()> {
        let off_from_rel_block = offset % u64::from(filesystem.superblock.block_size);
        let rel_baddr_start = offset / u64::from(filesystem.superblock.block_size);

        let mut block_bytes = vec![0u8; usize::try_from(filesystem.superblock.block_size).unwrap()]
            .into_boxed_slice();

        if off_from_rel_block != 0 {
            self.read_block_to(
                rel_baddr_start.try_into().unwrap(),
                filesystem,
                &mut block_bytes,
            )?;

            let off_from_rel_block_usize = usize::try_from(off_from_rel_block).unwrap();
            let end = std::cmp::min(
                buffer.len(),
                usize::try_from(filesystem.superblock.block_size).unwrap()
                    - off_from_rel_block_usize,
            );
            block_bytes[off_from_rel_block_usize..off_from_rel_block_usize + end]
                .copy_from_slice(&buffer[..end]);

            self.write_content_block(
                rel_baddr_start.try_into().unwrap(),
                filesystem,
                &block_bytes,
            )?;

            if u64::try_from(buffer.len()).unwrap() >= off_from_rel_block {
                return self.write(
                    filesystem,
                    fal::round_up(offset, u64::from(filesystem.superblock.block_size)),
                    &buffer[end..],
                );
            } else {
                return Ok(());
            }
        }

        let mut current_rel_baddr = u32::try_from(rel_baddr_start).unwrap();

        while buffer.len() >= usize::try_from(filesystem.superblock.block_size).unwrap() {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;

            block_bytes.copy_from_slice(
                &buffer[..usize::try_from(filesystem.superblock.block_size).unwrap()],
            );

            self.write_content_block(current_rel_baddr, filesystem, &block_bytes)?;

            buffer = &buffer[usize::try_from(filesystem.superblock.block_size).unwrap()..];
            current_rel_baddr += 1;
        }

        if buffer.len() != 0 {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;
            block_bytes[..buffer.len()].copy_from_slice(&buffer);
            self.write_content_block(current_rel_baddr, filesystem, &block_bytes)?;
        }

        Ok(())
    }
    fn raw_dir_entries<'a, D: fal::Device>(
        &'a self,
        filesystem: &'a Filesystem<D>,
    ) -> io::Result<RawDirIterator<'a, D>> {
        if self.ty() != InodeType::Dir {
            // TODO: ENOTDIR
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        Ok(RawDirIterator {
            filesystem,
            inode_struct: self,
            current_entry_offset: 0,
            entry_bytes: vec![0; 6],
            finished: false,
        })
    }
    pub fn dir_entries<'a, D: fal::Device>(
        &'a self,
        filesystem: &'a Filesystem<D>,
    ) -> io::Result<DirIterator<'a, D>> {
        Ok(DirIterator {
            raw: self.raw_dir_entries(filesystem)?,
        })
    }
    pub fn symlink_target<D: fal::Device>(
        &self,
        filesystem: &mut Filesystem<D>,
    ) -> fal::Result<Box<[u8]>> {
        if self.size <= 60 {
            // fast symlink
            Ok(self.blocks.inner[..].to_owned().into_boxed_slice())
        } else {
            // slow symlink

            // TODO: Waiting for try_reserve (https://github.com/rust-lang/rust/issues/48043).
            let mut bytes = vec![0u8; self.size.try_into().unwrap()];
            // TODO: Handle the amount of read bytes
            self.read(filesystem, 0, &mut bytes)?;
            Ok(bytes.into_boxed_slice())
        }
    }

    pub fn remove<D: fal::DeviceMut>(
        &self,
        filesystem: &mut Filesystem<D>,
        inode: u32,
    ) -> io::Result<()> {
        assert_eq!(self.base.hardlink_count, 0);

        // Frees the inode and its owned blocks.
        block_group::free_inode(inode, filesystem)
    }
    pub fn remove_entry<D: fal::DeviceMut>(
        &self,
        filesystem: &mut Filesystem<D>,
        name: &OsStr,
    ) -> fal::Result<()> {
        // Remove the entry by setting the length of the entry with matching name to zero, and
        // append the length of that entry to the previous (if any).

        let (index, (mut entry, offset)) = match self
            .raw_dir_entries(filesystem)?
            .enumerate()
            .find(|(_, (entry, _))| entry.name == name)
        {
            Some(x) => x,
            None => return Err(fal::Error::NoEntity),
        };

        if index > 0 {
            // TODO: Avoid iterating twice.

            let (mut previous_entry, previous_offset) =
                self.raw_dir_entries(filesystem)?.nth(index - 1).unwrap();
            previous_entry.total_entry_size += entry.total_entry_size;

            let mut bytes = [0u8; 8];
            DirEntry::serialize_raw(&previous_entry, &filesystem.superblock, &mut bytes);
            self.write(filesystem, previous_offset, &bytes)?;
        }

        let mut entry_inode_struct = Inode::load(filesystem, entry.inode)?;

        entry.total_entry_size = 0;
        entry.inode = 0;
        entry.type_indicator = Some(InodeType::File);
        entry.name = OsString::new();

        let mut bytes = [0u8; 8];
        DirEntry::serialize(&entry, &filesystem.superblock, &mut bytes);
        self.write(filesystem, offset, &bytes)?;

        entry_inode_struct.base.hardlink_count -= 1;

        if entry_inode_struct.base.hardlink_count == 0 {
            entry_inode_struct.remove(filesystem, entry.inode)?;
        } else {
            Inode::store(&entry_inode_struct, filesystem)?;
        }

        Ok(())
    }
}
impl std::ops::Deref for Inode {
    type Target = InodeRaw;

    fn deref(&self) -> &InodeRaw {
        &self.raw
    }
}
impl std::ops::Deref for InodeRaw {
    type Target = InodeRawBase;

    fn deref(&self) -> &InodeRawBase {
        &self.base
    }
}
impl std::ops::DerefMut for Inode {
    fn deref_mut(&mut self) -> &mut InodeRaw {
        &mut self.raw
    }
}
impl std::ops::DerefMut for InodeRaw {
    fn deref_mut(&mut self) -> &mut InodeRawBase {
        &mut self.base
    }
}
pub struct RawDirIterator<'a, D: fal::Device> {
    filesystem: &'a Filesystem<D>,
    inode_struct: &'a Inode,
    current_entry_offset: u64,
    entry_bytes: Vec<u8>,
    finished: bool,
}
pub struct DirIterator<'a, D: fal::Device> {
    raw: RawDirIterator<'a, D>,
}
impl<'a, D: fal::Device> Iterator for RawDirIterator<'a, D> {
    type Item = (DirEntry, u64);
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let size = self.inode_struct.size;

        if size == 0 {
            self.finished = true;
            return None;
        }

        if self.current_entry_offset + 6 < size {
            self.entry_bytes.clear();
            self.entry_bytes.resize(6, 0);

            if self
                .inode_struct
                .read(
                    self.filesystem,
                    self.current_entry_offset,
                    &mut self.entry_bytes[..6],
                )
                .is_err()
            {
                self.finished = true;
                return None;
            }
            let length = DirEntry::length(&self.entry_bytes[0..6])
                .try_into()
                .unwrap();

            if length == 0 {
                self.finished = true;
                return None;
            }

            self.entry_bytes.resize(length, 0);
            if self
                .inode_struct
                .read(
                    self.filesystem,
                    self.current_entry_offset,
                    &mut self.entry_bytes[..length],
                )
                .is_err()
            {
                self.finished = true;
                return None;
            }

            let entry = DirEntry::parse(&self.filesystem.superblock, &self.entry_bytes);

            let value = Some((entry, self.current_entry_offset));
            self.current_entry_offset += u64::try_from(length).unwrap();
            return value;
        } else {
            self.finished = true;
            return None;
        }
    }
}
impl<'a, D: fal::Device> Iterator for DirIterator<'a, D> {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next().map(|(entry, _)| entry)
    }
}
#[derive(Debug)]
pub struct DirEntry {
    pub inode: u32,
    pub type_indicator: Option<InodeType>,
    pub name: OsString,
    pub total_entry_size: u16,
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
                if extended
                    .opt_features_present
                    .contains(OptionalFeatureFlags::INODE_EXTENDED_ATTRS)
                {
                    InodeType::from_direntry_ty_indicator(read_u8(bytes, 7))
                } else {
                    None
                }
            } else {
                None
            },
            name,
            total_entry_size,
        }
    }
    pub fn serialize_raw(this: &Self, superblock: &Superblock, bytes: &mut [u8]) {
        write_u32(bytes, 0, this.inode);
        write_u16(bytes, 4, this.total_entry_size);
        write_u8(bytes, 6, this.name.len() as u8);

        if superblock
            .extended
            .as_ref()
            .map(|extended| {
                extended
                    .req_features_present
                    .contains(RequiredFeatureFlags::DIR_TYPE)
            })
            .unwrap_or(false)
        {
            write_u8(
                bytes,
                7,
                InodeType::to_direntry_ty_indicator(this.type_indicator.unwrap()),
            );
        } else {
            write_u8(bytes, 7, (this.name.len() >> 8) as u8);
        }
    }
    pub fn serialize(this: &Self, superblock: &Superblock, bytes: &mut [u8]) {
        Self::serialize_raw(this, superblock, bytes);
        bytes[8..8 + this.name.len()].copy_from_slice(&os_str_to_bytes(&this.name));
    }
    pub fn ty<D: fal::Device>(&self, filesystem: &Filesystem<D>) -> fal::Result<InodeType> {
        Ok(match self.type_indicator {
            Some(ty) => ty,
            None => Inode::load(filesystem, self.inode)?.ty(),
        })
    }
}
