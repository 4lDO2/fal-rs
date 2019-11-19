//! Journaling structures and procedures.
//! __NOTE: Every field here is stored as _big endian___

use std::{fmt, ops::Range};

use bitflags::bitflags;
use quick_error::quick_error;
use scroll::{Pread, Pwrite};
use uuid::Uuid;

use crate::{Filesystem, Inode, superblock::Superblock};

pub const JOURNAL_HEADER_MAGIC: u32 = 0xC03B3998;

#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct JournalHeader {
    pub magic: u32,
    pub block_ty: u32,
    pub xid: u32,
}

impl JournalHeader {
    pub fn is_valid(&self) -> bool {
        return self.magic == JOURNAL_HEADER_MAGIC && self.block_ty >= 1 && self.block_ty <= 5
    }
}

#[repr(u32)]
pub enum BlockType {
    Descriptor = 1,
    BlockCommitRecord,
    SuperblockV1,
    SuperblockV2,
    BlockRevocationRecords,
}


#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct JournalSuperblockV1 {
    pub header: JournalHeader,
    pub block_size: u32,
    pub max_len: u32,
    pub first_log_block: u32,
    pub first_commit_id: u32,
    pub log_start_baddr: u32,
    pub errno: u32,
}

#[derive(Clone, Copy, Pread, Pwrite)]
pub struct JournalSuperblockV2 {
    pub compat_features: u32,
    pub incompat_features: u32,
    pub ro_compat_features: u32,
    pub uuid: [u8; 16],
    pub user_count: u32,
    pub dyn_super_block: u32,
    pub checksum_ty: u8,
    pub padding2: [u8; 3],
    pub padding: [u32; 42],
    pub checksum: u32,
    pub users: [u8; 768],
}

impl fmt::Debug for JournalSuperblockV2 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("JournalSuperblockV2")
            .field("compat_features", &JournalCompatFeatureFlags::from_bits_truncate(self.compat_features))
            .field("incompat_features", &JournalIncompatFeatureFlags::from_bits_truncate(self.compat_features))
            .field("ro_compat_features", &"(empty)")
            .field("uuid", &Uuid::from_bytes(self.uuid))
            .field("user_count", &self.user_count)
            .field("dyn_super_block", &self.dyn_super_block)
            .field("checksum_ty", &JournalChecksumType::from_raw(self.checksum_ty))
            .field("checksum", &self.checksum)
            .field("users", &&self.users[..])
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct JournalSuperblock {
    pub base: JournalSuperblockV1,
    pub ext: Option<JournalSuperblockV2>,
}

impl std::ops::Deref for JournalSuperblock {
    type Target = JournalSuperblockV1;

    fn deref(&self) -> &JournalSuperblockV1 {
        &self.base
    }
}
impl std::ops::DerefMut for JournalSuperblock {
    fn deref_mut(&mut self) -> &mut JournalSuperblockV1 {
        &mut self.base
    }
}

impl JournalSuperblock {
    pub fn parse(bytes: &[u8]) -> Result<Self, scroll::Error> {
        let mut offset = 0;
        let v1: JournalSuperblockV1 = bytes.gread_with(&mut offset, scroll::BE)?;

        if v1.header.magic != JOURNAL_HEADER_MAGIC {
            return Err(scroll::Error::BadInput { size: 4, msg: "the journal superblock's magic number didn't match" });
        }

        let v2 = if v1.header.block_ty == BlockType::SuperblockV2 as u32 {
            Some(bytes.gread_with(&mut offset, scroll::BE)?)
        } else if v1.header.block_ty == BlockType::SuperblockV1 as u32 {
            None
        } else {
            return Err(scroll::Error::BadInput { size: 4, msg: "invalid journal superblock block type, expected superblock v1 or v2" });
        };

        Ok(Self {
            base: v1,
            ext: v2,
        })
    }
    pub fn incompat_features(&self) -> JournalIncompatFeatureFlags {
        self.ext.as_ref().map(|ext| JournalIncompatFeatureFlags::from_bits(ext.incompat_features).unwrap_or_else(JournalIncompatFeatureFlags::empty)).unwrap_or(JournalIncompatFeatureFlags::empty())
    }
    pub fn uuid(&self, ext4_superblock: &Superblock) -> [u8; 16] {
        if let Some(ref ext) = self.ext {
            ext.uuid
        } else {
            ext4_superblock.extended.as_ref().unwrap().journal_id
        }
    }
}

bitflags! {
    pub struct JournalCompatFeatureFlags: u32 {
        const DATA_CHECKSUMS = 0x1;
    }
}
bitflags! {
    pub struct JournalIncompatFeatureFlags: u32 {
        const REVOCATION_RECS = 0x1;
        const _64_BIT = 0x2;
        const ASYNC_COMMITS = 0x4;
        const V2_CHECKSUM = 0x8;
        const V3_CHECKSUM = 0x10;
    }
}

#[derive(Debug)]
pub struct JournalRoCompatFeatureFlags;

#[repr(u8)]
#[derive(Debug)]
pub enum JournalChecksumType {
    Crc32 = 1,
    Md5 = 2,
    Sha1 = 3,
    Crc32c = 4,
}

impl JournalChecksumType {
    fn from_raw(raw: u8) -> Option<Self> {
        Some(match raw {
            1 => Self::Crc32,
            2 => Self::Md5,
            3 => Self::Sha1,
            4 => Self::Crc32c,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy)]
pub struct JournalBlockTag {
    pub data_block_lo: u32,
    pub flags: u32,
    pub data_block_hi: u32,
    pub checksum: u32,
    pub uuid: [u8; 16],
}

bitflags! {
    pub struct JournalTagFlags: u32 {
        const ESCAPED = 0x1;
        const SAME_UUID = 0x2;
        const DELETED = 0x4;
        const LAST_TAG = 0x8;
    }
}

impl JournalBlockTag {
    pub fn parse(bytes: &[u8], offset: &mut usize, incompat_flags: JournalIncompatFeatureFlags, journal_uuid: [u8; 16]) -> Result<Self, scroll::Error> {
        let data_block_lo = bytes.gread_with(offset, scroll::BE)?;
        let flags = bytes.gread_with(offset, scroll::BE)?;
        let data_block_hi = bytes.gread_with(offset, scroll::BE)?;
        let checksum = bytes.gread_with(offset, scroll::BE)?;

        let wrapped_flags = match JournalTagFlags::from_bits(flags) {
            Some(f) => f,
            None => return Err(scroll::Error::BadInput { size: 4, msg: "the journal block tag flags contained unknown flags" }),
        };

        if !incompat_flags.contains(JournalIncompatFeatureFlags::_64_BIT) && data_block_hi != 0 {
            return Err(scroll::Error::BadInput { size: 4, msg: "Nonzero high data block address without the 64 bit journal feature" });
        }

        let mut uuid = journal_uuid;

        if !wrapped_flags.contains(JournalTagFlags::SAME_UUID) {
            let new_offset = *offset + 16;
            if new_offset >= bytes.len() {
                // TODO: The documentation is cryptic (in this particular case), what do size and
                // len mean?
                return Err(scroll::Error::TooBig { size: 32, len: bytes.len() - *offset });
            }
            uuid.copy_from_slice(&bytes[*offset..new_offset]);
            *offset = new_offset;
        }

        Ok(Self {
            checksum,
            data_block_hi,
            data_block_lo,
            flags,
            uuid,
        })
    }
    pub fn tag_size(incompat_flags: JournalIncompatFeatureFlags) -> usize {
        if incompat_flags.contains(JournalIncompatFeatureFlags::V3_CHECKSUM) {
            return 16;
        }
        12 + if incompat_flags.contains(JournalIncompatFeatureFlags::V2_CHECKSUM) { 2 } else { 0 } - if incompat_flags.contains(JournalIncompatFeatureFlags::_64_BIT) { 4 } else { 0 }
    }
    pub fn data_block(&self) -> u64 {
        u64::from(self.data_block_lo) | (u64::from(self.data_block_hi) << 32)
    }
}

impl fmt::Debug for JournalBlockTag {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("JournalBlockTag")
            .field("data_block", &self.data_block())
            .field("flags", &JournalTagFlags::from_bits_truncate(self.flags))
            .field("checksum", &self.checksum)
            .field("uuid", &Uuid::from_bytes(self.uuid))
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct JournalDescriptorBlockTail {
    pub checksum: u32,
}

#[derive(Debug)]
pub struct JournalDescriptorBlock {
    pub header: JournalHeader,
    pub tags: Vec<JournalBlockTag>,
    pub tail: JournalDescriptorBlockTail,
}

impl JournalDescriptorBlock {
    pub fn parse(bytes: &[u8], incompat_flags: JournalIncompatFeatureFlags, journal_uuid: [u8; 16]) -> Result<Self, scroll::Error> {
        let header: JournalHeader = bytes.pread_with(0, scroll::BE)?;

        if header.block_ty != BlockType::Descriptor as u32 {
            return Err(scroll::Error::BadInput { size: 4, msg: "journal descriptor block wasn't a descriptor" });
        }
        if header.magic != JOURNAL_HEADER_MAGIC {
            return Err(scroll::Error::BadInput { size: 4, msg: "the journal descriptor's magic number didn't match" });
        }

        let tag_size = JournalBlockTag::tag_size(incompat_flags);

        let mut offset = 12;

        let tags = (0..1012 / tag_size).map(|_| JournalBlockTag::parse(&bytes, &mut offset, incompat_flags, journal_uuid)).take_while(|t| t.is_ok()).collect::<Result<Vec<_>, _>>()?;
        let tail = bytes.pread_with(1020, scroll::BE)?;

        Ok(Self {
            header,
            tags,
            tail,
        })
    }
}

#[derive(Clone, Copy, Debug, Pread, Pwrite)]
pub struct JournalCommitBlock {
    pub header: JournalHeader,
    pub checksum_ty: u8,
    pub checksum_size: u8,
    pub padding: [u8; 2],
    pub checksum: [u32; 8],
    pub commit_sec: u64,
    pub commit_nsec: u32,
}

impl JournalCommitBlock {
    pub fn parse(bytes: &[u8]) -> Result<Self, scroll::Error> {
        let this: Self = bytes.pread_with(0, scroll::BE)?;

        if this.header.block_ty != BlockType::BlockCommitRecord as u32 {
            return Err(scroll::Error::BadInput { size: 4, msg: "the journal commit block header didn't have the type 'block commit header'" })
        }
        if this.header.magic != JOURNAL_HEADER_MAGIC {
            return Err(scroll::Error::BadInput { size: 4, msg: "the journal commit block's magic number didn't match" });
        }

        Ok(this)
    }
}

#[derive(Debug)]
pub struct Journal {
    pub inode: Inode,
    pub superblock: JournalSuperblock,
}

quick_error! {
    #[derive(Debug)]
    pub enum JournalInitError {
        Io(err: fal::Error) {
            cause(err)
            from()
            description("an i/o error occured when reading from or writing to the journal")
            display("i/o error: {}", err)
        }
        UnsupportedJournalFormat {
            description("journal doesn't support v3 checksums")
        }
        ParseError(err: scroll::Error) {
            cause(err)
            from()
            description("an error occured when parsing the journal structs")
            display("parse error: {}", err)
        }
    }
}

#[derive(Debug)]
pub struct Transaction {
    pub desc_blocks: Vec<(JournalDescriptorBlock, Range<u32>)>,
    pub commit_block: JournalCommitBlock,
}

/// Returns the address of the found block, and the header of that block. The block bytes are also
/// updated with the content of the last read block.
fn find_meta_block<D: fal::Device>(filesystem: &Filesystem<D>, journal_inode: &Inode, start_baddr: u32, block_bytes: &mut [u8], superblock: &JournalSuperblock) -> Result<Option<(u32, JournalHeader)>, JournalInitError> {
    for baddr in start_baddr..superblock.max_len {
        journal_inode.read_block_to(baddr, filesystem, block_bytes)?;
        let header: JournalHeader = match block_bytes.pread_with::<JournalHeader>(0, scroll::BE) {
            Ok(h) => h,
            Err(_) => continue,
        };
        if !header.is_valid() {
            continue;
        }
        return Ok(Some((baddr, header)));
    }
    Ok(None)
}
/// Returns the address of the block where the next transaction starts, and the transaction.
pub fn find_transaction<D: fal::Device>(filesystem: &Filesystem<D>, journal_inode: &Inode, start_baddr: u32, block_bytes: &mut [u8], journal_superblock: &JournalSuperblock) -> Result<Option<(u32, Transaction)>, JournalInitError> {
    let mut desc_blocks = vec! [];
    let mut start_block = start_baddr;
    let mut previous_desc_block = None;

    while let Some((meta_baddr, header)) = find_meta_block(filesystem, journal_inode, start_block, block_bytes, journal_superblock)? {
        if header.block_ty == BlockType::Descriptor as u32 || header.block_ty == BlockType::BlockCommitRecord as u32 {
            if let Some(desc_block) = previous_desc_block.take() {
                desc_blocks.push((desc_block, start_block..meta_baddr - 1));
            }
            start_block = meta_baddr + 1;
        }

        if header.block_ty == BlockType::Descriptor as u32 {
            // The block bytes won't be changed since the last read in find_meta_block.
            let new_desc_block = JournalDescriptorBlock::parse(block_bytes, journal_superblock.incompat_features(), journal_superblock.uuid(&filesystem.superblock))?;
            previous_desc_block = Some(new_desc_block);
        } else if header.block_ty == BlockType::BlockCommitRecord as u32 {
            let commit_block = JournalCommitBlock::parse(block_bytes)?;
            return Ok(Some((start_block, Transaction {
                commit_block,
                desc_blocks,
            })))
        }
    }
    Ok(None)
}

impl Journal {
    pub fn load<D: fal::Device>(filesystem: &Filesystem<D>) -> Result<Option<Self>, JournalInitError> {
        let journal_inode = match filesystem.superblock.extended.as_ref() {
            Some(i) => i,
            None => return Ok(None),
        }.journal_inode;

        // TODO: Support journal devices as well. This will require the frontend to be able to
        // provide the backend with additional devices when required. This will also happen in
        // multi-dev configurations, where one filesystem provides references to other disks.
        if journal_inode == 0 { return Ok(None) }

        let mut inode = Inode::load(filesystem, journal_inode)?;

        let mut superblock_bytes = vec! [0u8; 1024];
        // TODO: Use the journal block size.
        inode.read(filesystem, 0, &mut superblock_bytes)?;

        let superblock = JournalSuperblock::parse(&superblock_bytes)?;
        inode.block_size = superblock.block_size;

        if !superblock.incompat_features().contains(JournalIncompatFeatureFlags::V3_CHECKSUM) {
            return Err(JournalInitError::UnsupportedJournalFormat)
        }

        let mut block_bytes = vec! [0u8; superblock.block_size as usize];

        let mut current_baddr = superblock.first_log_block;
        while let Some((next_transacton_baddr, transaction)) = find_transaction(filesystem, &inode, current_baddr, &mut block_bytes, &superblock)? {
            dbg!(transaction);
            current_baddr = next_transacton_baddr;
        }

        Ok(Some(Self {
            inode,
            superblock,
        }))
    }
}
