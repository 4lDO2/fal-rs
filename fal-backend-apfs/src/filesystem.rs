use std::{io::{prelude::*, SeekFrom}, sync::Mutex};

use crate::{
    checkpoint::{self, CheckpointDescAreaEntry},
    superblock::NxSuperblock,
};

pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub container_superblock: NxSuperblock,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let container_superblock = NxSuperblock::load(&mut device);

        if container_superblock.chkpnt_desc_blkcnt & (1 << 31) != 0 {
            unimplemented!()
        }
        // Otherwise, checkpoint descriptor area is contiguos.

        let block_size = container_superblock.block_size as usize;

        let mut descriptor_area = vec! [0u8; block_size * container_superblock.chkpnt_desc_blkcnt() as usize];

        for block_index in 0..container_superblock.chkpnt_desc_blkcnt() {
            let range = block_index as usize * block_size .. (block_index as usize + 1) * block_size;
            Self::read_block_to(&container_superblock, &mut device, &mut descriptor_area[range.clone()], container_superblock.chkpnt_desc_base + i64::from(block_index));

            // Only when running fsck...
            //
            // let obj = crate::superblock::ObjPhys::parse(&descriptor_area[range]);
            // assert!(obj.object_type.ty == crate::superblock::ObjectType::CheckpointMap || obj.object_type.ty == crate::superblock::ObjectType::NxSuperblock);
            //
        }

        let superblock = (0..container_superblock.chkpnt_desc_len).map(|i| {
            checkpoint::read_from_desc_area(&mut device, &container_superblock, container_superblock.chkpnt_desc_first + i)
        }).filter_map(|entry| entry.into_superblock()).filter(|superblock| superblock.is_valid()).max_by_key(|superblock| superblock.header.transaction_id).unwrap();

        println!("Checkpoint superblock: {:?}", superblock);

        Self {
            container_superblock,
            device: Mutex::new(device),
        }
    }
    pub fn read_block_to(superblock: &NxSuperblock, device: &mut D, block: &mut [u8], address: crate::superblock::BlockAddr) {
        debug_assert!(address >= 0);
        debug_assert_eq!(block.len(), superblock.block_size as usize);

        device.seek(SeekFrom::Start(address as u64 * u64::from(superblock.block_size))).unwrap();
        device.read_exact(block).unwrap();
    }
    pub fn read_block(superblock: &NxSuperblock, device: &mut D, address: crate::superblock::BlockAddr) -> Box<[u8]> {
        let mut block_bytes = vec! [0u8; superblock.block_size as usize].into_boxed_slice();
        Self::read_block_to(superblock, device, &mut block_bytes, address);
        block_bytes
    }
}
impl<D: fal::DeviceMut> Filesystem<D> {
    pub fn write_block(superblock: &NxSuperblock, device: &mut D, address: crate::superblock::BlockAddr, block: &[u8]) {
        debug_assert_eq!(block.len(), superblock.block_size as usize);
        device.seek(SeekFrom::Start(address as u64 * u64::from(superblock.block_size))).unwrap();
        device.write_all(&block).unwrap();
    }
}
