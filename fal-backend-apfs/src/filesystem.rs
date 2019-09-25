use std::{
    collections::HashMap,
    io::{prelude::*, SeekFrom},
    sync::Mutex,
};

use crate::{
    btree::BTreeNode,
    checkpoint::{
        self, CheckpointDescAreaEntry, CheckpointMapping, CheckpointMappingPhys, GenericObject,
    },
    omap::OmapPhys,
    superblock::{NxSuperblock, ObjectIdentifier},
};

pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub container_superblock: NxSuperblock,
    pub ephemeral_objects: HashMap<ObjectIdentifier, GenericObject>,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let container_superblock = NxSuperblock::load(&mut device);

        if container_superblock.chkpnt_desc_blkcnt & (1 << 31) != 0 {
            unimplemented!()
        }
        // Otherwise, checkpoint descriptor area is contiguos.

        let block_size = container_superblock.block_size as usize;

        let mut descriptor_area =
            vec![0u8; block_size * container_superblock.chkpnt_desc_blkcnt() as usize];

        for block_index in 0..container_superblock.chkpnt_desc_blkcnt() {
            let range = block_index as usize * block_size..(block_index as usize + 1) * block_size;
            Self::read_block_to(
                &container_superblock,
                &mut device,
                &mut descriptor_area[range.clone()],
                container_superblock.chkpnt_desc_base + i64::from(block_index),
            );

            // Only when running fsck...
            //
            // let obj = crate::superblock::ObjPhys::parse(&descriptor_area[range]);
            // assert!(obj.object_type.ty == crate::superblock::ObjectType::CheckpointMap || obj.object_type.ty == crate::superblock::ObjectType::NxSuperblock);
            //
        }

        let superblock = (0..container_superblock.chkpnt_desc_len)
            .map(|i| {
                checkpoint::read_from_desc_area(
                    &mut device,
                    &container_superblock,
                    container_superblock.chkpnt_desc_first + i,
                )
            })
            .filter_map(|entry| entry.into_superblock())
            .filter(|superblock| superblock.is_valid())
            .max_by_key(|superblock| superblock.header.transaction_id)
            .unwrap();

        let ephemeral_object_ids = (0..container_superblock.chkpnt_desc_len)
            .map(|i| {
                checkpoint::read_from_desc_area(
                    &mut device,
                    &container_superblock,
                    container_superblock.chkpnt_desc_first + i,
                )
            })
            .filter_map(|entry| entry.into_mapping())
            .map(|mapping: CheckpointMappingPhys| {
                Vec::from(mapping.mappings)
                    .into_iter()
                    .map(|mapping: CheckpointMapping| (mapping.oid, mapping.paddr))
            })
            .flatten()
            .collect::<Vec<_>>();

        let ephemeral_objects = ephemeral_object_ids
            .into_iter()
            .map(|(id, paddr)| {
                (
                    id,
                    checkpoint::read_from_data_area(
                        &mut device,
                        &container_superblock,
                        (paddr.0 - superblock.chkpnt_data_base as u64) as u32,
                    )
                    .unwrap(),
                )
            })
            .collect::<HashMap<ObjectIdentifier, GenericObject>>();

        let container_superblock: NxSuperblock = superblock;

        let omap = OmapPhys::parse(&Self::read_block(&container_superblock, &mut device, container_superblock.object_map_oid.0 as i64));

        let omap_tree = BTreeNode::parse(&Self::read_block(&container_superblock, &mut device, omap.tree_oid.0 as i64));
        dbg!(omap_tree);

        Self {
            container_superblock,
            device: Mutex::new(device),
            ephemeral_objects,
        }
    }
    pub fn read_block_to(
        superblock: &NxSuperblock,
        device: &mut D,
        block: &mut [u8],
        address: crate::superblock::BlockAddr,
    ) {
        debug_assert!(address >= 0);
        debug_assert_eq!(block.len(), superblock.block_size as usize);

        device
            .seek(SeekFrom::Start(
                address as u64 * u64::from(superblock.block_size),
            ))
            .unwrap();
        device.read_exact(block).unwrap();
    }
    pub fn read_block(
        superblock: &NxSuperblock,
        device: &mut D,
        address: crate::superblock::BlockAddr,
    ) -> Box<[u8]> {
        let mut block_bytes = vec![0u8; superblock.block_size as usize].into_boxed_slice();
        Self::read_block_to(superblock, device, &mut block_bytes, address);
        block_bytes
    }
}
impl<D: fal::DeviceMut> Filesystem<D> {
    pub fn write_block(
        superblock: &NxSuperblock,
        device: &mut D,
        address: crate::superblock::BlockAddr,
        block: &[u8],
    ) {
        debug_assert_eq!(block.len(), superblock.block_size as usize);
        device
            .seek(SeekFrom::Start(
                address as u64 * u64::from(superblock.block_size),
            ))
            .unwrap();
        device.write_all(&block).unwrap();
    }
}
