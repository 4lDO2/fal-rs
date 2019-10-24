use std::{
    collections::HashMap,
    io::SeekFrom,
    sync::Mutex,
};

use crate::{
    btree::BTree,
    checkpoint::{
        self, CheckpointMapping, CheckpointMappingPhys, GenericObject,
    },
    omap::{Omap, OmapKey, OmapValue},
    read_block, read_block_to,
    superblock::{ApfsSuperblock, NxSuperblock},
    BlockAddr, ObjectIdentifier,
};

pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub container_superblock: NxSuperblock,
    pub ephemeral_objects: HashMap<ObjectIdentifier, GenericObject>,
    pub mounted_volumes: Vec<Volume>,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let container_superblock = NxSuperblock::load(&mut device);

        if container_superblock.chkpnt_desc_blkcnt & (1 << 31) != 0 {
            unimplemented!("B-tree checkpoints aren't implemented as of now")
        }
        // Otherwise, checkpoint descriptor area is contiguous.

        let block_size = container_superblock.block_size as usize;

        let mut descriptor_area =
            vec![0u8; block_size * container_superblock.chkpnt_desc_blkcnt() as usize];

        for block_index in 0..container_superblock.chkpnt_desc_blkcnt() {
            let range = block_index as usize * block_size..(block_index as usize + 1) * block_size;
            read_block_to(
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

        let omap = Omap::load(
            &mut device,
            &container_superblock,
            container_superblock.object_map_oid.0 as i64,
        );

        let mounted_volumes = container_superblock
            .volumes_oids
            .iter()
            .copied()
            .take_while(|oid| oid.is_valid())
            .map(|volume| {
                let (_, omap_value) = omap
                    .get_partial_latest(
                        &mut device,
                        &container_superblock,
                        OmapKey::partial(volume),
                    )
                    .expect("Volume virtual oid_t wasn't found in the omap B+ tree.");

                Volume::load(&mut device, &container_superblock, omap_value.paddr)
            })
            .collect::<Vec<_>>();

        Self {
            container_superblock,
            device: Mutex::new(device),
            ephemeral_objects,
            mounted_volumes,
        }
    }
}
impl<D: fal::DeviceMut> Filesystem<D> {
    pub fn write_block(
        superblock: &NxSuperblock,
        device: &mut D,
        address: BlockAddr,
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

#[derive(Debug)]
pub struct Volume {
    pub superblock: ApfsSuperblock,
    pub omap: Omap,
    pub root_tree: BTree,
}

impl Volume {
    pub fn load<D: fal::Device>(device: &mut D, nx_super: &NxSuperblock, phys: BlockAddr) -> Self {
        let superblock = ApfsSuperblock::parse(&read_block(nx_super, device, phys));

        let omap = Omap::load(device, nx_super, superblock.omap_oid.0 as i64);

        let root_oid_phys = Self::root_oid_phys(device, nx_super, &omap, &superblock).paddr;
        let root_tree = BTree::load(device, nx_super, root_oid_phys);

        dbg!(root_tree.pairs(device, nx_super, Some(&omap)).collect::<Vec<_>>());

        Self {
            omap,
            superblock,
            root_tree,
        }
    }
    fn root_oid_phys<D: fal::Device>(
        device: &mut D,
        superblock: &NxSuperblock,
        omap: &Omap,
        vol_superblock: &ApfsSuperblock,
    ) -> OmapValue {
        omap.get_partial_latest(
            device,
            superblock,
            OmapKey::partial(vol_superblock.root_tree_oid),
        )
        .unwrap()
        .1
    }
}
