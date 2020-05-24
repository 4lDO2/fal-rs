use std::sync::Mutex;

use crate::{chunk_map::ChunkMap, oid, superblock::Superblock, tree::{Tree, TreeOwned}, DiskKey, DiskKeyType, u64_le, u32_le, u16_le};

pub const FIRST_CHUNK_TREE_OBJECTID: u64 = 256;

pub fn read_node_phys<D: fal::DeviceRo>(
    device: &mut D,
    superblock: &Superblock,
    offset: u64,
) -> Box<[u8]> {
    let mut bytes = vec![0u8; superblock.node_size.get() as usize];
    // FIXME
    debug_assert_eq!(u64::from(superblock.node_size.get()) % u64::from(device.disk_info().unwrap().block_size), 0);
    device.read_blocks(offset, &mut bytes).unwrap();
    bytes.into_boxed_slice()
}

pub fn read_node<D: fal::DeviceRo>(
    device: &mut D,
    superblock: &Superblock,
    chunk_map: &ChunkMap,
    offset: u64,
) -> Box<[u8]> {
    read_node_phys(
        device,
        superblock,
        chunk_map.get(superblock, offset).unwrap(),
    )
}

#[derive(Debug)]
pub struct Filesystem<D: fal::DeviceRo> {
    pub device: Mutex<D>,
    pub superblock: Superblock,

    pub chunk_map: ChunkMap,

    pub root_tree: TreeOwned,
    pub chunk_tree: TreeOwned,
    pub extent_tree: TreeOwned,
    pub dev_tree: TreeOwned,
    pub fs_tree: TreeOwned,
    pub csum_tree: TreeOwned,
    pub quota_tree: Option<TreeOwned>,
    pub uuid_tree: TreeOwned,
    pub free_space_tree: Option<TreeOwned>,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let superblock = Superblock::load(&mut device);

        let mut chunk_map = ChunkMap::read_sys_chunk_array(&superblock);

        let chunk_tree = Tree::load(&mut device, &superblock, &chunk_map, superblock.chunk_root.get()).expect("failed to load chunk tree");
        chunk_map.read_chunk_tree(&mut device, &superblock, chunk_tree.as_ref());

        let root_tree = Tree::load(&mut device, &superblock, &chunk_map, superblock.root.get()).expect("failed to load root tree");

        let extent_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::EXTENT_TREE,
        )
        .unwrap();
        let dev_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::DEV_TREE,
        )
        .unwrap();
        let fs_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::FS_TREE,
        )
        .unwrap();
        let csum_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::CSUM_TREE,
        )
        .unwrap();

        // It seems like the quota tree may not necessarily exist. Or, the quota tree is simply
        // stored somewhere else.
        let quota_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::QUOTA_TREE,
        );

        let uuid_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::UUID_TREE,
        )
        .unwrap();

        // The existence of the free space tree also seems optional. It's possibly located in the
        // fs tree, right?
        let free_space_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::FREE_SPACE_TREE,
        );

        Self {
            device: Mutex::new(device),
            superblock,

            chunk_map,

            root_tree,
            chunk_tree,
            extent_tree,
            dev_tree,
            fs_tree,
            csum_tree,
            quota_tree,
            uuid_tree,
            free_space_tree,
        }
    }
    fn load_tree(
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        tree: Tree,
        oid: u64,
    ) -> Option<TreeOwned> {
        let value = match tree.get(
            device,
            superblock,
            chunk_map,
            &DiskKey {
                oid: u64_le::new(oid),
                ty: DiskKeyType::RootItem as u8,
                offset: u64_le::new(0),
            },
        ) {
            Some(v) => v,
            None => return None,
        };
        let root_item = value.as_root_item().unwrap();

        Some(Tree::load(device, superblock, chunk_map, root_item.addr.get()).unwrap())
    }
}
