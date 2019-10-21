use crate::{
    items::ChunkItem,
    superblock::Superblock,
    tree::Tree,
};

use std::collections::BTreeMap;

/// Maps logical addresses to physical (with some metadata) using the chunk tree. Addresses are
/// virtualized because of RAID.
#[derive(Debug)]
pub struct ChunkMap {
    map: BTreeMap<u64, ChunkItem>,
}

impl ChunkMap {
    pub fn read_sys_chunk_array(superblock: &Superblock) -> Self {
        Self {
            map: superblock.system_chunk_array.0.iter().cloned().map(|(disk_key, chunk_item)| (disk_key.offset, chunk_item)).collect(),
        }
    }
    pub fn get_full(&self, virt: u64) -> Option<(u64, &ChunkItem)> {
        // TODO: Create a new/modify the existing BTreeMap to support getting the highest item
        // below the searched item (used to retrieve which range an address belongs to).

        // Use the suboptimal O(n) approach.
        self.map.iter().filter(|(key, _)| *key <= &virt).max_by_key(|(k, _)| *k).map(|(&k, v)| (k, v))
    }
    pub fn get(&self, superblock: &Superblock, virt: u64) -> Option<u64> {
        // TODO: RAID (multiple stripes)
        let (key, chunk_item) = match self.get_full(virt) {
            Some(p) => p,
            None => return None,
        };

        let offset = virt - key;
        if offset >= chunk_item.len { return None }
        Some(chunk_item.stripe(superblock).offset + offset)
    }
    pub fn read_chunk_tree<D: fal::Device>(&mut self, device: &mut D, superblock: &Superblock, tree: &Tree) {
        let new_pairs = tree.pairs(device, superblock, self).filter_map(|(k, v)| v.into_chunk_item().map(|v| (k.offset, v))).collect::<Vec<_>>();
        self.map.extend(new_pairs)
    }
}
