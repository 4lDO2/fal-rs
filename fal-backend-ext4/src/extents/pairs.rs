use std::borrow::Cow;

use crate::{
    allocate_block_bytes,
    disk::BlockKind,
    extents::{ExtentLeaf, ExtentTree, ExtentTreeBody},
    inode::InodeIoError,
    Filesystem,
};

type Path<'a> = Vec<(Cow<'a, ExtentTree>, usize)>;

/// An iterator used primarily for debugging, that emits all of the leaves in a tree.
pub struct Pairs<'a, 'b, D: fal::Device> {
    current_path: Path<'a>,
    block_bytes: Box<[u8]>,
    filesystem: &'b Filesystem<D>,
}

impl ExtentTree {
    pub fn pairs<'a, 'b, D: fal::Device>(
        &'a self,
        filesystem: &'b Filesystem<D>,
    ) -> Pairs<'a, 'b, D> {
        Pairs {
            current_path: vec![(Cow::Borrowed(self), 0)],
            block_bytes: allocate_block_bytes(&filesystem.superblock),
            filesystem,
        }
    }
}

impl<'a, 'b, D: fal::Device> Iterator for Pairs<'a, 'b, D> {
    type Item = Result<ExtentLeaf, InodeIoError<D>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_path.is_empty() {
                return None;
            }
            if {
                let &(ref last, index) = self.current_path.last().unwrap();
                index >= last.local_items_len()
            } {
                self.current_path.pop();

                if let Some(&mut (_, ref mut index)) = self.current_path.last_mut() {
                    *index += 1;
                }

                continue;
            }
            let (last, index) = self.current_path.last_mut().unwrap();

            match last.body {
                ExtentTreeBody::Internal(ref int) => {
                    let subtree_block = int[*index].physical_leaf_block();

                    // XXX: Try/catch blocks would help readability.

                    match self.filesystem.disk.read_block(
                        self.filesystem,
                        BlockKind::Metadata,
                        subtree_block,
                        &mut self.block_bytes,
                    ) {
                        Ok(()) => (),
                        Err(err) => return Some(Err(err.into())),
                    }

                    let subtree =
                        match ExtentTree::from_block(last.checksum_seed, &self.block_bytes) {
                            Ok(s) => s,
                            Err(err) => return Some(Err(err.into())),
                        };

                    self.current_path.push((Cow::Owned(subtree), 0));
                    continue;
                }
                ExtentTreeBody::Leaf(ref leaves) => {
                    let leaf = leaves[*index];
                    *index += 1;
                    return Some(Ok(leaf));
                }
            }
        }
    }
}
