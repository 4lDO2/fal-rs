use std::{env, fs::File};

use fal_backend_btrfs::superblock;

fn main() {
    println!(
        "{:?}",
        superblock::Superblock::parse(File::open(env::args().nth(1).unwrap()).unwrap())
    );
}
