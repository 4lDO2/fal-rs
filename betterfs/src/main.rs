use std::{env, fs::File};

mod superblock;

fn main() {
    println!(
        "{:?}",
        superblock::Superblock::parse(File::open(env::args().nth(1).unwrap()).unwrap())
    );
}
