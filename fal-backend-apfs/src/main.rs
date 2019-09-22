use std::fs::File;

mod superblock;

fn main() {
    let device_path = std::env::args().nth(1).unwrap();
    let mut device = File::open(&device_path).unwrap();
    println!("Reading {}", device_path);

    let superblock = superblock::Superblock::load(&mut device);
    println!("Superblock: {:?}", superblock);
}
