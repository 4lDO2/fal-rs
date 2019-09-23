use std::fs::File;

mod checkpoint;
mod filesystem;
mod superblock;

fn main() {
    let device_path = std::env::args().nth(1).unwrap();
    let device = File::open(&device_path).unwrap();
    println!("Reading {}", device_path);

    let fs = filesystem::Filesystem::mount(device);

    println!("Container superblock: {:?}", fs.container_superblock);
}
