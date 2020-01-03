use std::fs::File;

use fal::BasicDevice;

fn main() {
    let device_path = std::env::args().nth(1).unwrap();
    let device = BasicDevice::new(File::open(&device_path).unwrap());
    println!("Reading {}", device_path);

    let fs = fal_backend_btrfs::filesystem::Filesystem::mount(device);

    println!("Superblock: {:?}", fs.superblock);
}
