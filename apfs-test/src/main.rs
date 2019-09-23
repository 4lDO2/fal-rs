use std::fs::File;

fn main() {
    let device_path = std::env::args().nth(1).unwrap();
    let device = File::open(&device_path).unwrap();
    println!("Reading {}", device_path);

    let fs = fal_backend_apfs::filesystem::Filesystem::mount(device);

    println!("Container superblock: {:?}", fs.container_superblock);
}
