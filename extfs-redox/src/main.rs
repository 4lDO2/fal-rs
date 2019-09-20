use std::{
    fs::{File, OpenOptions},
    io::prelude::*,
    mem,
};

use syscall::{data::Packet, SchemeMut};

use fal_frontend_redox::RedoxFilesystem;

fn main() {
    let mut socket = File::create(":ext2").expect("Failed to create scheme");

    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(std::env::args().nth(1).unwrap())
        .unwrap();

    let mut scheme = RedoxFilesystem::<fal_backend_ext2::Filesystem<File>>::init(file, "/".as_ref()); // TODO: Should this driver set the last mount path to "ext2:" or "/"?

    println!("{:?}", scheme.inner.superblock);

    loop {
        let mut packet = Packet::default();

        while socket.read(&mut packet).unwrap() == mem::size_of::<Packet>() {
            scheme.handle(&mut packet);
            socket.write(&packet).unwrap();
        }
    }
}
