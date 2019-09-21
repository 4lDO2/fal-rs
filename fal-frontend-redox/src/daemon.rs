use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::prelude::*,
    mem,
};

use syscall::{data::Packet, SchemeMut};

use fal_frontend_redox::RedoxFilesystem;

pub fn daemon<Backend: fal::FilesystemMut<File>>(scheme: &OsStr) {
    let mut socket = File::create(scheme).expect("Failed to create scheme");

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(std::env::args().nth(1).unwrap())
        .unwrap();

    let mut scheme = RedoxFilesystem::<Backend>::init(file, "/".as_ref()); // TODO: Should this driver set the last mount path to "<scheme>:" or "/"?

    loop {
        let mut packet = Packet::default();

        while socket.read(&mut packet).unwrap() == mem::size_of::<Packet>() {
            scheme.handle(&mut packet);
            socket.write(&packet).unwrap();
        }
    }
    // TODO: Unmounting on Redox?
}
