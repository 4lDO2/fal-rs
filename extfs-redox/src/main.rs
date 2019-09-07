use std::{
    io::prelude::*,
    fs::File,
    mem,
};

use syscall::{data::Packet, Scheme};

struct ExtfsScheme {
}

impl Scheme for ExtfsScheme {
    
}


fn main() {
    let mut socket = File::create(":ext2").expect("Failed to create scheme.");

    let scheme = ExtfsScheme {};

    loop {
        let mut packet = Packet::default();

        while socket.read(&mut packet).unwrap() == mem::size_of::<Packet>() {
            scheme.handle(&mut packet);
            socket.write(&packet).unwrap();
        }
    }
}
