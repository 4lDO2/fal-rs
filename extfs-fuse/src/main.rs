use std::{ffi::OsString, fs::OpenOptions};
use clap::{App, Arg, crate_authors, crate_version, SubCommand};

mod filesystem;

use filesystem::FuseFilesystem;

fn main() {
    let app = App::new("Extfs fuse")
        .author(crate_authors!())
        .version(crate_version!())
        .about("A userspace ext2 driver")
        .subcommand(
            SubCommand::with_name("mount")
                .about("Mount an ext2 filesystem")
                .author(crate_authors!())
                .version(crate_version!())
                .arg(
                    Arg::with_name("DEVICE")
                        .required(true)
                        .index(1)
                )
                .arg(
                    Arg::with_name("MOUNTPOINT")
                        .required(true)
                        .index(2)
                )
                .arg(
                    Arg::with_name("OPTIONS")
                        .short("o")
                        .long("options")
                        .takes_value(true)
                )
        );

    let matches = app.get_matches();

    if let Some(matches) = matches.subcommand_matches("mount") {
        let device = matches.value_of("DEVICE").unwrap();
        let mount_point = matches.value_of("MOUNTPOINT").unwrap();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(device).expect("Failed to open filesystem device");

        let filesystem = FuseFilesystem::init(file).expect("Failed to initialize the driver");

        if let Some(options_str) = matches.value_of("OPTIONS") {
            let options_owned = options_str.split(',').map(OsString::from).collect::<Vec<_>>();
            let options = options_owned.iter().map(|option| option.as_os_str()).collect::<Vec<_>>();

            fuse::mount(filesystem, &mount_point, &options).unwrap();
        } else {
            fuse::mount(filesystem, &mount_point, &[]).unwrap();
        }
    } else {
        eprintln!("{}", matches.usage());
    }
}
