use clap::{crate_authors, crate_version, App, Arg, SubCommand};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
};

use fal_frontend_fuse::FuseFilesystem;

fn main() {
    env_logger::init();
    let app = App::new("FAL FUSE frontend")
        .author(crate_authors!())
        .version(crate_version!())
        .about("Access several filesystems through FUSE")
        .arg(
            Arg::with_name("FILESYSTEM_TYPE")
                .takes_value(true)
                .required(true)
                .help("Specify the filesystem which will be used. Currently required, but in the future the filesystem type will usually be inferred.")
                .short("-t")
                .long("--fs-type")
        )
        .subcommand(
            SubCommand::with_name("mount")
                .about("Mount a filesystem")
                .author(crate_authors!())
                .version(crate_version!())
                .arg(Arg::with_name("DEVICE").required(true).index(1))
                .arg(Arg::with_name("MOUNTPOINT").required(true).index(2))
                .arg(
                    Arg::with_name("OPTIONS")
                        .short("o")
                        .long("options")
                        .takes_value(true),
                ),
        );

    let matches = app.get_matches();

    let filesystem_type = matches.value_of("FILESYSTEM_TYPE").unwrap();

    if let Some(matches) = matches.subcommand_matches("mount") {
        let device = matches.value_of("DEVICE").unwrap();
        let mount_point = matches.value_of("MOUNTPOINT").unwrap();

        let mut fuse_options = matches
            .value_of("OPTIONS")
            .map(|string| fal_frontend_fuse::parse_fs_options(string).unwrap())
            .unwrap_or_default();

        let options_owned = if let Some(options_str) = matches.value_of("OPTIONS") {
            options_str
                .split(',')
                .map(OsString::from)
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        let options = options_owned
            .iter()
            .map(|option| option.as_os_str())
            .collect::<Vec<_>>();

        let device_readonly = fs::metadata(device)
            .expect("Failed to retrieve device metadata")
            .permissions()
            .readonly();

        fuse_options.immutable |= device_readonly;

        let file = OpenOptions::new()
            .read(true)
            .write(!fuse_options.immutable)
            .open(device)
            .unwrap_or_else(|_| File::open(device).expect("Failed to open device"));

        match filesystem_type {
            #[cfg(feature = "ext2")]
            "ext2" => fuse::mount(
                FuseFilesystem::<fal_backend_ext2::Filesystem<std::fs::File>>::init(
                    file,
                    mount_point.as_ref(),
                    fuse_options,
                )
                .expect("Failed to initialize the driver"),
                &mount_point,
                &options,
            )
            .unwrap(),

            #[cfg(feature = "btrfs")]
            "btrfs" => fuse::mount(
                FuseFilesystem::<fal_backend_btrfs::Filesystem<std::fs::File>>::init(
                    file,
                    mount_point.as_ref(),
                    fuse_options,
                )
                .expect("Failed to initialize the driver"),
                &mount_point,
                &options,
            )
            .unwrap(),

            #[cfg(feature = "apfs")]
            "apfs" => fuse::mount(
                FuseFilesystem::<fal_backend_apfs::Filesystem<std::fs::File>>::init(
                    file,
                    mount_point.as_ref(),
                    fuse_options,
                )
                .expect("Failed to initialize the driver"),
                &mount_point,
                &options,
            )
            .unwrap(),

            other => panic!("unknown filesystem type: {}.", other),
        }
    } else {
        eprintln!("{}", matches.usage());
    }
}
