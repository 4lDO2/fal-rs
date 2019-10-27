mod daemon;

fn main() {
    daemon::daemon::<fal_backend_apfs::Filesystem<std::fs::File>>(":apfs".as_ref())
}
