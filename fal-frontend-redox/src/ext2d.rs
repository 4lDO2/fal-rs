mod daemon;

fn main() {
    daemon::daemon::<fal_backend_ext2::Filesystem<std::fs::File>>(":ext2".as_ref())
}
