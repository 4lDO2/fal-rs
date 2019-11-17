mod daemon;

fn main() {
    daemon::daemon::<fal_backend_ext4::Filesystem<std::fs::File>>(":ext4".as_ref())
}
