mod daemon;

fn main() {
    daemon::daemon::<fal_backend_apfs::Filesystem<fal::BasicDevice<std::fs::File>>>(
        ":apfs".as_ref(),
    )
}
