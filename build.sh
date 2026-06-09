cargo clippy --all --all-targets -- -D warnings -D clippy::all && \
cargo fmt --all && \
cargo test --all