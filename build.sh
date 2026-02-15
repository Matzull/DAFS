cargo test -p master -p datanode -p cli -p common && \
cargo clippy --all --all-targets -- -D clippy::all   && \
cargo fmt --all -- --check