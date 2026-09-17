# Cloudroom core

- Keep one Rust package and one service; app integrations call the API.
- Run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked --all-targets`.
- Use disposable databases for tests. Only the owner applies production migrations.
- SQL and application instructions live in `docs/database/`.
- Keep credentials and internal notes in ignored files. Review changes before publication.
