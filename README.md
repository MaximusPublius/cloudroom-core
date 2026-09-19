# Cloudroom core

A pre-release Rust service for running coding harnesses and preserving their session history.
One Cargo package; one service per Linux VM. Harnesses make their own inference calls.
No Cloudroom account or managed-hosting service is required.

Start with the [Ubuntu 24.04 setup guide](docs/setup.md) to build from source and run your first agent through the API.

## macOS GUI beta

[Download the Cloudroom GUI](https://github.com/davidondrej/cloudroom-installer/releases/tag/gui-v0.43.1-beta) for Apple Silicon Macs (M1 or newer) running macOS 13+.
This beta is not notarized by Apple; see the release page for installation instructions.
Downloads are public, but hosted cloud access remains invite-only. The core does not require the GUI.

## Build and test

Requires Rust 1.89+, Git, and the supported harness runtimes.

```sh
cargo build --locked --release
cargo test --locked --all-targets
```

Apply the two SQL files in `docs/database/` to a dedicated PostgreSQL database as its owner.
The service never applies migrations. Keep database and core credentials server-side.

- [Installation and workload protection](docs/storage.md)
- [Harness configuration and verification](docs/harnesses.md)
- [Session lifecycle](docs/session-lifecycle.md)
- [Diagnostics](docs/observability.md)
- [Dashboard API](docs/dashboard.md)

## Contributing

Contributions and pull requests are welcome from day one. Open an issue to discuss larger changes.
Development happens in a private monorepo; accepted contributions are included in future public snapshots.

This is a development snapshot, not a claim that all planned features are complete.
Licensed under [Apache 2.0](LICENSE).
