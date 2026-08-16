SHELL := /usr/bin/env bash

.PHONY: check rust-check default-off-check extension-check

check: rust-check default-off-check extension-check

rust-check:
	openssl_dev="$$(nix build --no-link --print-out-paths 'nixpkgs#openssl.dev')"; \
	PKG_CONFIG_PATH="$$openssl_dev/lib/pkgconfig" \
	nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#rustfmt nixpkgs#clippy nixpkgs#pkg-config -c bash -lc 'cd sidecar && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test && cargo build --locked'

default-off-check:
	! env -u MATRIX_AGENT_ENABLED sidecar/target/debug/pi-matrix-transport-sidecar >/dev/null 2>sidecar/target/default-off.err
	rg -q 'disabled' sidecar/target/default-off.err

extension-check:
	cd extension && node --experimental-strip-types --test test/*.test.ts
