# pi-matrix-transport

Default-OFF transport for the XO-only Matrix Phase 2 canary.

## Components

- `sidecar/`: Rust `matrix-sdk` process that owns the normal MAS Matrix session, encrypted SQLite crypto store, exact room/sender allowlists, durable event deduplication, and encrypted sends.
- `extension/`: dependency-free Pi extension that polls the sidecar over a mode-0600 Unix socket, injects one Matrix text turn into XO, waits for `agent_settled`, and returns exactly one final answer.

The sidecar and extension are deliberately not wired into XO or startup configuration in this repository. Both require explicit enable flags and fail closed.

## Local protocol

One newline-delimited JSON request and response per Unix-socket connection:

- `{"op":"status"}`
- `{"op":"claim"}`
- `{"op":"send","event_id":"...","idempotency_key":"...","body":"..."}`
- `{"op":"release","event_id":"..."}`

The Matrix transaction ID is deterministically derived from `idempotency_key`, so a retry after an ambiguous HTTP result remains idempotent at the homeserver.

## Configuration

Sidecar activation requires `MATRIX_XO_ENABLED=1` plus:

- `MATRIX_XO_HOMESERVER`
- `MATRIX_XO_USER_ID`
- `MATRIX_XO_DEVICE_ID`
- `MATRIX_XO_ACCESS_TOKEN_FILE`
- `MATRIX_XO_STORE_PATH`
- `MATRIX_XO_STORE_PASSPHRASE_FILE`
- `MATRIX_XO_STATE_DB`
- `MATRIX_XO_SOCKET`
- `MATRIX_XO_ROOM_ID`
- `MATRIX_XO_SENDER_ID`

The extension requires `PI_MATRIX_XO_ENABLED=1` and `PI_MATRIX_XO_SOCKET`. Initial activation remains approval-gated.

## Validation

Run `make check`. Rust tooling is obtained ephemerally through Nix; the Makefile resolves the `openssl.dev` pkg-config path explicitly and supplies modern `cargo`, `rustc`, `rustfmt`, `clippy`, and `pkg-config`. TypeScript tests use Node's built-in test runner and type stripping. Validation also proves the built sidecar fails closed when its enable flag is absent.
