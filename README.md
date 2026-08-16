# pi-matrix-transport

Default-OFF transport for the bounded Matrix Phase 2 canary.

## Components

- `sidecar/`: Rust `matrix-sdk` process that owns the normal MAS Matrix session, encrypted SQLite crypto store, exact room/sender allowlists, durable event deduplication, encrypted media download/decryption, local Whisper transcription, and encrypted text/audio sends.
- `extension/`: dependency-free Pi extension that polls the sidecar over a mode-0600 Unix socket, injects one Matrix text or transcribed voice turn into Pi, waits for `agent_settled`, and returns exactly one final answer. Text replies retain a plain-text fallback and include Matrix-safe HTML rendered from Markdown. Voice-origin turns receive an MP3 audio attachment; TTS failure falls back to the same encrypted rich-text reply. It also exposes Matrix slash-command handling for `/reload`, `/new`, `/eco`, `/balanced`, and `/power`, and emits bounded activity feedback while a turn is active.

Both components require explicit enable flags and fail closed. Deployment wiring lives separately in `nix-config`; current bounded wiring is lane-specific and must be explicitly enabled per lane.

## Local protocol

One newline-delimited JSON request and response per Unix-socket connection:

- `{"op":"status"}`
- `{"op":"claim"}`
- `{"op":"activity_start","event_id":"..."}`
- `{"op":"activity_heartbeat","event_id":"...","status_event_id":"...","long_running":false}`
- `{"op":"activity_stop","event_id":"...","status_event_id":"...","outcome":"done"}`
- `{"op":"send","event_id":"...","idempotency_key":"...","body":"..."}`
- `{"op":"release","event_id":"..."}`

Activity operations expose only fixed phases. The sidecar maps them to typing notifications and one encrypted `m.notice` that is edited from `Processing…` to `Still working…` and finally `Done.` or `Stopped.`. Model reasoning, prompts, tool payloads, message bodies, and raw errors are never accepted as activity content.

Matrix transaction IDs are deterministically derived from idempotency inputs, so retries after ambiguous HTTP results remain idempotent at the homeserver.

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
- `MATRIX_XO_MEDIA_TEMP_PATH`
- `MATRIX_XO_TRANSCRIBE_COMMAND`
- `MATRIX_XO_TTS_COMMAND`
- `MATRIX_XO_TTS_VOICE` (Matrix-only Edge TTS voice, currently `en-GB-SoniaNeural`)
- `MATRIX_XO_ROOM_ID`
- `MATRIX_XO_SENDER_ID`
- `MATRIX_XO_REQUIRE_VERIFIED_DEVICE` (`1` required for enforced trust gate)
- `MATRIX_XO_ALLOW_CROSS_SIGNING_REPAIR` (`0` for enforced mode, temporary `1` only for explicit recovery)

The extension requires `PI_MATRIX_XO_ENABLED=1` and `PI_MATRIX_XO_SOCKET`. Initial activation remains approval-gated.

### Trust recovery + enforced rollout

Use this contract:

1. **Recovery window (explicit):** set `MATRIX_XO_ALLOW_CROSS_SIGNING_REPAIR=1` only while repairing local trust material.
2. **Enforced runtime (default):** set `MATRIX_XO_REQUIRE_VERIFIED_DEVICE=1` and `MATRIX_XO_ALLOW_CROSS_SIGNING_REPAIR=0`.
3. Keep `MATRIX_XO_ALLOW_CROSS_SIGNING_REPAIR=0` for steady-state operation so the sidecar fails closed when trust drifts.


## Validation

Run `make check`. Rust tooling is obtained ephemerally through Nix; the Makefile resolves the `openssl.dev` pkg-config path explicitly and supplies modern `cargo`, `rustc`, `rustfmt`, `clippy`, and `pkg-config`. TypeScript tests use Node's built-in test runner and type stripping. Validation also proves the built sidecar fails closed when its enable flag is absent.
