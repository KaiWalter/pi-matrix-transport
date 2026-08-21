> Disclaimer: This repository is AI-generated and may contain errors. Review and validate all changes before production use.

# pi-matrix-transport

`pi-matrix-transport` connects a [Pi coding agent](https://github.com/badlogic/pi-mono) to one or more end-to-end encrypted Matrix rooms.

It is intended for a small, tightly controlled deployment: one Matrix account/device owner sidecar, one allowed sender, explicit room bindings, and FIFO conversation handling. Incoming Matrix text or audio is delivered to Pi; Pi's final answer is returned as encrypted Matrix text. For an audio-origin turn, the sidecar sends encrypted text detail plus an encrypted MP3 spoken overview.

The transport is **default-off** and fails closed unless both components are explicitly enabled.

## What it provides

- End-to-end encrypted Matrix send and receive through `matrix-sdk`
- Exact room and sender allowlists, including room-scoped claims for dedicated worker lanes
- Durable SQLite FIFO queue, event deduplication, and outbound idempotency
- A mode-`0600` Unix-socket API between Matrix and Pi; no TCP listener
- Text input and bounded encrypted audio input with an external transcription command
- Markdown-aware encrypted text replies
- Audio-origin replies deliver encrypted text detail plus a spoken encrypted MP3 overview through an external text-to-speech command (text-only if synthesis fails)
- Typing notifications and one sanitized progress notice (`Processing…`, `Still working…`, `Done.` or `Stopped.`)
- Optional enforcement that the configured Matrix device is cross-signed and verified

It does **not** stream private model reasoning. Activity messages contain fixed status text only.

## Architecture

```text
Matrix homeserver
      │ encrypted sync/send
      ▼
Rust sidecar
  ├─ matrix-sdk encrypted crypto store
  ├─ SQLite queue, deduplication, and idempotency state
  ├─ bounded audio download/transcription and reply synthesis
  └─ Unix socket (NDJSON, mode 0600)
      │ claim / activity / send / release
      ▼
Pi extension
  ├─ polls only while the Pi session is available
  ├─ injects one claimed Matrix turn into Pi
  ├─ captures the final assistant answer
  └─ retries delivery without duplicating the Matrix response
```

The sidecar owns Matrix credentials, encryption state, filtering, persistence, and media. The Pi extension never connects directly to the homeserver and does not read secret files.

## Architectural prerequisites

### Operating system and filesystem

- Linux or another Unix environment with Unix-domain sockets and Unix permission bits. The current implementation uses Linux/Unix-specific APIs and is not Windows-native.
- A dedicated unprivileged runtime user is strongly recommended. By default, the sidecar and Pi runtime must run as the same operating-system user because the socket is mode `0600`; a different-user design requires an explicitly reviewed IPC permission model.
- Private writable locations for:
  - the `matrix-sdk` crypto store
  - the transport SQLite database
  - a temporary media directory
  - the Unix socket
- The state directory and socket parent must not be group/world accessible. The sidecar enforces private permissions and rejects unsafe state or secret files. The socket parent must be owned by the runtime user; do not place the socket directly under a shared directory such as `/tmp`.

### Matrix

- A Matrix homeserver reachable from the sidecar.
- A normal Matrix account and a dedicated device ID for the transport.
- A non-interactive access token for that account. This project restores an existing session; it does not implement password login.
- One room that the account has already joined and that has end-to-end encryption enabled.
- One exact Matrix sender user ID to allow. Messages from all other users and rooms are ignored.
- If the verified-device gate is enabled, complete local cross-signing keys and a self-cross-signed configured device are required. Repair/bootstrap may require homeserver-specific user-interactive authentication and should only be enabled for a controlled recovery window.

### Pi extension runtime

- A Pi version that supports TypeScript extensions and these lifecycle/API surfaces:
  - `session_start`
  - `agent_end`
  - `agent_settled`
  - `session_shutdown`
  - `ctx.isIdle()`
  - `pi.sendUserMessage()`
- Node.js with TypeScript type stripping for the included tests. Use the Node version supported by your Pi installation.

### Build toolchain

The supplied `Makefile` uses Nix to provide:

- Rust and Cargo
- rustfmt and Clippy
- `pkg-config`
- OpenSSL development files
- Node.js from the host environment for extension tests

A non-Nix build is possible with an equivalent stable Rust toolchain, OpenSSL development headers, `pkg-config`, and Node.js.

### External media commands

Both command paths are currently required at sidecar startup, even if you initially intend to use text only.

The transcription executable is invoked as:

```text
<transcribe-command> <audio-file>
```

It must emit a non-empty UTF-8 transcript on stdout and exit successfully.

The text-to-speech executable is invoked as:

```text
<tts-command> --stdin --out <output-file> --voice <voice-name>
```

It receives plain speech text on stdin and must create a non-empty MP3 file at the supplied output path.

## Build and test

```bash
git clone https://github.com/KaiWalter/pi-matrix-transport.git
cd pi-matrix-transport
make check
```

`make check` runs Rust formatting, Clippy, unit tests, a locked build, the default-off assertion, and TypeScript extension tests.

To build the sidecar directly with an already prepared Rust/OpenSSL environment:

```bash
cd sidecar
cargo build --locked --release
```

The binary is then available at `sidecar/target/release/pi-matrix-transport-sidecar`.

## Configuration

### Sidecar

Every variable in the following table is required unless marked optional.

| Variable | Purpose |
|---|---|
| `MATRIX_AGENT_ENABLED` | Must equal `1`; otherwise startup fails closed. |
| `MATRIX_AGENT_HOMESERVER` | Matrix client API base URL. |
| `MATRIX_AGENT_USER_ID` | Matrix user ID of the transport account. |
| `MATRIX_AGENT_DEVICE_ID` | Dedicated Matrix device ID restored with the access token. |
| `MATRIX_AGENT_ACCESS_TOKEN_FILE` | Private regular file containing only the access token. |
| `MATRIX_AGENT_STORE_PATH` | Private directory for the encrypted `matrix-sdk` store. |
| `MATRIX_AGENT_STORE_PASSPHRASE_FILE` | Private regular file containing the crypto-store passphrase. |
| `MATRIX_AGENT_STATE_DB` | SQLite queue/state database path. |
| `MATRIX_AGENT_SOCKET` | Unix socket path shared with the Pi extension. |
| `MATRIX_AGENT_MEDIA_TEMP_PATH` | Must be the `media-tmp` sibling of `MATRIX_AGENT_STATE_DB`. |
| `MATRIX_AGENT_TRANSCRIBE_COMMAND` | Executable transcription command path. |
| `MATRIX_AGENT_TTS_COMMAND` | Executable text-to-speech command path. |
| `MATRIX_AGENT_TTS_VOICE` | Voice identifier passed to the TTS command. |
| `MATRIX_AGENT_ROOM_ID` | Default encrypted room allowlist entry. Treat as sensitive deployment data. |
| `MATRIX_AGENT_ROOM_IDS` | Optional comma-separated additional encrypted room IDs to preload as allowlisted bindings. |
| `MATRIX_AGENT_SENDER_ID` | Exact sender allowlist entry. Treat as sensitive deployment data. |
| `MATRIX_AGENT_REQUIRE_VERIFIED_DEVICE` | Optional; `1` enforces the verified-device gate. |
| `MATRIX_AGENT_ALLOW_CROSS_SIGNING_REPAIR` | Optional; `1` permits controlled cross-signing repair/bootstrap. Keep disabled during normal operation. |

Secret files must be non-empty regular files with no group/world permission bits. Do not put access tokens, passphrases, production room IDs, sender IDs, or decrypted events in source control.

### Pi extension

| Variable | Purpose / default |
|---|---|
| `PI_MATRIX_AGENT_ENABLED` | Must equal `1`; otherwise the extension remains inactive. |
| `PI_MATRIX_AGENT_SOCKET` | Required when enabled. Must exactly match `MATRIX_AGENT_SOCKET` and reside in a private directory owned by the runtime user. |
| `PI_MATRIX_AGENT_POLL_MS` | Poll interval; default `1000`, clamped to 250–30000 ms. |
| `PI_MATRIX_AGENT_IDEMPOTENCY_PREFIX` | Outbound idempotency namespace; default `matrix-reply`. |
| `PI_MATRIX_AGENT_PROMPT_TAG` | Tag placed on injected turns; default `matrix`. |
| `PI_MATRIX_AGENT_LABEL` | Content-free operational log label; default `Matrix lane`. |
| `PI_MATRIX_AGENT_ROOM_ID` | Optional room-scoped claim filter for dedicated project workers. |

The extension entry point is declared in `extension/package.json`. Install or reference that directory using the local-extension mechanism supported by your Pi deployment, then set its socket path to exactly the same path used by the sidecar.

## Generic integration sequence

1. Create a dedicated Matrix account/device and an encrypted room.
2. Invite and join the transport account before starting the sidecar.
3. Choose the one room and one sender that will be accepted.
4. Create private runtime, state, crypto-store, media, and secret-file locations. Run both components as the same OS user unless you have designed a separate secure socket-sharing policy.
5. Install executable transcription and TTS adapters that satisfy the contracts above.
6. Start the sidecar with `MATRIX_AGENT_ENABLED=1`, but leave the Pi extension disabled.
7. Query the local socket with `{"op":"status"}` and confirm a `ready` response.
8. Load the extension into one Pi runtime, point it at the same socket, and set `PI_MATRIX_AGENT_ENABLED=1`.
9. Perform text, audio, ordering, restart-recovery, rejection, and duplicate-delivery acceptance tests.
10. Keep another administration path available so the transport can be disabled without using Matrix itself.

See [`docs/deployment.md`](docs/deployment.md) for a generic service-manager deployment and rollback checklist.

## Local socket protocol

The protocol is one newline-terminated JSON request and one newline-terminated JSON response per Unix-socket connection. Requests are capped at 65,536 bytes and reject unknown fields.

Operations:

- `{"op":"status"}`
- `{"op":"claim"}`
- `{"op":"activity_start","event_id":"..."}`
- `{"op":"activity_heartbeat","event_id":"...","status_event_id":"...","long_running":false}`
- `{"op":"activity_stop","event_id":"...","status_event_id":"...","outcome":"done"}`
- `{"op":"send","event_id":"...","idempotency_key":"...","body":"..."}`
- `{"op":"release","event_id":"..."}`

Representative statuses are `ready`, `claimed`, `empty`, `activity_started`, `activity_refreshed`, `activity_stopped`, `sent`, `duplicate`, `released`, and `unchanged`. Generic failures are returned as `invalid_request` or `operation_failed`; sensitive error internals are not sent over IPC.

Outbound Matrix transaction IDs are deterministically derived from idempotency inputs, making retries safe after ambiguous network results.

## Security model

- Default-off gates exist independently in the sidecar and extension.
- The sidecar has no TCP listener; its socket is mode `0600`.
- Room and sender allowlists are exact and fail closed.
- The room must be joined and encrypted before processing begins.
- Access tokens and crypto-store passphrases are read from private files, not command-line arguments.
- The Matrix crypto store and queue state are local and private.
- Routine logs contain state transitions and counters, not message bodies or bearer material.
- Activity feedback uses fixed strings and never carries chain-of-thought, prompts, tool inputs/results, or raw errors.
- Device verification can be enforced. Cross-signing repair is a recovery capability, not a steady-state setting.
- Decrypted audio is handled in a private per-turn temporary directory and removed after processing.
- Accepted image bytes are durably stored only in the mode-0600 queue database while queued/claimed and are erased when the event completes; image bytes and base64 are never logged.

You remain responsible for host hardening, secret provisioning, Matrix account lifecycle, backups, monitoring, and incident response.

## Bounds and limitations

- One configured sender and one or more explicitly bound rooms per sidecar instance
- FIFO claim semantics per sidecar; workers can scope claims by room id
- Accepted inbound event types: plain Matrix text, Matrix audio, and Matrix images
- Replies, edits, reactions, threads, and general non-image file attachments are not handled
- Text, caption, and transcript limit: 16,000 characters
- Audio limit: 25 MiB and five minutes when duration metadata is available
- Image limit: 25 MiB; JPEG, PNG, GIF, and WebP only; file signatures are validated before queueing
- Transcription timeout: ten minutes; stdout limit: 65,536 bytes
- TTS timeout: two minutes; generated audio limit: 25 MiB
- The verified-device repair path depends on homeserver authentication policy
- No built-in interactive login, account creation, secret manager, service unit, metrics exporter, or multi-tenant routing

## Deployment status

This repository provides the transport components and generic deployment guidance. It does not ship production credentials, account identifiers, room identifiers, host-specific service configuration, or operator-specific infrastructure wiring.
