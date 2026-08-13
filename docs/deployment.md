# Inactive deployment contract

This document prepares deployment without changing Nix, XO, Matrix accounts, or the phone.

## Runtime ownership

- A user systemd service will own the Rust sidecar.
- The sidecar listens only on `%t/pi-matrix-xo.sock`; it has no TCP listener.
- Persistent state lives under `%h/.local/share/pi-matrix-transport/xo/`:
  - `crypto/`: encrypted `matrix-sdk` SQLite store
  - `state.sqlite`: event queue/deduplication state
- Runtime directory and files are user-only.
- The XO Pi extension is loaded only by the live XO interactive lane.

## Secret materialization

Activation must follow the established chain: 1Password → SOPS-encrypted Nix secret → `/run/secrets/*`.

Required secret files:

- XO Matrix access token
- Matrix crypto-store passphrase
- exact canary room ID
- exact Kai sender Matrix user ID

The normal XO Matrix user ID and device ID may be declarative non-secret configuration. Do not place access tokens, passphrases, room IDs, sender IDs, or decrypted messages in this repository or generated logs.

## Default-off wiring

The Home Manager module lives at `~/nix-config/features/pi-agent/matrix-xo-canary.nix` and is imported only by `kai-lenovo`. After explicit operator approval, `services.piMatrixTransportXoCanary.enable` was set to `true` with a package pinned to this GitHub source. It starts the sidecar and stages the extension outside agent directories.

The XO Telegram launcher is the only Pi startup surface that sets `PI_MATRIX_XO_ENABLED=1` and loads the staged extension. The source remains default-OFF and all other agents remain unwired.

## Activation sequence

1. Create the dedicated normal non-admin XO MAS account.
2. Materialize secrets and initialize an encrypted crypto store.
3. Create/select one encrypted room through Element Web and set exact allowlists.
4. Start the sidecar with Pi extension absent; verify `status` over the Unix socket.
5. Capture XO settings and runtime backup.
6. Obtain explicit activation approval.
7. Add the extension only to XO, restart/reload only XO, and perform one text round trip.

## Rollback

1. Remove or set `PI_MATRIX_XO_ENABLED=0` in XO.
2. Stop the sidecar.
3. Restore pre-activation XO settings if changed.
4. Verify XO Telegram and Phase 1 Matrix health.
5. Preserve encrypted sidecar state for diagnosis unless explicit deletion is approved.
