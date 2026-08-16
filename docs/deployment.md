# Generic deployment guide

This guide describes the deployment contract without assuming a specific Linux distribution, secret manager, host name, service manager, or Pi runtime layout.

## Runtime ownership

Run one sidecar instance per Matrix account/room/sender boundary.

Recommended ownership:

- a dedicated unprivileged operating-system user
- a user or system service manager that can restart the sidecar
- one private runtime directory for the Unix socket
- one private persistent directory containing:
  - the encrypted `matrix-sdk` crypto store
  - the transport SQLite state database
  - its required `media-tmp` sibling directory
- one Pi runtime that loads the extension and has access to the socket

The sidecar has no TCP listener. Because its socket is mode `0600`, the normal deployment runs the Pi runtime and sidecar as the same OS user. A different-user deployment requires an explicitly designed and reviewed IPC permission model. The service manager should create private parent directories before startup and should not relax the permissions enforced by the sidecar.

## Secret materialization

Provision these values outside the repository:

- Matrix access token
- Matrix crypto-store passphrase
- exact encrypted room ID
- exact allowed sender Matrix user ID

Place the token and passphrase in separate private regular files. The sidecar rejects empty files and files with group/world permission bits. Treat room and sender identifiers as sensitive deployment metadata even though they are not authentication secrets.

Use any secret manager that can materialize private files before service startup. Do not expose secret values through unit-file text, process arguments, logs, source control, or world-readable environment files.

## Example filesystem layout

The following is illustrative; choose paths appropriate to your service manager:

```text
<state-root>/
  crypto/
  state.sqlite
  media-tmp/

<runtime-root>/
  pi-matrix-agent.sock

<secrets-root>/
  access-token
  store-passphrase
```

Requirements:

- `<state-root>` and `<runtime-root>` are private to and owned by the runtime user; never use a shared directory such as `/tmp` as `<runtime-root>`.
- `media-tmp` is the literal sibling of `state.sqlite` required by `MATRIX_AGENT_MEDIA_TEMP_PATH`.
- Secret files are private regular files.
- The extension and sidecar use the same socket path.

## Service-manager contract

A service definition should:

1. run as the dedicated unprivileged user;
2. depend on network availability and secret materialization;
3. set all `MATRIX_AGENT_*` variables described in the README;
4. set a restrictive umask such as `0077`;
5. create private state/runtime directories before execution;
6. restart on unexpected failure with bounded backoff;
7. avoid logging the complete environment;
8. execute `pi-matrix-transport-sidecar` in the foreground;
9. stop cleanly with the service manager's normal termination signal.

Do not enable cross-signing repair in the normal service definition. Use it only in a deliberate, time-bounded recovery invocation and disable it immediately afterward.

## Staged activation

1. Build and test the exact source revision to be deployed.
2. Create the Matrix account and dedicated device, obtain an access token, and record the user/device IDs securely.
3. Create or select one encrypted room; join the transport account and establish the exact sender allowlist.
4. Materialize private secret files and runtime/state directories.
5. Install and independently test the transcription and TTS command adapters.
6. Start the sidecar with the Pi extension still disabled.
7. Verify the sidecar remains active, the room is joined/encrypted, and the local `status` IPC operation returns `ready`.
8. If required, complete a controlled cross-signing bootstrap/repair and then restart with repair disabled and verification required.
9. Back up the encrypted crypto store and transport state according to your recovery policy.
10. Load the extension into one Pi runtime, configure the shared socket path, and explicitly enable it.
11. Perform acceptance tests:
    - text round trip
    - audio transcription and playable audio response
    - typing/progress feedback
    - two-message FIFO ordering
    - no duplicate reply after retry
    - sidecar restart recovery
    - Pi runtime restart/release recovery
    - rejection of messages from other rooms and senders
    - privacy-safe logs
12. Observe a soak period before expanding scope.

## Monitoring

At minimum, monitor:

- service active/restart state
- IPC `status` queue counters
- stuck claimed rows or continuously growing queue depth
- repeated transcription/TTS failures
- Matrix sync or device-trust failures
- duplicate-response reports
- disk usage for the crypto store and SQLite state

Do not include message bodies, transcripts, access tokens, passphrases, room IDs, sender IDs, or decrypted fixtures in monitoring output.

## Rollback

1. Disable or unload the Pi extension first so it stops claiming new work.
2. Stop the sidecar.
3. Confirm the Unix socket is gone and no process retains it.
4. Verify the Pi runtime remains usable through an independent administration path.
5. Preserve the encrypted crypto store and SQLite state for diagnosis unless your incident policy requires secure deletion.
6. Restore the previously tested package/configuration if rollback is due to a regression.
7. Re-enable only after the standard acceptance checks pass again.

## Recovery notes

- Claimed queue rows are requeued at sidecar startup.
- Outbound idempotency state prevents a successful answer from being duplicated after an ambiguous retry.
- The local encrypted crypto store is required to preserve device/session continuity.
- Server-side key backup may improve recovery but is outside this repository's implementation scope.
