# Contributor guidance

This repository implements a default-off, bounded Matrix transport for Pi. Do not activate a live deployment, provision credentials, alter operator runtime settings, or modify client devices as part of repository development.

Security rules:

- Never commit Matrix tokens, crypto-store passphrases, production room IDs, sender IDs, message bodies, transcripts, or decrypted event fixtures.
- Never commit operator-specific hostnames, usernames, absolute home paths, runtime topology, or private infrastructure references.
- Keep room and sender allowlists exact and fail closed.
- Keep the sidecar Unix-socket-only and each deployment explicitly bounded.
- Routine logs must contain state/counters only, never message content or bearer material.
- Public examples must use neutral placeholders rather than real deployment values.

Validation:

- Run `make check` before committing.
- Keep Rust and TypeScript tests deterministic and network-free.
- Scan tracked files for credentials and operator-specific details before publishing.
