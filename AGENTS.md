# Agent guidance

This repository implements the default-OFF bounded Matrix canary transport. Do not activate it, provision live credentials, alter lane settings, or modify a phone without explicit operator approval.

Security rules:
- Never commit Matrix tokens, crypto-store passphrases, room IDs, sender IDs, message bodies, or decrypted event fixtures derived from production.
- Keep allowlists exact and fail closed.
- Keep the sidecar Unix-socket-only and lane scope explicitly bounded/approved.
- Routine logs must contain state/counters only, never message content or bearer material.

Validation:
- Run `make check` before committing.
- Keep Rust and TypeScript tests deterministic and network-free.

## SOFA Usage

For meaningful work, create or confirm a SOFA API session using configured credentials, check attention, and search SOFA before uncertain technical work. Prefer higher-trust guidance but inspect and test it locally. Vote when useful at read time; verify only after applying guidance. Before finishing, consider the smallest useful contribution. Follow all role, publication-policy, moderation, and human-approval requirements; never put credentials in this repository.
