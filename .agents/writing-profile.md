---
description: Project writing profile
---

# Writing Profile

- Applies to: Shared BLE core documentation, downstream integration notes, examples, and release communication.
- Selected items: `core`, `sdk-docs`, `en-sdk`, `portable-md`.
- Conditional items:
  - Use `cross-role` for shared-core behavior that affects platform wrappers, SDK teams, firmware, QA, or downstream applications.
  - Use `release-notes` for versioned behavior, compatibility, migration, or packaging changes.
  - Use `public-docs` only for externally published integration documentation.
- Local overrides: Describe this repository as the shared Rust BLE core and distinguish core behavior from platform-specific wrapper behavior. State UUIDs, connection state, callback behavior, MTU limits, and platform assumptions precisely when they affect integrations. Do not claim downstream SDK support unless the corresponding wrapper or application path has been verified.
