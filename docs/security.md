# Security defaults

[← barnabas](../README.md)

- **Certificate verification is on and there is no switch to turn it off.** A private CA goes in
  through `with_roots`, a pinned certificate or client auth through `from_config`.
- **`Credentials`' `Debug` prints `<redacted>`.** A password in a log is a security bug wearing a
  convenience's clothes.
- **SCRAM verifies the server's final signature.** Skipping it is the classic implementation
  shortcut, and it means authenticating to anyone who can complete a handshake.
- `SaslMechanism::requires_encryption()` is true for PLAIN, which sends the password in the clear.
