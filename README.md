# Omega
The Omega is Tensamin's central Server. It maintains the centralized user Registry & manages Omikron useage.

On first start Omega creates an unencrypted private keyring at `./omega.mk` and the matching public bundle at `./omega.mpkb`. Existing identity files are loaded fail-closed, so a malformed keyring or mismatched public bundle stops startup. A missing public bundle is rebuilt from a valid private keyring without generating a new identity.

## MTP routing contract

Omega preserves the MTP distinction between an absent routing field and a
present zero value on the wire. For Omega request and response operations,
both forms are rejected:

- request IDs are required and nonzero for correlation;
- authenticated sender and receiver IDs are required and nonzero;
- fields that are optional for a particular notification remain `Option` and
  are not converted to zero.

## Resource limits

Short links accept HTTPS targets up to `OMEGA_SHORT_LINK_MAX_TARGET_BYTES` bytes (default 2048). Each authenticated Omikron can hold at most `OMEGA_SHORT_LINK_MAX_PER_OMIKRON` links (default 10000); allocation retries stop after `OMEGA_SHORT_LINK_INSERT_ATTEMPTS` (default 16). Existing links created before migration are unassigned and remain valid. `OMEGA_RELAY_MAX_CONCURRENT` defaults to 16 outstanding relays per connection; `OMEGA_RELAY_BYTES_PER_WINDOW` defaults to 64 MiB of encrypted payload per `OMEGA_RELAY_WINDOW_SECONDS` (default 60).

SQL pool defaults to 200 maximum and zero minimum connections. Override `OMEGA_DB_MAX_CONNECTIONS`, `OMEGA_DB_MIN_CONNECTIONS`, `OMEGA_DB_ACQUIRE_TIMEOUT_SECONDS`, `OMEGA_DB_IDLE_TIMEOUT_SECONDS`, and `OMEGA_DB_MAX_LIFETIME_SECONDS` as needed.
