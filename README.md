# Omega
The Omega is Tensamin's central Server. It maintains the centralized user Registry & manages Omikron useage.

Omega requires the non-empty `OMEGA_IDENTITY_SECRET` environment variable at startup. On first start it creates the protected private identity at `./omega.mk` and the matching public bundle at `./omega.mpkb`. Existing identity files are loaded fail-closed, so a malformed keyring or mismatched public bundle stops startup. A missing public bundle is rebuilt from a valid private keyring without generating a new identity.

## MTP routing contract

Omega preserves the MTP distinction between an absent routing field and a
present zero value on the wire. For Omega request and response operations,
both forms are rejected:

- request IDs are required and nonzero for correlation;
- authenticated sender and receiver IDs are required and nonzero;
- fields that are optional for a particular notification remain `Option` and
  are not converted to zero.
