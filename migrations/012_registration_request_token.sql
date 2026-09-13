-- Omikron transport IDs are relay-local. Keep registration retries idempotent
-- with an operation token that remains stable when the transport ID changes.
ALTER TABLE registration_leases
    ADD COLUMN request_token BINARY(16) NULL,
    ADD CONSTRAINT uk_registration_leases_iota_token UNIQUE (iota_id, request_token);
