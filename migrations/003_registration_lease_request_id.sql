-- A relay retry uses the original MTP request ID.  Make allocation idempotent
-- for that authenticated Iota/request pair, so a lost response returns the
-- same user ID and lease rather than allocating another one.
ALTER TABLE registration_leases
    -- Existing leases predate retry correlation; leave their request ID NULL
    -- rather than assigning a shared sentinel that could violate uniqueness.
    ADD COLUMN request_id INT UNSIGNED NULL,
    ADD CONSTRAINT uk_registration_leases_iota_request UNIQUE (iota_id, request_id);
