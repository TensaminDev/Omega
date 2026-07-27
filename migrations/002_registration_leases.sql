-- Registration IDs are allocated before the user row is created.  Keep the
-- allocation durable and bound to the Iota that requested it so a completed
-- registration can safely be retried after any response is lost.
CREATE TABLE registration_leases (
    token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE,
    iota_id BIGINT NOT NULL,
    expires_at DATETIME NOT NULL,
    completed_at DATETIME NULL,
    CONSTRAINT fk_registration_leases_iota
        FOREIGN KEY (iota_id) REFERENCES iotas (id),
    INDEX idx_registration_leases_expiry (expires_at)
);
