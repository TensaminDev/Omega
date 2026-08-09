CREATE TABLE pending_iota_user_erasure (
    user_id BIGINT NOT NULL,
    iota_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, iota_id),
    CONSTRAINT fk_pending_iota_user_erasure_iota
        FOREIGN KEY (iota_id) REFERENCES iotas(id) ON DELETE CASCADE
);
