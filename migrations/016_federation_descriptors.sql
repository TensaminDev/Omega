ALTER TABLE iotas
    ADD COLUMN node_id VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD COLUMN node_descriptor LONGTEXT NULL,
    ADD COLUMN node_descriptor_revision BIGINT NULL,
    ADD COLUMN node_descriptor_valid_until BIGINT NULL,
    ADD UNIQUE KEY uk_iotas_node_id (node_id);

CREATE TABLE omega_principal_descriptors (
    user_id BIGINT NOT NULL PRIMARY KEY,
    source_hash BINARY(32) NOT NULL,
    revision BIGINT NOT NULL,
    issued_at BIGINT NOT NULL,
    valid_until BIGINT NULL,

    CONSTRAINT fk_omega_principal_descriptors_user
        FOREIGN KEY (user_id)
        REFERENCES users (id)
        ON DELETE CASCADE
);
