ALTER TABLE users
    DROP FOREIGN KEY fk_users_iota;

ALTER TABLE users
    MODIFY iota_id BIGINT NULL;

ALTER TABLE users
    ADD CONSTRAINT fk_users_iota
        FOREIGN KEY (iota_id)
        REFERENCES iotas (id)
        ON DELETE SET NULL;
