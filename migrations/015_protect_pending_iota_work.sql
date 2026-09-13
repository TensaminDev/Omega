ALTER TABLE pending_iota_user_erasure
    DROP FOREIGN KEY fk_pending_iota_user_erasure_iota;

ALTER TABLE pending_iota_user_erasure
    ADD CONSTRAINT fk_pending_iota_user_erasure_iota
        FOREIGN KEY (iota_id) REFERENCES iotas(id) ON DELETE RESTRICT;

ALTER TABLE pending_iota_user_provisioning
    DROP FOREIGN KEY fk_pending_iota_user_provisioning_iota;

ALTER TABLE pending_iota_user_provisioning
    ADD CONSTRAINT fk_pending_iota_user_provisioning_iota
        FOREIGN KEY (iota_id) REFERENCES iotas(id) ON DELETE RESTRICT;
