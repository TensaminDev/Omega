ALTER TABLE short_links
    ADD COLUMN creator_omikron_id BIGINT NULL,
    ADD INDEX idx_short_links_creator (creator_omikron_id);
