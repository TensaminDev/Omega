ALTER TABLE pending_iota_user_provisioning
    DROP FOREIGN KEY fk_pending_iota_user_provisioning_invitation;

CREATE TABLE invitation_id_migration_010 (
    legacy_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    numeric_id BIGINT NOT NULL UNIQUE
);

SET @invitation_migration_id := (
    SELECT COALESCE(MAX(CAST(invitation_id AS UNSIGNED)), 0)
    FROM user_invitations
    WHERE invitation_id REGEXP '^[1-9][0-9]{0,14}$'
      AND CAST(invitation_id AS UNSIGNED) <= 281474976710655
);
INSERT INTO invitation_id_migration_010 (legacy_id, numeric_id)
SELECT invitation_id, (@invitation_migration_id := @invitation_migration_id + 1)
FROM user_invitations
WHERE invitation_id NOT REGEXP '^[1-9][0-9]{0,14}$'
   OR CAST(invitation_id AS UNSIGNED) > 281474976710655
ORDER BY created_at, invitation_id;

UPDATE pending_iota_user_provisioning AS pending
JOIN invitation_id_migration_010 AS migration
    ON migration.legacy_id = pending.invitation_id
SET pending.invitation_id = CAST(migration.numeric_id AS CHAR);

UPDATE user_invitations AS invitation
JOIN invitation_id_migration_010 AS migration
    ON migration.legacy_id = invitation.invitation_id
SET invitation.invitation_id = CAST(migration.numeric_id AS CHAR),
    invitation.state = CASE
        WHEN invitation.state = 'pending' THEN 'expired'
        ELSE invitation.state
    END,
    invitation.expires_at = COALESCE(invitation.expires_at, UTC_TIMESTAMP());

UPDATE user_invitations
SET expires_at = DATE_ADD(created_at, INTERVAL 7 DAY)
WHERE expires_at IS NULL;

ALTER TABLE user_invitations
    MODIFY invitation_id BIGINT NOT NULL,
    MODIFY expires_at TIMESTAMP NOT NULL,
    MODIFY state ENUM('pending', 'provisioning', 'redeemed', 'revoked', 'expired')
        NOT NULL DEFAULT 'pending',
    ADD COLUMN password_hash BINARY(32) NULL AFTER token_hash,
    ADD COLUMN label VARBINARY(255) NULL AFTER iota_id,
    ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 AFTER state;

ALTER TABLE pending_iota_user_provisioning
    MODIFY invitation_id BIGINT NOT NULL,
    ADD CONSTRAINT fk_pending_iota_user_provisioning_invitation
        FOREIGN KEY (invitation_id) REFERENCES user_invitations(invitation_id) ON DELETE CASCADE;

DROP TABLE invitation_id_migration_010;

CREATE TABLE direct_link_keys (
    key_hash BINARY(32) NOT NULL PRIMARY KEY,
    target_kind ENUM('url', 'invitation') NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_direct_link_keys_created_at (created_at)
);

INSERT INTO direct_link_keys (key_hash, target_kind, created_at)
SELECT UNHEX(SHA2(short_key, 256)), 'url', created_at FROM short_links;

RENAME TABLE short_links TO short_links_before_hash_010;

CREATE TABLE short_links (
    key_hash BINARY(32) NOT NULL PRIMARY KEY,
    long_url TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_short_links_created_at (created_at),
    CONSTRAINT fk_short_links_direct_key
        FOREIGN KEY (key_hash) REFERENCES direct_link_keys(key_hash) ON DELETE CASCADE
);

INSERT INTO short_links (key_hash, long_url, created_at)
SELECT UNHEX(SHA2(short_key, 256)), long_url, created_at
FROM short_links_before_hash_010;

DROP TABLE short_links_before_hash_010;

CREATE TABLE invitation_short_links (
    key_hash BINARY(32) NOT NULL PRIMARY KEY,
    invitation_id BIGINT NOT NULL UNIQUE,
    CONSTRAINT fk_invitation_short_links_direct_key
        FOREIGN KEY (key_hash) REFERENCES direct_link_keys(key_hash) ON DELETE CASCADE,
    CONSTRAINT fk_invitation_short_links_invitation
        FOREIGN KEY (invitation_id) REFERENCES user_invitations(invitation_id) ON DELETE CASCADE
);
