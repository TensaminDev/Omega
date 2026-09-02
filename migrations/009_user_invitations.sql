CREATE TABLE user_invitations (
    invitation_id CHAR(36) NOT NULL,
    token_hash BINARY(32) NOT NULL,
    iota_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    state ENUM('pending', 'redeemed', 'revoked', 'expired') NOT NULL DEFAULT 'pending',
    redeemed_user_id BIGINT NULL,
    redeemed_at TIMESTAMP NULL,
    revoked_at TIMESTAMP NULL,
    PRIMARY KEY (invitation_id),
    KEY idx_user_invitations_target_state (iota_id, state),
    CONSTRAINT fk_user_invitations_iota
        FOREIGN KEY (iota_id) REFERENCES iotas(id) ON DELETE CASCADE
);

CREATE TABLE pending_iota_user_provisioning (
    user_id BIGINT NOT NULL,
    iota_id BIGINT NOT NULL,
    invitation_id CHAR(36) NOT NULL,
    username VARBINARY(255) NOT NULL,
    public_key BLOB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, iota_id),
    CONSTRAINT fk_pending_iota_user_provisioning_iota
        FOREIGN KEY (iota_id) REFERENCES iotas(id) ON DELETE CASCADE,
    CONSTRAINT fk_pending_iota_user_provisioning_invitation
        FOREIGN KEY (invitation_id) REFERENCES user_invitations(invitation_id) ON DELETE CASCADE
);
