ALTER TABLE user_invitations
    ADD COLUMN short_token_hash BINARY(32) NULL AFTER token_hash;

UPDATE user_invitations AS invitation
JOIN invitation_short_links AS link
    ON link.invitation_id = invitation.invitation_id
SET invitation.short_token_hash = link.key_hash;
