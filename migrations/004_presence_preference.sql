ALTER TABLE users
    ADD COLUMN presence_preference VARBINARY(32) NOT NULL DEFAULT 'user_online';

UPDATE users
SET presence_preference = 'user_online'
WHERE presence_preference NOT IN (
    'user_online',
    'user_idle',
    'user_dnd',
    'user_wc',
    'user_invisible'
);

ALTER TABLE users
    ADD CONSTRAINT chk_users_presence_preference
    CHECK (presence_preference IN (
        'user_online',
        'user_idle',
        'user_dnd',
        'user_wc',
        'user_invisible'
    ));
