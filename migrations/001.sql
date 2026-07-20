CREATE TABLE IF NOT EXISTS users (
    id BIGINT NOT NULL PRIMARY KEY,
    iota_id BIGINT NOT NULL,
    username VARBINARY(255) NOT NULL,
    display VARBINARY(255),
    status VARBINARY(255),
    about VARBINARY(1000),
    avatar BLOB,
    sub_level INT NOT NULL DEFAULT 0,
    sub_end BIGINT NOT NULL DEFAULT 0,
    public_key BLOB NOT NULL,
    token BLOB NOT NULL,
    UNIQUE KEY uk_users_username (username),
    UNIQUE KEY uk_users_iota_id (iota_id)
);

CREATE TABLE IF NOT EXISTS iotas (
    id BIGINT NOT NULL PRIMARY KEY,
    public_key BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS omikrons (
    id BIGINT NOT NULL PRIMARY KEY,
    public_key BLOB NOT NULL,
    location VARBINARY(255) NOT NULL,
    ip_address VARBINARY(45) NOT NULL,
    port INT NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sender_id BIGINT NOT NULL,
    receiver_id BIGINT NOT NULL,
    amount BIGINT NOT NULL DEFAULT 1,
    UNIQUE KEY uk_notifications_sender_receiver (sender_id, receiver_id),
    INDEX idx_notifications_receiver (receiver_id)
);

CREATE TABLE IF NOT EXISTS short_links (
    short_key VARCHAR(12) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    long_url TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_short_links_created_at (created_at)
);
