ALTER TABLE iotas
    ADD COLUMN IF NOT EXISTS public_key_hash BINARY(32) NULL;

UPDATE iotas
SET public_key_hash = UNHEX(SHA2(public_key, 256))
WHERE public_key_hash IS NULL;

/*
 * Fail before schema enforcement when one key already owns several IDs.
 * Resolve rows reported by:
 * SELECT HEX(public_key_hash) AS key_hash, COUNT(*) AS identities
 * FROM iotas GROUP BY public_key_hash HAVING COUNT(*) > 1;
 */
DROP TEMPORARY TABLE IF EXISTS iota_public_key_duplicate_guard;

CREATE TEMPORARY TABLE iota_public_key_duplicate_guard (
    guard TINYINT NOT NULL PRIMARY KEY
);

INSERT INTO iota_public_key_duplicate_guard (guard)
SELECT 1
FROM (
    SELECT public_key_hash
    FROM iotas
    GROUP BY public_key_hash
    HAVING COUNT(*) > 1
) AS duplicate_identities
CROSS JOIN (
    SELECT 1 AS copy
    UNION ALL
    SELECT 2 AS copy
) AS duplicate_guard_rows;

DROP TEMPORARY TABLE iota_public_key_duplicate_guard;

ALTER TABLE iotas
    MODIFY public_key_hash BINARY(32) NOT NULL,
    ADD UNIQUE KEY uk_iotas_public_key_hash (public_key_hash);

CREATE TABLE IF NOT EXISTS iota_key_authority (
    public_key_hash BINARY(32) NOT NULL PRIMARY KEY,
    revoked_at TIMESTAMP(3) NULL
);

INSERT INTO iota_key_authority (public_key_hash)
SELECT public_key_hash FROM iotas
ON DUPLICATE KEY UPDATE public_key_hash = VALUES(public_key_hash);
