use crate::{db::pool, error::Result};
use mtp::crypto::sha256;

pub async fn count() -> Result<u64> {
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM short_links")
        .fetch_one(&pool().await?)
        .await?;
    Ok(count.max(0) as u64)
}

pub async fn insert(creator: i64, short_key: &str, long_url: &str, max_links: i64) -> Result<bool> {
    let key_hash = sha256(short_key.as_bytes());
    let database = pool().await?;
    let mut transaction = database.begin().await?;
    sqlx::query_scalar::<_, i64>("SELECT id FROM omikrons WHERE id = ? FOR UPDATE")
        .bind(creator)
        .fetch_one(&mut *transaction)
        .await?;
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM short_links WHERE creator_omikron_id = ?",
    )
    .bind(creator)
    .fetch_one(&mut *transaction)
    .await?;
    if count >= max_links {
        return Err(crate::OmegaError::Validation(
            "short-link quota exceeded".into(),
        ));
    }
    let reserved = sqlx::query(
        "INSERT IGNORE INTO direct_link_keys (key_hash, target_kind) VALUES (?, 'url')",
    )
    .bind(key_hash.as_slice())
    .execute(&mut *transaction)
    .await?;
    if reserved.rows_affected() == 0 {
        return Ok(false);
    }
    sqlx::query(
        "INSERT INTO short_links (key_hash, long_url, creator_omikron_id) VALUES (?, ?, ?)",
    )
    .bind(key_hash.as_slice())
    .bind(long_url)
    .bind(creator)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(true)
}

pub async fn get(short_key: &str) -> Result<Option<String>> {
    let key_hash = sha256(short_key.as_bytes());
    Ok(
        sqlx::query_scalar::<_, String>("SELECT long_url FROM short_links WHERE key_hash = ?")
            .bind(key_hash.as_slice())
            .fetch_optional(&pool().await?)
            .await?,
    )
}

pub async fn delete_expired() -> Result<u64> {
    let database = pool().await?;
    let mut transaction = database.begin().await?;
    let generic = sqlx::query(
        "DELETE FROM direct_link_keys WHERE target_kind = 'url' \
         AND created_at < CURRENT_TIMESTAMP - INTERVAL 7 DAY",
    )
    .execute(&mut *transaction)
    .await?;
    let invitations = sqlx::query(
        "DELETE direct_link_keys FROM direct_link_keys \
         JOIN invitation_short_links ON invitation_short_links.key_hash = direct_link_keys.key_hash \
         JOIN user_invitations ON user_invitations.invitation_id = invitation_short_links.invitation_id \
         WHERE direct_link_keys.target_kind = 'invitation' \
         AND (user_invitations.state IN ('redeemed', 'revoked', 'expired') \
              OR (user_invitations.state = 'pending' AND user_invitations.expires_at <= UTC_TIMESTAMP()))",
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(generic.rows_affected() + invitations.rows_affected())
}
