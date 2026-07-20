use crate::{db::pool, error::Result};

pub async fn count() -> Result<u64> {
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM short_links")
        .fetch_one(&pool().await?)
        .await?;
    Ok(count.max(0) as u64)
}

pub async fn insert(short_key: &str, long_url: &str) -> Result<bool> {
    let result = sqlx::query("INSERT IGNORE INTO short_links (short_key, long_url) VALUES (?, ?)")
        .bind(short_key)
        .bind(long_url)
        .execute(&pool().await?)
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn get(short_key: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT long_url FROM short_links WHERE short_key = ?")
            .bind(short_key)
            .fetch_optional(&pool().await?)
            .await?,
    )
}

pub async fn delete_expired() -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM short_links WHERE created_at < CURRENT_TIMESTAMP - INTERVAL 7 DAY",
    )
    .execute(&pool().await?)
    .await?;
    Ok(result.rows_affected())
}
