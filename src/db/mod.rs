use crate::error::{OmegaError, Result};
use once_cell::sync::Lazy;
use sqlx::{MySql, Pool, mysql::MySqlPoolOptions};
use std::{env, sync::Arc};
use tokio::sync::RwLock;

pub mod iota_repo;
pub mod notification_repo;
pub mod omikron_repo;
pub mod short_link_repo;
pub mod user_repo;

pub type DbPool = Pool<MySql>;

static POOL: Lazy<Arc<RwLock<Option<DbPool>>>> = Lazy::new(|| Arc::new(RwLock::new(None)));

pub async fn pool() -> Result<DbPool> {
    POOL.read()
        .await
        .as_ref()
        .cloned()
        .ok_or(OmegaError::DatabaseNotInitialized)
}

pub async fn initialize() -> Result<()> {
    let url =
        env::var("DB_URL").map_err(|_| OmegaError::Validation("DB_URL is not set".to_string()))?;
    let pool = MySqlPoolOptions::new()
        .max_connections(200)
        .connect(&url)
        .await?;
    sqlx::migrate!()
        .run(&pool)
        .await
        .map_err(|error| OmegaError::Validation(format!("database migration failed: {error}")))?;
    *POOL.write().await = Some(pool);
    Ok(())
}
