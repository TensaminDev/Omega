use crate::log;
use once_cell::sync::Lazy;
use sqlx::{MySql, Pool, Row, mysql::MySqlPoolOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

/*
use crate::sql::{
    iota_omikron_tracker::{get_omikron_for_iota, track_iota_omikron, untrack_iota},
    sql::{
        change_about, change_avatar, change_display_name, change_iota_id, change_iota_key,
        change_keys, change_status, change_username, get_by_id, get_by_username, get_iota_by_id,
        get_register_id, register_complete_iota, register_complete_user,
    },
};
 */

static SQL_DB: Lazy<Arc<RwLock<Option<Pool<MySql>>>>> = Lazy::new(|| Arc::new(RwLock::new(None)));

pub async fn connect() -> Result<Pool<MySql>, sqlx::Error> {
    let user = env::var("DB_USERNAME").expect("DB_USERNAME is not set");
    let passwd = env::var("DB_PASSWD").expect("DB_PASSWD is not set");
    let table = env::var("DB_TABLE").expect("DB_TABLE is not set");

    MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "mysql://{}:{}@127.0.0.1:3306/{}",
            user, passwd, table
        ))
        .await
}
// Omega
// - Omikron
//   - Iota
//     - User
//     - User
//   - Iota
//     - User
//     - User
// - Omikron
//   - Iota
//     - User
//     - User
//   - Iota
//     - User
//     - User
pub async fn initialize_db() -> Result<(), sqlx::Error> {
    let pool = connect().await?;
    let mut db_lock = SQL_DB.write().await;
    // create tables
    // with indexes
    let _ = sqlx::query(
        "CREATE TABLE IF NOT EXISTS
        users (
        id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        username VARCHAR(15) NOT NULL UNIQUE COLLATE utf8mb4_bin,
        display VARCHAR(15) COLLATE utf8mb4_bin,
        status VARCHAR(15) COLLATE utf8mb4_bin,
        about VARCHAR(200) COLLATE utf8mb4_bin,
        avatar MEDIUMBLOB,
        sub_level INT(11) NOT NULL DEFAULT 0,
        sub_end BIGINT(20) NOT NULL,
        public_key TEXT NOT NULL COLLATE utf8mb4_bin,
        private_key_hash TEXT NOT NULL COLLATE utf8mb4_bin,
        iota_id BIGINT UNSIGNED NOT NULL,
        token VARCHAR(255) NOT NULL UNIQUE COLLATE utf8mb4_bin
        )",
    )
    .execute(&pool)
    .await;
    let _ = sqlx::query(
        "CREATE TABLE IF NOT EXISTS
        iotas (
        id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        public_key VARCHAR(255) NOT NULL COLLATE utf8mb4_bin
        )",
    )
    .execute(&pool)
    .await;
    let _ = sqlx::query(
        "CREATE TABLE IF NOT EXISTS
        omikrons (
        id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
        is_active INT(1) NOT NULL DEFAULT 0,
        public_key VARCHAR(255) NOT NULL COLLATE utf8mb4_bin,
        location VARCHAR(255) NOT NULL COLLATE utf8mb4_bin,
        ip_address VARCHAR(255) NOT NULL COLLATE utf8mb4_bin
        )",
    )
    .execute(&pool)
    .await;

    *db_lock = Some(pool);
    Ok(())
}

// ==========================================================================================
//                                           REGISTER
// ==========================================================================================
pub static CURRENT_MILLI_USED: Lazy<Arc<AtomicU64>> = Lazy::new(|| Arc::new(AtomicU64::new(0)));
pub static CURRENT_REGISTER_PROCESS: Lazy<Arc<RwLock<Vec<u64>>>> =
    Lazy::new(|| Arc::new(RwLock::new(Vec::new())));

pub async fn get_register_id() -> u64 {
    let mut current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    loop {
        let current_locked = CURRENT_MILLI_USED.load(Ordering::SeqCst);

        if current_locked < current_time {
            let result = CURRENT_MILLI_USED.compare_exchange(
                current_locked,   // expected value
                current_time,     // new value
                Ordering::SeqCst, // acquire/release ordering
                Ordering::SeqCst, // failure ordering
            );

            match result {
                Ok(_) => {
                    CURRENT_REGISTER_PROCESS.write().await.push(current_time);
                    return current_time;
                }
                Err(_) => {
                    continue;
                }
            }
        } else {
            current_time = current_locked + 1;
        }
    }
}

// ==========================================================================================
//                                           USERS
// ==========================================================================================

pub async fn get_by_username(
    username: &str,
) -> Result<
    (
        i64,
        i64,
        String,
        String,
        String,
        String,
        String,
        i32,
        i64,
        String,
        String,
        String,
    ),
    sqlx::Error,
> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    let row = sqlx::query(
        "SELECT id, iota_id, username, display, status, about, avatar, sub_level, sub_end, public_key, private_key_hash, token FROM users WHERE username = ?",
    )
    .bind(username)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => {
            let id: i64 = row.get("id");
            let iota_id: i64 = row.get("iota_id");
            let username: String = row.get("username");
            let display: Vec<u8> = row.get("display");
            let status: Vec<u8> = row.get("status");
            let about: Vec<u8> = row.get("about");
            let avatar: Vec<u8> = row.get("avatar");
            let sub_level: i32 = row.get("sub_level");
            let sub_end: i64 = row.get("sub_end");
            let public_key: String = row.get("public_key");
            let private_key_hash: String = row.get("private_key_hash");
            let token: Vec<u8> = row.get("token");

            Ok((
                id,
                iota_id,
                username,
                String::from_utf8_lossy(&display).to_string(),
                String::from_utf8_lossy(&status).to_string(),
                String::from_utf8_lossy(&about).to_string(),
                String::from_utf8_lossy(&avatar).to_string(),
                sub_level,
                sub_end,
                public_key,
                private_key_hash,
                String::from_utf8_lossy(&token).to_string(),
            ))
        }
        None => Err(sqlx::Error::RowNotFound),
    }
}

pub async fn get_by_id(
    id: i64,
) -> Result<
    (
        i64,
        i64,
        String,
        String,
        String,
        String,
        String,
        i32,
        i64,
        String,
        String,
        String,
    ),
    sqlx::Error,
> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    let row = sqlx::query(
        "SELECT id, iota_id, username, display, status, about, avatar, sub_level, sub_end, public_key, private_key_hash, token FROM users WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => {
            let id: i64 = row.get("id");
            let iota_id: i64 = row.get("iota_id");
            let username: String = row.get("username");
            let display: Vec<u8> = row.get("display");
            let status: Vec<u8> = row.get("status");
            let about: Vec<u8> = row.get("about");
            let avatar: Vec<u8> = row.get("avatar");
            let sub_level: i32 = row.get("sub_level");
            let sub_end: i64 = row.get("sub_end");
            let public_key: String = row.get("public_key");
            let private_key_hash: String = row.get("private_key_hash");
            let token: Vec<u8> = row.get("token");

            Ok((
                id,
                iota_id,
                username,
                String::from_utf8_lossy(&display).to_string(),
                String::from_utf8_lossy(&status).to_string(),
                String::from_utf8_lossy(&about).to_string(),
                String::from_utf8_lossy(&avatar).to_string(),
                sub_level,
                sub_end,
                public_key,
                private_key_hash,
                String::from_utf8_lossy(&token).to_string(),
            ))
        }
        None => Err(sqlx::Error::RowNotFound),
    }
}

pub async fn change_username(id: i64, new_username: String) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE users SET username = ? WHERE id = ?")
        .bind(new_username)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn change_display_name(id: i64, new_display: String) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE users SET display = ? WHERE id = ?")
        .bind(new_display)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn change_avatar(id: i64, new_avatar: String) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE users SET avatar = ? WHERE id = ?")
        .bind(new_avatar)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn change_about(id: i64, new_about: String) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE users SET about = ? WHERE id = ?")
        .bind(new_about)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn change_status(id: i64, new_status: String) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE users SET status = ? WHERE id = ?")
        .bind(new_status)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn change_iota_id(id: i64, new_iota_id: i64) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE users SET iota_id = ? WHERE id = ?")
        .bind(new_iota_id)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn change_keys(
    id: i64,
    new_public_key: String,
    new_private_key_hash: String,
) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE users SET public_key = ?, private_key_hash = ? WHERE id = ?")
        .bind(new_public_key)
        .bind(new_private_key_hash)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}
pub async fn register_complete_user(
    id: i64,
    username: String,
    public_key: String,
    private_key_hash: String,
    iota_id: i64,
    token: String,
) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("INSERT INTO users (id, username, public_key, private_key_hash, iota_id, token) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(id)
        .bind(username)
        .bind(public_key)
        .bind(private_key_hash)
        .bind(iota_id)
        .bind(token)
        .execute(pool)
        .await?;

    Ok(())
}
pub async fn print_users() -> Result<(), Box<dyn std::error::Error>> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    log!("Printing users...");
    for row in sqlx::query(
        "SELECT id, iota_id, username, display, status, about, sub_level, sub_end, public_key, private_key_hash, token FROM users",
    )
    .fetch_all(pool)
    .await?
    .iter()
    {
        let id: i64 = row.get("id");
        let iota_id: i64 = row.get("iota_id");
        let username: String = row.get("username");
        let display: Vec<u8> = row.get("display");
        let status: Vec<u8> = row.get("status");
        let about: Vec<u8> = row.get("about");
        let sub_level: i32 = row.get("sub_level");
        let sub_end: i64 = row.get("sub_end");
        let public_key: String = row.get("public_key");
        let private_key_hash: String = row.get("private_key_hash");
        let token: Vec<u8> = row.get("token");

        log!(
            "User: {:?}",
            (
                id,
                iota_id,
                username,
                String::from_utf8_lossy(&display),
                String::from_utf8_lossy(&status),
                String::from_utf8_lossy(&about),
                sub_level,
                sub_end,
                public_key,
                private_key_hash,
                String::from_utf8_lossy(&token)
            )
        );
    }

    Ok(())
}

// ==========================================================================================
//                                                IOTA
// ==========================================================================================
pub async fn register_complete_iota(id: i64, public_key: String) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("INSERT INTO iotas (id, public_key) VALUES (?, ?)")
        .bind(id)
        .bind(public_key)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn delete_user(id: i64) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn get_iota_by_id(id: i64) -> Result<(i64, String), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query_as::<_, (i64, Vec<u8>)>("SELECT id, public_key FROM iotas WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .map(|(id, public_key)| (id, String::from_utf8_lossy(&public_key).to_string()))
        .ok_or_else(|| sqlx::Error::RowNotFound)
}

pub async fn change_iota_key(id: i64, new_key: String) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("UPDATE iotas SET public_key = ? WHERE id = ?")
        .bind(new_key)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn delete_iota(id: i64) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    sqlx::query("DELETE FROM iotas WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

// ==========================================================================================
//                                         OMIKRONS
// ==========================================================================================

pub async fn get_random_omikron() -> Result<(i64, String, String), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");
    let row = sqlx::query_as::<_, (i64, Vec<u8>, Vec<u8>)>("SELECT id, public_key, ip_address FROM omikrons WHERE is_active = 1 ORDER BY RAND() LIMIT 1")
        .fetch_optional(pool)
        .await?;

    match row {
        Some((id, public_key, ip_address)) => Ok((
            id,
            String::from_utf8_lossy(&public_key).to_string(),
            String::from_utf8_lossy(&ip_address).to_string(),
        )),
        None => Err(sqlx::Error::RowNotFound),
    }
}

pub async fn get_omikron_by_id(id: i64) -> Result<(String, String), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    let row = sqlx::query_as::<_, (Vec<u8>, Vec<u8>)>(
        "SELECT public_key, ip_address FROM omikrons WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((public_key, ip_address)) => Ok((
            String::from_utf8_lossy(&public_key).to_string(),
            String::from_utf8_lossy(&ip_address).to_string(),
        )),
        None => Err(sqlx::Error::RowNotFound),
    }
}

pub async fn set_omikron_active(id: i64, active: bool) -> Result<(), sqlx::Error> {
    let db_lock = SQL_DB.read().await;
    let pool = db_lock.as_ref().expect("Database pool is not initialized");

    let active = if active { 1 } else { 0 };

    sqlx::query("UPDATE omikrons SET active = ? WHERE id = ?")
        .bind(active)
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}
