use crate::{
    db::pool,
    error::{OmegaError, Result},
    models::{IotaId, User, UserId},
};
use mtp::crypto::PublicKeyBundle;
use sqlx::FromRow;

pub async fn get_register_id() -> Result<UserId> {
    let bytes = *uuid::Uuid::now_v7().as_bytes();
    let id =
        i64::from_be_bytes(bytes[8..].try_into().map_err(|_| {
            OmegaError::Validation("generated ID has an invalid length".to_string())
        })?) & i64::MAX;
    Ok(UserId::from(id.max(1)))
}

const USER_BY_USERNAME_QUERY: &str = "SELECT id, iota_id, username, display, status, about, avatar, sub_level, sub_end, public_key, token FROM users WHERE username = ?";
const USER_BY_ID_QUERY: &str = "SELECT id, iota_id, username, display, status, about, avatar, sub_level, sub_end, public_key, token FROM users WHERE id = ?";
const USERS_BY_IOTA_ID_QUERY: &str = "SELECT id, iota_id, username, display, status, about, avatar, sub_level, sub_end, public_key, token FROM users WHERE iota_id = ?";

#[derive(FromRow)]
struct UserRow {
    id: i64,
    iota_id: i64,
    username: Vec<u8>,
    display: Option<Vec<u8>>,
    status: Option<Vec<u8>>,
    about: Option<Vec<u8>>,
    avatar: Option<Vec<u8>>,
    sub_level: i32,
    sub_end: i64,
    public_key: Vec<u8>,
    token: Vec<u8>,
}

impl TryFrom<UserRow> for User {
    type Error = sqlx::Error;

    fn try_from(row: UserRow) -> std::result::Result<Self, Self::Error> {
        let public_key = PublicKeyBundle::from_bytes(&row.public_key)
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let decode =
            |value| String::from_utf8(value).map_err(|error| sqlx::Error::Decode(Box::new(error)));
        Ok(User {
            id: row.id.into(),
            iota_id: row.iota_id.into(),
            username: decode(row.username)?,
            display: row.display.map(decode).transpose()?,
            status: row.status.map(decode).transpose()?,
            about: row.about.map(decode).transpose()?,
            avatar: row.avatar,
            sub_level: row.sub_level,
            sub_end: row.sub_end,
            public_key,
            token: decode(row.token)?,
        })
    }
}

pub async fn get_by_username(username: &str) -> Result<User> {
    let row = sqlx::query_as::<_, UserRow>(USER_BY_USERNAME_QUERY)
        .bind(username)
        .fetch_optional(&pool().await?)
        .await?
        .ok_or(OmegaError::NotFound)?;
    row.try_into().map_err(OmegaError::from)
}

pub async fn get_by_user_id(id: UserId) -> Result<User> {
    let row = sqlx::query_as::<_, UserRow>(USER_BY_ID_QUERY)
        .bind(id.0)
        .fetch_optional(&pool().await?)
        .await?
        .ok_or(OmegaError::NotFound)?;
    row.try_into().map_err(OmegaError::from)
}

pub async fn get_users_by_iota_id(id: IotaId) -> Result<Vec<User>> {
    let rows = sqlx::query_as::<_, UserRow>(USERS_BY_IOTA_ID_QUERY)
        .bind(id.0)
        .fetch_all(&pool().await?)
        .await?;
    rows.into_iter()
        .map(|row| row.try_into().map_err(OmegaError::from))
        .collect()
}

async fn update(
    id: UserId,
    query: &'static str,
    value: impl Send + sqlx::Encode<'static, sqlx::MySql> + sqlx::Type<sqlx::MySql> + 'static,
) -> Result<()> {
    sqlx::query(query)
        .bind(value)
        .bind(id.0)
        .execute(&pool().await?)
        .await?;
    Ok(())
}

pub async fn change_username(id: UserId, value: String) -> Result<()> {
    update(
        id,
        "UPDATE users SET username = ? WHERE id = ?",
        value.into_bytes(),
    )
    .await
}
pub async fn change_display_name(id: UserId, value: String) -> Result<()> {
    update(
        id,
        "UPDATE users SET display = ? WHERE id = ?",
        value.into_bytes(),
    )
    .await
}
pub async fn change_avatar(id: UserId, value: String) -> Result<()> {
    update(
        id,
        "UPDATE users SET avatar = ? WHERE id = ?",
        value.into_bytes(),
    )
    .await
}
pub async fn change_about(id: UserId, value: String) -> Result<()> {
    update(
        id,
        "UPDATE users SET about = ? WHERE id = ?",
        value.into_bytes(),
    )
    .await
}
pub async fn change_status(id: UserId, value: String) -> Result<()> {
    update(
        id,
        "UPDATE users SET status = ? WHERE id = ?",
        value.into_bytes(),
    )
    .await
}

pub async fn change_iota_id(id: UserId, value: IotaId) -> Result<()> {
    sqlx::query("UPDATE users SET iota_id = ? WHERE id = ?")
        .bind(value.0)
        .bind(id.0)
        .execute(&pool().await?)
        .await?;
    Ok(())
}
pub async fn change_token(id: UserId, value: String) -> Result<()> {
    update(
        id,
        "UPDATE users SET token = ? WHERE id = ?",
        value.into_bytes(),
    )
    .await
}
pub async fn delete_user(id: UserId) -> Result<()> {
    sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(id.0)
        .execute(&pool().await?)
        .await?;
    Ok(())
}

pub async fn change_keys(id: UserId, public_key: PublicKeyBundle) -> Result<()> {
    sqlx::query("UPDATE users SET public_key = ? WHERE id = ?")
        .bind(public_key.as_bytes())
        .bind(id.0)
        .execute(&pool().await?)
        .await?;
    Ok(())
}

pub async fn register_complete_user(
    id: UserId,
    username: String,
    public_key: PublicKeyBundle,
    iota_id: IotaId,
    token: String,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO users (id, username, public_key, iota_id, token) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(id.0)
    .bind(username.into_bytes())
    .bind(public_key.as_bytes())
    .bind(iota_id.0)
    .bind(token.into_bytes())
    .execute(&pool().await?)
    .await?;
    Ok(())
}
