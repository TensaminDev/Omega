use crate::{
    db::pool,
    error::{OmegaError, Result},
    models::{IotaId, User, UserId},
    sql::connection_status::UserStatus,
};
use mtp::crypto::PublicKeyBundle;
use sqlx::{FromRow, MySql, QueryBuilder, Row};
use std::collections::HashMap;

pub const MAX_PROTOCOL_ID: i64 = (1_i64 << 48) - 1;
const ID_ALLOCATION_ATTEMPTS: usize = 16;

pub async fn get_register_id() -> Result<UserId> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let ts = timestamp as i64;
    if ts >= 1 && ts <= MAX_PROTOCOL_ID {
        return Ok(UserId::from(ts));
    }

    // Fall back to random if the timestamp is outside the 48-bit range.
    let id = (rand::random::<u64>() & ((1_u64 << 48) - 1)) as i64;
    Ok(UserId::from(id.max(1)))
}

pub fn valid_protocol_id(id: i64) -> bool {
    (1..=MAX_PROTOCOL_ID).contains(&id)
}

/// Allocate an ID that is durable, short-lived, and bound to the connected
/// Iota.  `token` is presented again when completing the registration.
pub async fn allocate_registration(iota_id: IotaId, request_id: u32) -> Result<(UserId, String)> {
    if !valid_protocol_id(iota_id.0) {
        return Err(OmegaError::Validation(
            "Iota ID is outside the 48-bit protocol range".into(),
        ));
    }

    for _ in 0..ID_ALLOCATION_ATTEMPTS {
        let id = get_register_id().await?;
        let token = uuid::Uuid::new_v4().to_string();
        let result = sqlx::query(
            "INSERT INTO registration_leases (token, user_id, iota_id, request_id, expires_at) \
             VALUES (?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(), INTERVAL 10 MINUTE))",
        )
        .bind(&token)
        .bind(id.0)
        .bind(iota_id.0)
        .bind(request_id)
        .execute(&pool().await?)
        .await;
        match result {
            Ok(_) => return Ok((id, token)),
            Err(error) if is_duplicate_key(&error) => {
                let existing = sqlx::query(
                    "SELECT user_id, token, expires_at >= UTC_TIMESTAMP() AS current \
                     FROM registration_leases WHERE iota_id = ? AND request_id = ?",
                )
                .bind(iota_id.0)
                .bind(request_id)
                .fetch_optional(&pool().await?)
                .await?;
                if let Some(existing) = existing {
                    let current: i8 = existing.get("current");
                    if current != 0 {
                        return Ok((
                            UserId::from(existing.get::<i64, _>("user_id")),
                            existing.get::<String, _>("token"),
                        ));
                    }
                }
                continue;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(OmegaError::Validation(
        "could not allocate a unique registration ID".into(),
    ))
}

pub(crate) fn is_duplicate_key(error: &sqlx::Error) -> bool {
    error.as_database_error().is_some_and(|database| {
        // MySQL's generic database-error API exposes SQLSTATE (23000) as
        // `code()`.  The driver-specific duplicate-key number is retained in
        // the diagnostic message.
        database.code().as_deref() == Some("23000") && database.message().contains("1062")
    })
}

const USER_BY_USERNAME_QUERY: &str = "SELECT id, iota_id, username, display, status, presence_preference, about, avatar, sub_level, sub_end, public_key, token FROM users WHERE username = ?";
const USER_BY_ID_QUERY: &str = "SELECT id, iota_id, username, display, status, presence_preference, about, avatar, sub_level, sub_end, public_key, token FROM users WHERE id = ?";
const USER_COLUMNS: &str = "SELECT id, iota_id, username, display, status, presence_preference, about, avatar, sub_level, sub_end, public_key, token FROM users";

#[derive(FromRow)]
struct UserRow {
    id: i64,
    iota_id: i64,
    username: Vec<u8>,
    display: Option<Vec<u8>>,
    status: Option<Vec<u8>>,
    presence_preference: Vec<u8>,
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
            presence_preference: decode(row.presence_preference)?,
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
    get_users_by_iota_ids(&[id.0]).await
}

fn normalized_ids(ids: &[i64]) -> Vec<i64> {
    let mut ids = ids
        .iter()
        .copied()
        .filter(|id| valid_protocol_id(*id))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn append_in_clause(query: &mut QueryBuilder<'_, MySql>, ids: &[i64]) {
    query.push("(");
    for (index, id) in ids.iter().enumerate() {
        if index > 0 {
            query.push(", ");
        }
        query.push_bind(*id);
    }
    query.push(")");
}

async fn fetch_users(mut query: QueryBuilder<'_, MySql>) -> Result<Vec<User>> {
    query
        .build_query_as::<UserRow>()
        .fetch_all(&pool().await?)
        .await?
        .into_iter()
        .map(|row| row.try_into().map_err(OmegaError::from))
        .collect()
}

pub async fn get_users_by_ids(ids: &[i64]) -> Result<Vec<User>> {
    let ids = normalized_ids(ids);
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::<MySql>::new(USER_COLUMNS);
    query.push(" WHERE id IN ");
    append_in_clause(&mut query, &ids);
    fetch_users(query).await
}

pub async fn get_users_by_iota_ids(ids: &[i64]) -> Result<Vec<User>> {
    let ids = normalized_ids(ids);
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::<MySql>::new(USER_COLUMNS);
    query.push(" WHERE iota_id IN ");
    append_in_clause(&mut query, &ids);
    fetch_users(query).await
}

pub async fn get_users_by_ids_and_iota_ids(
    user_ids: &[i64],
    iota_ids: &[i64],
) -> Result<Vec<User>> {
    let user_ids = normalized_ids(user_ids);
    let iota_ids = normalized_ids(iota_ids);
    if user_ids.is_empty() && iota_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut query = QueryBuilder::<MySql>::new(USER_COLUMNS);
    query.push(" WHERE ");
    if !user_ids.is_empty() {
        query.push("id IN ");
        append_in_clause(&mut query, &user_ids);
    }
    if !iota_ids.is_empty() {
        if !user_ids.is_empty() {
            query.push(" OR ");
        }
        query.push("iota_id IN ");
        append_in_clause(&mut query, &iota_ids);
    }
    fetch_users(query).await
}

#[derive(FromRow)]
struct PresencePreferenceRow {
    id: i64,
    presence_preference: Vec<u8>,
}

pub async fn get_presence_preferences(ids: &[i64]) -> Result<HashMap<i64, UserStatus>> {
    let ids = normalized_ids(ids);
    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut query =
        QueryBuilder::<MySql>::new("SELECT id, presence_preference FROM users WHERE id IN ");
    append_in_clause(&mut query, &ids);
    let rows = query
        .build_query_as::<PresencePreferenceRow>()
        .fetch_all(&pool().await?)
        .await?;
    let mut preferences = HashMap::with_capacity(rows.len());
    for row in rows {
        let status = String::from_utf8(row.presence_preference)
            .ok()
            .and_then(|value| UserStatus::from_client_preference(&value));
        let status = match status {
            Some(status) => status,
            None => {
                crate::log_in!(
                    crate::util::logger::PrintType::General,
                    "Invalid persisted presence preference for user {}, using user_online",
                    row.id
                );
                UserStatus::user_online
            }
        };
        preferences.insert(row.id, status);
    }
    Ok(preferences)
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

pub async fn change_presence_preference(id: UserId, value: String) -> Result<()> {
    update(
        id,
        "UPDATE users SET presence_preference = ? WHERE id = ?",
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
    registration_token: String,
) -> Result<()> {
    if !valid_protocol_id(id.0) || !valid_protocol_id(iota_id.0) {
        return Err(OmegaError::Validation(
            "user or Iota ID is outside the 48-bit protocol range".into(),
        ));
    }
    if !valid_username(&username) {
        return Err(OmegaError::Validation("invalid username".into()));
    }

    let mut transaction = pool().await?.begin().await?;
    let lease = sqlx::query(
        "SELECT iota_id, completed_at IS NOT NULL AS completed, \
         expires_at >= UTC_TIMESTAMP() AS current FROM registration_leases \
         WHERE token = ? AND user_id = ? FOR UPDATE",
    )
    .bind(&registration_token)
    .bind(id.0)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| OmegaError::Validation("unknown registration lease".into()))?;
    let lease_iota_id: i64 = lease.get("iota_id");
    let completed: i8 = lease.get("completed");
    let current: i8 = lease.get("current");
    if lease_iota_id != iota_id.0 || (completed == 0 && current == 0) {
        return Err(OmegaError::Validation(
            "expired or mismatched registration lease".into(),
        ));
    }

    let insert_result = sqlx::query(
        "INSERT INTO users (id, username, public_key, iota_id, token) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(id.0)
    .bind(username.as_bytes())
    .bind(public_key.as_bytes())
    .bind(iota_id.0)
    .bind(token.as_bytes())
    .execute(&mut *transaction)
    .await;

    let result: Result<()> = match insert_result {
        Ok(_) => Ok(()),
        Err(insert_error) => {
            let existing = sqlx::query_as::<_, UserRow>(USER_BY_ID_QUERY)
                .bind(id.0)
                .fetch_optional(&mut *transaction)
                .await?;
            match existing
                .map(User::try_from)
                .transpose()
                .map_err(OmegaError::from)?
            {
                Some(existing)
                    if existing.iota_id == iota_id
                        && existing.username == username
                        && existing.public_key.as_bytes() == public_key.as_bytes()
                        && existing.token == token =>
                {
                    Ok(())
                }
                _ => Err(insert_error.into()),
            }
        }
    };
    result?;
    sqlx::query("UPDATE registration_leases SET completed_at = UTC_TIMESTAMP() WHERE token = ?")
        .bind(&registration_token)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}

fn valid_username(username: &str) -> bool {
    !username.is_empty()
        && username.chars().count() <= 15
        && !username.chars().any(char::is_control)
        && !username.contains(['/', '\\'])
}

#[cfg(test)]
mod tests {
    use super::{MAX_PROTOCOL_ID, get_register_id, valid_protocol_id};

    #[tokio::test]
    async fn generated_registration_ids_fit_the_mtp_wire_range() {
        for _ in 0..128 {
            assert!(valid_protocol_id(get_register_id().await.unwrap().0));
        }
        assert!(!valid_protocol_id(0));
        assert!(valid_protocol_id(MAX_PROTOCOL_ID));
        assert!(!valid_protocol_id(MAX_PROTOCOL_ID + 1));
    }
}
