use crate::{
    db::pool,
    error::{OmegaError, Result},
    models::{Omikron, OmikronId},
};
use mtp::crypto::PublicKeyBundle;
use sqlx::Row;

pub async fn get_omikron_by_id(id: OmikronId) -> Result<Omikron> {
    let row =
        sqlx::query("SELECT id, public_key, location, ip_address, port FROM omikrons WHERE id = ?")
            .bind(id.0)
            .fetch_optional(&pool().await?)
            .await?
            .ok_or(OmegaError::NotFound)?;
    let key = PublicKeyBundle::from_bytes(&row.get::<Vec<u8>, _>("public_key"))
        .map_err(|error| OmegaError::Validation(error.to_string()))?;
    let location = String::from_utf8(row.get("location"))
        .map_err(|error| OmegaError::Validation(format!("invalid location UTF-8: {error}")))?;
    let ip_address = String::from_utf8(row.get("ip_address"))
        .map_err(|error| OmegaError::Validation(format!("invalid IP address UTF-8: {error}")))?;
    Ok(Omikron {
        id: row.get::<i64, _>("id").into(),
        public_key: key,
        location,
        ip_address,
        port: row.get::<i32, _>("port") as u16,
    })
}
