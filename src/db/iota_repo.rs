use crate::{
    db::pool,
    error::{OmegaError, Result},
    models::{Iota, IotaId},
};
use mtp::crypto::PublicKeyBundle;
use sqlx::Row;

pub async fn get_iota_by_id(id: IotaId) -> Result<Iota> {
    let row = sqlx::query("SELECT id, public_key FROM iotas WHERE id = ?")
        .bind(id.0)
        .fetch_optional(&pool().await?)
        .await?
        .ok_or(OmegaError::NotFound)?;
    let key = PublicKeyBundle::from_bytes(&row.get::<Vec<u8>, _>("public_key"))
        .map_err(|error| OmegaError::Validation(error.to_string()))?;
    Ok(Iota {
        id: row.get::<i64, _>("id").into(),
        public_key: key,
    })
}

pub async fn create_new_iota(public_key: PublicKeyBundle) -> Result<IotaId> {
    for _ in 0..16 {
        let id = crate::db::user_repo::generate_protocol_id();
        let iota_id = IotaId::from(id.0);
        match register_complete_iota(iota_id, public_key.clone()).await {
            Ok(()) => return Ok(iota_id),
            Err(OmegaError::Database(error)) => {
                if crate::db::user_repo::is_duplicate_key(&error) {
                    continue;
                }
                return Err(OmegaError::Database(error));
            }
            Err(error) => return Err(error),
        }
    }
    Err(OmegaError::Validation(
        "could not allocate a unique Iota ID".into(),
    ))
}

pub async fn register_complete_iota(id: IotaId, public_key: PublicKeyBundle) -> Result<()> {
    if !crate::db::user_repo::valid_protocol_id(id.0) {
        return Err(OmegaError::Validation(
            "Iota ID is outside the 48-bit protocol range".into(),
        ));
    }
    sqlx::query("INSERT INTO iotas (id, public_key) VALUES (?, ?)")
        .bind(id.0)
        .bind(public_key.try_as_bytes()?)
        .execute(&pool().await?)
        .await?;
    Ok(())
}

pub async fn delete_iota(id: IotaId) -> Result<()> {
    sqlx::query("DELETE FROM iotas WHERE id = ?")
        .bind(id.0)
        .execute(&pool().await?)
        .await?;
    Ok(())
}
