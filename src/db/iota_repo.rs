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
    let id = crate::db::user_repo::get_register_id().await?;
    let iota_id = IotaId::from(id.0);
    register_complete_iota(iota_id, public_key).await?;
    Ok(iota_id)
}

pub async fn register_complete_iota(id: IotaId, public_key: PublicKeyBundle) -> Result<()> {
    sqlx::query("INSERT INTO iotas (id, public_key) VALUES (?, ?)")
        .bind(id.0)
        .bind(public_key.as_bytes())
        .execute(&pool().await?)
        .await?;
    Ok(())
}

pub async fn change_iota_key(id: IotaId, key: PublicKeyBundle) -> Result<()> {
    sqlx::query("UPDATE iotas SET public_key = ? WHERE id = ?")
        .bind(key.as_bytes())
        .bind(id.0)
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
