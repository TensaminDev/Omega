use crate::{
    db::pool,
    error::Result,
    models::{Notification, UserId},
};
use sqlx::Row;

pub async fn add_notification(sender_id: UserId, receiver_id: UserId) -> Result<()> {
    sqlx::query("INSERT INTO notifications (sender_id, receiver_id, amount) VALUES (?, ?, 1) ON DUPLICATE KEY UPDATE amount = amount + 1").bind(sender_id.0).bind(receiver_id.0).execute(&pool().await?).await?;
    Ok(())
}

pub async fn read_notification(sender_id: UserId, receiver_id: UserId) -> Result<()> {
    sqlx::query("DELETE FROM notifications WHERE sender_id = ? AND receiver_id = ?")
        .bind(sender_id.0)
        .bind(receiver_id.0)
        .execute(&pool().await?)
        .await?;
    Ok(())
}

pub async fn get_notifications(receiver_id: UserId) -> Result<Vec<Notification>> {
    let rows = sqlx::query(
        "SELECT id, sender_id, receiver_id, amount FROM notifications WHERE receiver_id = ?",
    )
    .bind(receiver_id.0)
    .fetch_all(&pool().await?)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Notification {
            id: row.get("id"),
            sender_id: row.get::<i64, _>("sender_id").into(),
            receiver_id: row.get::<i64, _>("receiver_id").into(),
            amount: row.get("amount"),
        })
        .collect())
}
