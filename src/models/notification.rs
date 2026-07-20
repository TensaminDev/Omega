use super::UserId;

#[derive(Clone, Debug, serde::Serialize)]
pub struct Notification {
    pub id: i64,
    pub sender_id: UserId,
    pub receiver_id: UserId,
    pub amount: i64,
}
