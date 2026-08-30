use super::{IotaId, UserId};
use mtp::crypto::PublicKeyBundle;

#[derive(Clone, Debug, serde::Serialize)]
pub struct User {
    pub id: UserId,
    pub iota_id: Option<IotaId>,
    pub username: String,
    pub display: Option<String>,
    pub status: Option<String>,
    pub presence_preference: String,
    pub about: Option<String>,
    pub avatar: Option<Vec<u8>>,
    pub sub_level: i32,
    pub sub_end: i64,
    #[serde(serialize_with = "crate::models::serialize_public_key")]
    pub public_key: PublicKeyBundle,
    #[serde(skip_serializing)]
    pub token: String,
    pub created_at: i64,
}
