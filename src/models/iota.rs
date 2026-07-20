use super::IotaId;
use mtp::crypto::PublicKeyBundle;

#[derive(Clone, Debug, serde::Serialize)]
pub struct Iota {
    pub id: IotaId,
    #[serde(serialize_with = "crate::models::serialize_public_key")]
    pub public_key: PublicKeyBundle,
}
