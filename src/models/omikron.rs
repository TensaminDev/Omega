use super::OmikronId;
use mtp::crypto::PublicKeyBundle;

#[derive(Clone, Debug, serde::Serialize)]
pub struct Omikron {
    pub id: OmikronId,
    #[serde(serialize_with = "crate::models::serialize_public_key")]
    pub public_key: PublicKeyBundle,
    pub location: String,
    pub ip_address: String,
    pub port: u16,
}
