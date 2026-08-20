mod ids;
mod iota;
mod notification;
mod omikron;
mod user;

fn serialize_public_key<S>(
    key: &mtp::crypto::PublicKeyBundle,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let encoded = key.try_to_base64().map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&encoded)
}

pub use ids::{IotaId, OmikronId, UserId};
pub use iota::Iota;
pub use notification::Notification;
pub use omikron::Omikron;
pub use user::User;
