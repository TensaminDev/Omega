use super::super::connection::{
    OmikronConnection, OmikronResult, OptionalDataValueCompat, RequiredMtpFields,
};
use crate::{
    db::{iota_repo, user_repo},
    models::{IotaId, UserId},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use mtp::{
    codec::{CommunicationType, CommunicationValue, DataType, DataValue},
    crypto::PublicKeyBundle,
};
use std::sync::Arc;

fn connections(connection: &OmikronConnection, iota_id: i64) -> DataValue {
    DataValue::Array(
        connection
            .state()
            .presence
            .iota_connections(iota_id)
            .unwrap_or_default()
            .into_iter()
            .map(|id| DataValue::SignedNumber(id.into()))
            .collect(),
    )
}

pub async fn get_user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let sender = value.require_sender_i64()?;
    let state = connection.state();
    let user = if let Some(id) = value
        .get_data(DataType::UserId)
        .as_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| *id > 0)
    {
        user_repo::get_by_user_id(UserId::from(id)).await.ok()
    } else if let Some(name) = value.get_data(DataType::Username).as_str() {
        user_repo::get_by_username(name).await.ok()
    } else {
        None
    };
    let Some(user) = user else {
        return connection
            .send_error_response(request_id, CommunicationType::ErrorNotFound)
            .await;
    };
    let id = user.id.0;
    let iota_id = user.iota_id.map(|id| id.0);
    let username = user.username.clone();
    let display = user
        .display
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| username.clone());
    let mut response = CommunicationValue::new(CommunicationType::GetUserData)
        .with_id(request_id)
        .add_typed_default(DataType::Username, DataValue::Str(username))
        .add_typed_default(
            DataType::PublicKey,
            DataValue::Str(user.public_key.try_to_base64()?),
        )
        .add_typed_default(DataType::UserId, DataValue::SignedNumber(id.into()))
        .add_typed_default(DataType::Display, DataValue::Str(display))
        .add_typed_default(
            DataType::SubLevel,
            DataValue::SignedNumber(user.sub_level as i128),
        )
        .add_typed_default(
            DataType::SubEnd,
            DataValue::SignedNumber(user.sub_end.into()),
        );
    if let Some(status) = user.status.filter(|value| !value.is_empty()) {
        response = response.add_typed_default(DataType::Status, DataValue::Str(status));
    }
    if let Some(about) = user.about.filter(|value| !value.is_empty()) {
        response = response.add_typed_default(DataType::About, DataValue::Str(about));
    }
    if let Some(avatar) = user.avatar {
        response =
            response.add_typed_default(DataType::Avatar, DataValue::Str(STANDARD.encode(avatar)));
    }
    let route = state.presence.user_route(id);
    let private_request = sender == id;
    let resolved_status = if private_request {
        if !state
            .presence
            .load_preference(id, &user.presence_preference)
        {
            crate::log_in!(
                crate::util::logger::PrintType::General,
                "Invalid persisted presence preference for user {}, using user_online",
                id
            );
        }
        state.presence.resolve_private_state(id)
    } else {
        iota_id
            .map(|iota_id| state.presence.resolve_public_state(id, iota_id))
            .unwrap_or(crate::sql::connection_status::UserStatus::user_offline)
    };
    response = response
        .add_typed_default(
            DataType::OnlineStatus,
            DataValue::Str(resolved_status.to_string()),
        )
        .add_typed_default(
            DataType::OmikronConnections,
            iota_id
                .map(|iota_id| connections(&connection, iota_id))
                .unwrap_or_else(|| DataValue::Array(Vec::new())),
        );
    if let Some(iota_id) = iota_id {
        response =
            response.add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()));
    }
    if let Some(route) = route {
        response = response.add_typed_default(
            DataType::OmikronId,
            DataValue::SignedNumber(route.omikron_id.into()),
        );
    }
    connection.send(&response).await
}

pub async fn get_iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let found = if let Some(id) = value
        .get_data(DataType::IotaId)
        .as_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| *id > 0)
    {
        iota_repo::get_iota_by_id(IotaId::from(id))
            .await
            .ok()
            .map(|iota| (iota.id.0, iota.public_key, None, None))
    } else if let Some(id) = value.get_data(DataType::UserId).as_number() {
        if let Some(id) = i64::try_from(id).ok().filter(|id| *id > 0)
            && let Ok(user) = user_repo::get_by_user_id(UserId::from(id)).await
        {
            match user.iota_id {
                Some(iota_id) => iota_repo::get_iota_by_id(iota_id)
                    .await
                    .ok()
                    .map(|iota| (iota.id.0, iota.public_key, Some(user.id.0), None)),
                None => None,
            }
        } else {
            None
        }
    } else if let Some(name) = value.get_data(DataType::Username).as_str() {
        if let Ok(user) = user_repo::get_by_username(name).await {
            match user.iota_id {
                Some(iota_id) => iota_repo::get_iota_by_id(iota_id).await.ok().map(|iota| {
                    (
                        iota.id.0,
                        iota.public_key,
                        Some(user.id.0),
                        Some(name.to_owned()),
                    )
                }),
                None => None,
            }
        } else {
            None
        }
    } else {
        None
    };
    let Some((id, key, user_id, username)) = found else {
        return connection
            .send_error_response(request_id, CommunicationType::ErrorNotFound)
            .await;
    };
    let mut response = CommunicationValue::new(CommunicationType::GetIotaData)
        .with_id(request_id)
        .add_typed_default(DataType::PublicKey, DataValue::Str(key.try_to_base64()?))
        .add_typed_default(DataType::IotaId, DataValue::SignedNumber(id.into()))
        .add_typed_default(DataType::OmikronConnections, connections(&connection, id));
    if let Some(user_id) = user_id {
        response =
            response.add_typed_default(DataType::UserId, DataValue::SignedNumber(user_id.into()));
    }
    if let Some(username) = username {
        response = response.add_typed_default(DataType::Username, DataValue::Str(username));
    }
    connection.send(&response).await
}

async fn update_user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let id = UserId::from(value.require_sender_i64()?);
    let mut error = None;
    if let Some(name) = value.get_data(DataType::Username).as_str() {
        error = user_repo::change_username(id, name.to_owned())
            .await
            .err()
            .map(|e| e.to_string());
    }
    if error.is_none()
        && let Some(name) = value.get_data(DataType::Display).as_str()
    {
        error = user_repo::change_display_name(id, name.to_owned())
            .await
            .err()
            .map(|e| e.to_string());
    }
    if error.is_none()
        && let Some(avatar) = value.get_data(DataType::Avatar).as_str()
    {
        error = user_repo::change_avatar(id, avatar.to_owned())
            .await
            .err()
            .map(|e| e.to_string());
    }
    if error.is_none()
        && let Some(about) = value.get_data(DataType::About).as_str()
    {
        error = user_repo::change_about(id, about.to_owned())
            .await
            .err()
            .map(|e| e.to_string());
    }
    if error.is_none()
        && let Some(status) = value.get_data(DataType::Status).as_str()
    {
        error = user_repo::change_status(id, status.to_owned())
            .await
            .err()
            .map(|e| e.to_string());
    }
    if error.is_none()
        && let Some(key) = value
            .get_data(DataType::PublicKey)
            .as_str()
            .and_then(|key| PublicKeyBundle::from_base64(key).ok())
    {
        error = user_repo::change_keys(id, key)
            .await
            .err()
            .map(|e| e.to_string());
    }
    let response = match error {
        None => CommunicationValue::new(CommunicationType::Success),
        Some(error) => CommunicationValue::new(CommunicationType::ErrorInternal)
            .add_typed_default(DataType::ErrorType, DataValue::Str(error)),
    };
    connection.send(&response.with_id(request_id)).await
}

pub async fn change_user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    update_user(connection, value).await
}
