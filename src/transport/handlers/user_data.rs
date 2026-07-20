use super::super::omikron_connection::{OmikronConnection, OmikronResult};
use crate::{
    db::{iota_repo, user_repo},
    models::{IotaId, UserId},
    sql::{connection_status::UserStatus, user_online_tracker},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use mtp::{
    codec::{CommunicationType, CommunicationValue, DataType, DataValue},
    crypto::PublicKeyBundle,
};
use std::sync::Arc;

fn connections(iota_id: i64) -> DataValue {
    DataValue::Array(
        user_online_tracker::get_iota_omikron_connections(iota_id)
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
    let user = if let Some(id) = value.get_data(DataType::UserId).as_number() {
        user_repo::get_by_user_id(UserId::from(id as i64))
            .await
            .ok()
    } else if let Some(name) = value.get_data(DataType::Username).as_str() {
        user_repo::get_by_username(name).await.ok()
    } else {
        None
    };
    let Some(user) = user else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorNotFound)
            .await;
    };
    let id = user.id.0;
    let iota_id = user.iota_id.0;
    let username = user.username.clone();
    let display = user
        .display
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| username.clone());
    let mut response = CommunicationValue::new(CommunicationType::GetUserData)
        .with_id(value.get_id())
        .add_typed_default(DataType::Username, DataValue::Str(username))
        .add_typed_default(
            DataType::PublicKey,
            DataValue::Str(user.public_key.to_base64()),
        )
        .add_typed_default(DataType::UserId, DataValue::SignedNumber(id.into()))
        .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()))
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
    let online = user_online_tracker::get_user_status(id);
    response = response
        .add_typed_default(
            DataType::OnlineStatus,
            DataValue::Str(
                online
                    .as_ref()
                    .map(|status| {
                        if status.connection_type == UserStatus::user_invisible {
                            UserStatus::user_offline.to_string()
                        } else {
                            status.connection_type.to_string()
                        }
                    })
                    .unwrap_or_else(|| UserStatus::iota_offline.to_string()),
            ),
        )
        .add_typed_default(DataType::OmikronConnections, connections(iota_id));
    if let Some(status) = online {
        response = response.add_typed_default(
            DataType::OmikronId,
            DataValue::SignedNumber(status.omikron_id.into()),
        );
    }
    connection.send(&response).await
}

pub async fn get_iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let found = if let Some(id) = value.get_data(DataType::IotaId).as_number() {
        iota_repo::get_iota_by_id(IotaId::from(id as i64))
            .await
            .ok()
            .map(|iota| (iota.id.0, iota.public_key, None, None))
    } else if let Some(id) = value.get_data(DataType::UserId).as_number() {
        if let Ok(user) = user_repo::get_by_user_id(UserId::from(id as i64)).await {
            iota_repo::get_iota_by_id(user.iota_id)
                .await
                .ok()
                .map(|iota| (iota.id.0, iota.public_key, Some(user.id.0), None))
        } else {
            None
        }
    } else if let Some(name) = value.get_data(DataType::Username).as_str() {
        if let Ok(user) = user_repo::get_by_username(name).await {
            iota_repo::get_iota_by_id(user.iota_id)
                .await
                .ok()
                .map(|iota| {
                    (
                        iota.id.0,
                        iota.public_key,
                        Some(user.id.0),
                        Some(name.to_owned()),
                    )
                })
        } else {
            None
        }
    } else {
        None
    };
    let Some((id, key, user_id, username)) = found else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorNotFound)
            .await;
    };
    let mut response = CommunicationValue::new(CommunicationType::GetIotaData)
        .with_id(value.get_id())
        .add_typed_default(DataType::PublicKey, DataValue::Str(key.to_base64()))
        .add_typed_default(DataType::IotaId, DataValue::SignedNumber(id.into()))
        .add_typed_default(DataType::OmikronConnections, connections(id));
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
    let id = UserId::from(value.get_sender() as i64);
    let mut error = None;
    if let Some(name) = value.get_data(DataType::Username).as_str() {
        error = user_repo::change_username(id, name.to_owned())
            .await
            .err()
            .map(|e| e.to_string());
    }
    if error.is_none() {
        if let Some(name) = value.get_data(DataType::Display).as_str() {
            error = user_repo::change_display_name(id, name.to_owned())
                .await
                .err()
                .map(|e| e.to_string());
        }
    }
    if error.is_none() {
        if let Some(avatar) = value.get_data(DataType::Avatar).as_str() {
            error = user_repo::change_avatar(id, avatar.to_owned())
                .await
                .err()
                .map(|e| e.to_string());
        }
    }
    if error.is_none() {
        if let Some(about) = value.get_data(DataType::About).as_str() {
            error = user_repo::change_about(id, about.to_owned())
                .await
                .err()
                .map(|e| e.to_string());
        }
    }
    if error.is_none() {
        if let Some(status) = value.get_data(DataType::Status).as_str() {
            error = user_repo::change_status(id, status.to_owned())
                .await
                .err()
                .map(|e| e.to_string());
        }
    }
    if error.is_none() {
        if let Some(key) = value
            .get_data(DataType::PublicKey)
            .as_str()
            .and_then(|key| PublicKeyBundle::from_base64(key).ok())
        {
            error = user_repo::change_keys(id, key)
                .await
                .err()
                .map(|e| e.to_string());
        }
    }
    let response = match error {
        None => CommunicationValue::new(CommunicationType::Success),
        Some(error) => CommunicationValue::new(CommunicationType::ErrorInternal)
            .add_typed_default(DataType::ErrorType, DataValue::Str(error)),
    };
    connection.send(&response.with_id(value.get_id())).await
}

pub async fn change_user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    update_user(connection, value).await
}

pub async fn change_iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let Some(reset) = value.get_data(DataType::ResetToken).as_str() else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };
    let Some(new_token) = value.get_data(DataType::NewToken).as_str() else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };
    let user_id = UserId::from(value.get_sender() as i64);
    let user = match user_repo::get_by_user_id(user_id).await {
        Ok(user) => user,
        Err(_) => {
            return connection
                .send_error_response(value.get_id(), CommunicationType::ErrorNotFound)
                .await;
        }
    };
    if user.token != reset {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidChallenge)
            .await;
    }
    let result =
        match user_repo::change_iota_id(user_id, IotaId::from(value.get_sender() as i64)).await {
            Ok(()) => user_repo::change_token(user_id, new_token.to_owned()).await,
            Err(error) => Err(error),
        };
    let response = match result {
        Ok(()) => CommunicationValue::new(CommunicationType::Success),
        Err(error) => CommunicationValue::new(CommunicationType::ErrorInternal)
            .add_typed_default(DataType::ErrorType, DataValue::Str(error.to_string())),
    };
    connection.send(&response.with_id(value.get_id())).await
}
