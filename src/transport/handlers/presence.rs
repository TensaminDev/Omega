use super::super::omikron_connection::{OmikronConnection, OmikronResult};
use crate::{
    db::user_repo,
    log_in,
    models::IotaId,
    sql::{connection_status::UserStatus, user_online_tracker},
};
use mtp::codec::{CommunicationType, CommunicationValue, DataType, DataValue};
use std::sync::Arc;

pub async fn user_connected(
    _connection: Arc<OmikronConnection>,
    value: CommunicationValue,
    omikron_id: i64,
) -> OmikronResult<()> {
    log_in!(crate::util::logger::PrintType::Omega, "User connected");
    if let Some(user_id) = value.get_data(DataType::UserId).as_number() {
        let status = value
            .get_data(DataType::UserState)
            .as_str()
            .and_then(UserStatus::from_str)
            .unwrap_or(UserStatus::user_online);
        if let Ok(user_id) = i64::try_from(user_id) {
            user_online_tracker::track_user_status(user_id, status, omikron_id);
        }
    }
    Ok(())
}

pub async fn user_disconnected(
    _: Arc<OmikronConnection>,
    value: CommunicationValue,
    omikron_id: i64,
) -> OmikronResult<()> {
    log_in!(crate::util::logger::PrintType::Omega, "User disconnected");
    if let Some(user_id) = value.get_data(DataType::UserId).as_number() {
        user_online_tracker::untrack_user_status(user_id as i64, omikron_id);
    }
    Ok(())
}

pub async fn iota_connected(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
    omikron_id: i64,
) -> OmikronResult<()> {
    log_in!(crate::util::logger::PrintType::Omega, "IOTA connected");
    let Some(iota_id) = value
        .get_data(DataType::IotaId)
        .as_number()
        .map(|id| id as i64)
    else {
        return Ok(());
    };
    user_online_tracker::track_iota_connection(iota_id, omikron_id, true);
    let mut user_ids = Vec::new();
    match user_repo::get_users_by_iota_id(IotaId::from(iota_id)).await {
        Ok(users) => {
            for user in users {
                user_ids.push(DataValue::SignedNumber(user.id.0.into()));
                user_online_tracker::track_user_status(
                    user.id.0,
                    UserStatus::user_offline,
                    omikron_id,
                );
            }
        }
        Err(_) => log_in!(
            crate::util::logger::PrintType::General,
            "SQL error loading users for IOTA"
        ),
    }
    let response = CommunicationValue::new(CommunicationType::IotaUserData)
        .with_id(value.get_id())
        .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()))
        .add_typed_default(DataType::UserIds, DataValue::Array(user_ids));
    let _ = connection.send(&response).await;
    Ok(())
}

pub async fn iota_disconnected(
    _: Arc<OmikronConnection>,
    value: CommunicationValue,
    omikron_id: i64,
) -> OmikronResult<()> {
    log_in!(crate::util::logger::PrintType::Omega, "IOTA disconnected");
    let Some(iota_id) = value
        .get_data(DataType::IotaId)
        .as_number()
        .map(|id| id as i64)
    else {
        return Ok(());
    };
    if user_online_tracker::untrack_iota_connection(iota_id, omikron_id) {
        if let Ok(users) = user_repo::get_users_by_iota_id(IotaId::from(iota_id)).await {
            user_online_tracker::untrack_many_users(
                &users.iter().map(|user| user.id.0).collect::<Vec<_>>(),
            );
        }
    }
    Ok(())
}

pub async fn sync_status(
    _: Arc<OmikronConnection>,
    value: CommunicationValue,
    omikron_id: i64,
) -> OmikronResult<()> {
    if let DataValue::Array(ids) = value.get_data(DataType::UserIds) {
        for id in ids {
            if let DataValue::SignedNumber(id) = id {
                user_online_tracker::track_user_status(
                    *id as i64,
                    UserStatus::user_offline,
                    omikron_id,
                );
            }
        }
    }
    if let DataValue::Array(ids) = value.get_data(DataType::IotaIds) {
        for id in ids {
            if let DataValue::SignedNumber(id) = id {
                user_online_tracker::track_iota_connection(*id as i64, omikron_id, true);
            }
        }
    }
    Ok(())
}
