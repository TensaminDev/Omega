use super::super::connection::{
    OmikronConnection, OmikronResult, OptionalDataValueCompat, RequiredMtpFields,
};
use crate::{db::notification_repo, log, models::UserId};
use mtp::codec::{CommunicationType, CommunicationValue, DataType, DataValue};
use mtp::type_map::TypeMap;
use std::sync::Arc;

pub async fn get(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let sender = value.require_sender_i64()?;
    let notifications = match notification_repo::get_notifications(UserId::from(sender)).await {
        Ok(items) => items
            .into_iter()
            .map(|item| {
                let tm = TypeMap::latest();
                let Some(sender) = DataType::SenderId.try_to_id(&tm) else {
                    return DataValue::Container(Vec::new());
                };
                let Some(amount) = DataType::Amount.try_to_id(&tm) else {
                    return DataValue::Container(Vec::new());
                };
                DataValue::Container(vec![
                    (sender, DataValue::SignedNumber(item.sender_id.0.into())),
                    (amount, DataValue::SignedNumber(item.amount.into())),
                ])
            })
            .collect(),
        Err(error) => {
            log!(
                crate::util::logger::PrintType::General,
                "SQL get_notifications error: {}",
                error
            );
            Vec::new()
        }
    };
    let response = CommunicationValue::new(CommunicationType::GetNotifications)
        .with_id(request_id)
        .add_typed_default(DataType::Notifications, DataValue::Array(notifications));
    connection.send(&response).await
}

pub async fn read(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let receiver = value.require_sender_i64()?;
    let Some(other) = value
        .get_data(DataType::SenderId)
        .as_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| *id > 0)
    else {
        return connection
            .send_error_response(request_id, CommunicationType::ErrorInvalidData)
            .await;
    };
    if let Err(error) =
        notification_repo::read_notification(UserId::from(other), UserId::from(receiver)).await
    {
        log!(
            crate::util::logger::PrintType::General,
            "SQL read_notification error: {}",
            error
        );
    } else {
        let response =
            CommunicationValue::new(CommunicationType::ReadNotification).with_id(request_id);
        let _ = connection.send(&response).await;
        let sync = CommunicationValue::new(CommunicationType::ReadNotification)
            .with_receiver(receiver as u64)
            .add_typed_default(DataType::SenderId, DataValue::SignedNumber(other.into()));
        crate::transport::omikron_manager::send_to_user(receiver, &sync).await;
    }
    Ok(())
}

pub async fn push(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.id().filter(|id| *id != 0);
    let receiver = value.require_receiver_i64().ok().or_else(|| {
        value
            .get_data(DataType::ReceiverId)
            .as_number()
            .and_then(|id| i64::try_from(id).ok())
            .filter(|id| *id > 0)
    });
    let Some(receiver) = receiver else {
        if let Some(request_id) = request_id {
            return connection
                .send_error_response(request_id, CommunicationType::ErrorInvalidData)
                .await;
        }
        return Ok(());
    };
    let Some(sender) = value
        .get_data(DataType::SenderId)
        .as_number()
        .and_then(|sender| i64::try_from(sender).ok())
        .filter(|sender| *sender > 0)
    else {
        if let Some(request_id) = request_id {
            return connection
                .send_error_response(request_id, CommunicationType::ErrorInvalidData)
                .await;
        }
        return Ok(());
    };
    let source_omikron = connection
        .clone()
        .get_omikron_id()
        .await
        .ok_or(crate::OmegaError::NotConnected)?;
    if !connection
        .state()
        .presence
        .routes_for_user(sender)
        .iter()
        .any(|route| route.omikron_id == source_omikron)
    {
        if let Some(request_id) = request_id {
            return connection
                .send_error_response(request_id, CommunicationType::ErrorNotAuthenticated)
                .await;
        }
        return Ok(());
    }
    if let Err(error) =
        notification_repo::add_notification(UserId::from(sender), UserId::from(receiver)).await
    {
        log!(
            crate::util::logger::PrintType::General,
            "SQL add_notification error: {}",
            error
        );
    } else {
        if let Some(request_id) = request_id {
            let response =
                CommunicationValue::new(CommunicationType::PushNotification).with_id(request_id);
            let _ = connection.send(&response).await;
        }
        let push = CommunicationValue::new(CommunicationType::PushNotification)
            .with_receiver(receiver as u64)
            .add_typed_default(DataType::SenderId, DataValue::SignedNumber(sender.into()));
        crate::transport::omikron_manager::send_to_user(receiver, &push).await;
    }
    Ok(())
}
