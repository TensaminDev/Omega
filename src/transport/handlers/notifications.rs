use super::super::connection::{
    MtpValueCompat, OmikronConnection, OmikronResult, OptionalDataValueCompat,
};
use crate::{db::notification_repo, log, models::UserId};
use mtp::codec::{CommunicationType, CommunicationValue, DataType, DataValue};
use mtp::type_map::TypeMap;
use std::sync::Arc;

pub async fn get(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let notifications =
        match notification_repo::get_notifications(UserId::from(value.get_sender() as i64)).await {
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
        .with_id(value.get_id())
        .add_typed_default(DataType::Notifications, DataValue::Array(notifications));
    connection.send(&response).await
}

pub async fn read(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let receiver = match value.get_sender() {
        sender if sender > 0 => sender as i64,
        _ => match value.get_data(DataType::ReceiverId).as_number() {
            Some(id) => id as i64,
            None => return Ok(()),
        },
    };
    let Some(other) = value
        .get_data(DataType::SenderId)
        .as_number()
        .map(|id| id as i64)
    else {
        return Ok(());
    };
    if let Err(error) =
        notification_repo::read_notification(UserId::from(receiver), UserId::from(other)).await
    {
        log!(
            crate::util::logger::PrintType::General,
            "SQL read_notification error: {}",
            error
        );
    } else {
        let response =
            CommunicationValue::new(CommunicationType::ReadNotification).with_id(value.get_id());
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
    let receiver = match value.get_receiver() {
        receiver if receiver > 0 => receiver as i64,
        _ => match value.get_data(DataType::ReceiverId).as_number() {
            Some(id) => id as i64,
            None => return Ok(()),
        },
    };
    let sender = value
        .get_data(DataType::SenderId)
        .as_number()
        .map(|id| id as i64)
        .unwrap_or(value.get_sender() as i64);
    if let Err(error) =
        notification_repo::add_notification(UserId::from(receiver), UserId::from(sender)).await
    {
        log!(
            crate::util::logger::PrintType::General,
            "SQL add_notification error: {}",
            error
        );
    } else {
        let response =
            CommunicationValue::new(CommunicationType::PushNotification).with_id(value.get_id());
        let _ = connection.send(&response).await;
        let push = CommunicationValue::new(CommunicationType::PushNotification)
            .with_receiver(receiver as u64)
            .add_typed_default(DataType::SenderId, DataValue::SignedNumber(sender.into()));
        crate::transport::omikron_manager::send_to_user(receiver, &push).await;
    }
    Ok(())
}
