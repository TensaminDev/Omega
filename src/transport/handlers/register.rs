use super::super::omikron_connection::{OmikronConnection, OmikronResult};
use crate::{
    db::{iota_repo, user_repo},
    models::{IotaId, UserId},
};
use mtp::{
    codec::{CommunicationType, CommunicationValue, DataType, DataValue},
    crypto::PublicKeyBundle,
};
use std::sync::Arc;

pub async fn get_register(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let register_id = user_repo::get_register_id().await?;
    let response = CommunicationValue::new(CommunicationType::GetRegister)
        .with_id(value.get_id())
        .add_typed_default(
            DataType::UserId,
            DataValue::SignedNumber(register_id.0.into()),
        );
    connection.send(&response).await
}

pub async fn complete_iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let iota_id = value
        .get_data(DataType::IotaId)
        .as_number()
        .map(|id| id as i64);
    let public_key = value
        .get_data(DataType::PublicKey)
        .as_str()
        .and_then(|key| PublicKeyBundle::from_base64(key).ok());
    let Some(public_key) = public_key else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };
    match iota_id {
        Some(iota_id) => {
            match iota_repo::register_complete_iota(IotaId::from(iota_id), public_key).await {
                Ok(()) => {
                    connection
                        .send(
                            &CommunicationValue::new(CommunicationType::Success)
                                .with_id(value.get_id()),
                        )
                        .await
                }
                Err(error) => {
                    connection
                        .send(
                            &CommunicationValue::new(CommunicationType::ErrorInternal)
                                .with_id(value.get_id())
                                .add_typed_default(
                                    DataType::ErrorType,
                                    DataValue::Str(error.to_string()),
                                ),
                        )
                        .await
                }
            }
        }
        None => match iota_repo::create_new_iota(public_key).await {
            Ok(id) => {
                connection
                    .send(
                        &CommunicationValue::new(CommunicationType::CompleteRegisterIota)
                            .with_id(value.get_id())
                            .add_typed_default(
                                DataType::IotaId,
                                DataValue::SignedNumber(id.0.into()),
                            ),
                    )
                    .await
            }
            Err(error) => {
                connection
                    .send(
                        &CommunicationValue::new(CommunicationType::ErrorInternal)
                            .with_id(value.get_id())
                            .add_typed_default(
                                DataType::ErrorType,
                                DataValue::Str(error.to_string()),
                            ),
                    )
                    .await
            }
        },
    }
}

pub async fn complete_user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let user_id = value
        .get_data(DataType::UserId)
        .as_number()
        .map(|id| id as i64);
    let username = value
        .get_data(DataType::Username)
        .as_str()
        .map(str::to_owned);
    let public_key = value
        .get_data(DataType::PublicKey)
        .as_str()
        .and_then(|key| PublicKeyBundle::from_base64(key).ok());
    let reset_token = value
        .get_data(DataType::ResetToken)
        .as_str()
        .map(str::to_owned);
    let Some((user_id, username, public_key, reset_token)) = user_id
        .zip(username)
        .zip(public_key)
        .zip(reset_token)
        .map(|(((id, name), key), token)| (id, name, key, token))
    else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };
    match user_repo::register_complete_user(
        UserId::from(user_id),
        username,
        public_key,
        IotaId::from(value.get_sender() as i64),
        reset_token,
    )
    .await
    {
        Ok(()) => {
            connection
                .send(&CommunicationValue::new(CommunicationType::Success).with_id(value.get_id()))
                .await
        }
        Err(error) => {
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::ErrorInternal)
                        .with_id(value.get_id())
                        .add_typed_default(DataType::ErrorType, DataValue::Str(error.to_string())),
                )
                .await
        }
    }
}
