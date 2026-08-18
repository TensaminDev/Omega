use super::super::connection::{
    MtpValueCompat, OmikronConnection, OmikronResult, OptionalDataValueCompat,
};
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
    let iota_id = value
        .get_data(DataType::IotaId)
        .as_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| user_repo::valid_protocol_id(*id));
    let Some(iota_id) = iota_id else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };
    let (register_id, registration_token) =
        user_repo::allocate_registration(IotaId::from(iota_id), value.get_id()).await?;
    let response = CommunicationValue::new(CommunicationType::GetRegister)
        .with_id(value.get_id())
        .add_typed_default(
            DataType::UserId,
            DataValue::SignedNumber(register_id.0.into()),
        )
        .add_typed_default(DataType::RegisterId, DataValue::Str(registration_token));
    connection.send(&response).await
}

pub async fn complete_iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let public_key = value
        .get_data(DataType::PublicKey)
        .as_str()
        .and_then(|key| PublicKeyBundle::from_base64(key).ok());
    let Some(public_key) = public_key else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };
    match iota_repo::create_new_iota(public_key).await {
        Ok(id) => {
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::CompleteRegisterIota)
                        .with_id(value.get_id())
                        .add_typed_default(DataType::IotaId, DataValue::SignedNumber(id.0.into())),
                )
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

pub async fn complete_user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let user_id = value
        .get_data(DataType::UserId)
        .as_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| user_repo::valid_protocol_id(*id));
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
    let registration_token = value
        .get_data(DataType::RegisterId)
        .as_str()
        .filter(|token| uuid::Uuid::parse_str(token).is_ok())
        .map(str::to_owned);
    let Some((user_id, username, public_key, reset_token, registration_token)) = user_id
        .zip(username)
        .zip(public_key)
        .zip(reset_token)
        .zip(registration_token)
        .map(|((((id, name), key), token), registration_token)| {
            (id, name, key, token, registration_token)
        })
    else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };
    // Omikron supplies the authenticated Iota ID in the payload.  The lease
    // check below binds completion to that Iota rather than trusting sender.
    let iota_id = value
        .get_data(DataType::IotaId)
        .as_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| user_repo::valid_protocol_id(*id));
    let Some(iota_id) = iota_id else {
        return connection
            .send_error_response(value.get_id(), CommunicationType::ErrorInvalidData)
            .await;
    };

    match user_repo::register_complete_user(
        UserId::from(user_id),
        username,
        public_key,
        IotaId::from(iota_id),
        reset_token,
        registration_token,
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
