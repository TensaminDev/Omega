use super::super::connection::{
    OmikronConnection, OmikronResult, OptionalDataValueCompat, RequiredMtpFields,
    require_iota_data_route,
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
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidData)
            .await;
    };
    require_iota_data_route(&connection, iota_id)?;
    let request_token = match value.get_data(DataType::RegisterId).as_str() {
        Some(token) => match uuid::Uuid::parse_str(token) {
            Ok(token) => Some(token),
            Err(_) => {
                return connection
                    .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidData)
                    .await;
            }
        },
        None => None,
    };
    let (register_id, registration_token) =
        user_repo::allocate_registration(IotaId::from(iota_id), request_token, value.require_id()?)
            .await?;
    let response = CommunicationValue::new(CommunicationType::GetRegister)
        .with_id(value.require_id()?)
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
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidData)
            .await;
    };
    match iota_repo::get_or_create_iota(public_key).await {
        Ok(id) => {
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::CompleteRegisterIota)
                        .with_id(value.require_id()?)
                        .add_typed_default(DataType::IotaId, DataValue::SignedNumber(id.0.into())),
                )
                .await
        }
        Err(error) => {
            crate::log_event!(
                connection.id_i64(),
                crate::util::logger::PrintType::Omega,
                crate::util::logger::LogLevel::Error,
                "registration.iota_failed",
                "request_id={} error={}",
                value.require_id()?,
                error
            );
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorInternal)
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
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidData)
            .await;
    };
    let iota_id = value
        .get_data(DataType::IotaId)
        .as_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| user_repo::valid_protocol_id(*id));
    let Some(iota_id) = iota_id else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidData)
            .await;
    };
    require_iota_data_route(&connection, iota_id)?;

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
            if let Err(error) = crate::transport::omikron_manager::publish_iota_user_snapshot(
                &connection.state(),
                iota_id,
            )
            .await
            {
                crate::log_in!(
                    crate::util::logger::PrintType::General,
                    "Could not publish Iota snapshot after registering user {} on Iota {}: {}",
                    user_id,
                    iota_id,
                    error
                );
            }
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::Success)
                        .with_id(value.require_id()?),
                )
                .await
        }
        Err(error) => {
            crate::log_event!(
                connection.id_i64(),
                crate::util::logger::PrintType::Omega,
                crate::util::logger::LogLevel::Error,
                "registration.user_failed",
                "request_id={} error={}",
                value.require_id()?,
                error
            );
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorInternal)
                .await
        }
    }
}
