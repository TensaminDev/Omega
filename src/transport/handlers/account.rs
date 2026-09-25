use super::super::connection::{
    OmikronConnection, OmikronResult, OptionalDataValueCompat, RequiredMtpFields,
};
use crate::log_event;
use crate::{
    db::{iota_repo, user_repo},
    models::{IotaId, UserId},
    state::AccountChallengeOperation,
    util::logger::{LogLevel, PrintType},
};
use mtp::{
    codec::{CommunicationType, CommunicationValue, DataType, DataValue},
    crypto::{verify_ed25519, verify_ml_dsa},
};
use std::sync::Arc;

pub async fn user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    value.require_sender_i64()?;
    let user_id = UserId::from(value.require_sender_i64()?);
    complete_delete(connection, value, user_id).await
}
pub async fn iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let iota_id = value.require_sender_i64()?;
    let omikron_ids = connection
        .state()
        .presence
        .iota_connections(iota_id)
        .unwrap_or_default();
    match iota_repo::delete_iota(IotaId::from(iota_id)).await {
        Ok(()) => {
            for omikron_id in &omikron_ids {
                connection
                    .state()
                    .presence
                    .untrack_iota_connection(iota_id, *omikron_id);
            }
            let response_result = connection
                .clone()
                .send(&CommunicationValue::new(CommunicationType::Success).with_id(request_id))
                .await;
            let invalidation = CommunicationValue::new(CommunicationType::DeleteIota)
                .without_id()
                .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()));
            for omikron_id in omikron_ids {
                if let Some(target) = connection.state().connected_omikron(omikron_id) {
                    if let Err(error) = target.send_message(&invalidation).await {
                        log_event!(
                            connection.id_i64(),
                            PrintType::Omega,
                            LogLevel::Warn,
                            "account.invalidation_delivery_failed",
                            "request_id={} iota_id={} target_omikron_id={} error={}",
                            request_id,
                            iota_id,
                            omikron_id,
                            error
                        );
                    }
                }
            }
            response_result
        }
        Err(crate::error::OmegaError::Validation(error)) => {
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::ErrorInvalidData)
                        .with_id(request_id)
                        .add_typed_default(DataType::ErrorType, DataValue::Str(error)),
                )
                .await
        }
        Err(crate::error::OmegaError::NotFound) => {
            connection
                .send_error_response(request_id, CommunicationType::ErrorNotFound)
                .await
        }
        Err(error) => {
            log_event!(
                connection.id_i64(),
                PrintType::Omega,
                LogLevel::Error,
                "account.delete_failed",
                "request_id={} iota_id={} error={}",
                request_id,
                iota_id,
                error
            );
            connection
                .send_error_response(request_id, CommunicationType::ErrorInternal)
                .await
        }
    }
}

pub async fn release_from_iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    value.require_sender_i64()?;
    let Some(user_id) = value
        .get_data(DataType::UserId)
        .as_signed_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| *id > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidUserId)
            .await;
    };
    let requester = IotaId::from(value.require_sender_i64()?);
    let Ok(user) = user_repo::get_by_user_id(UserId::from(user_id)).await else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorNotFound)
            .await;
    };
    if user.iota_id != Some(requester) {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorNotAuthenticated,
            )
            .await;
    }
    let previous_iota = user.iota_id;
    match user_repo::change_iota_id(user.id, None).await {
        Ok(()) => {
            log_event!(
                connection.id_i64(),
                PrintType::Omega,
                LogLevel::Info,
                "account.released",
                "request_id={} user_id={} iota_id={}",
                value.require_id()?,
                user_id,
                requester.0
            );
            if let Some(iota) = previous_iota {
                if let Err(error) = crate::transport::omikron_manager::publish_iota_user_snapshot(
                    &connection.state(),
                    iota.0,
                )
                .await
                {
                    log_event!(
                        connection.id_i64(),
                        PrintType::Omega,
                        LogLevel::Warn,
                        "account.snapshot_publish_failed",
                        "request_id={} user_id={} iota_id={} error={}",
                        value.require_id()?,
                        user_id,
                        iota.0,
                        error
                    );
                }
            }
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::Success)
                        .with_id(value.require_id()?),
                )
                .await
        }
        Err(error) => {
            log_event!(
                connection.id_i64(),
                PrintType::Omega,
                LogLevel::Error,
                "account.release_failed",
                "request_id={} user_id={} error={}",
                value.require_id()?,
                user_id,
                error
            );
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorInternal)
                .await
        }
    }
}

fn lifecycle_payload(domain: &[u8], user_id: i64, iota_id: i64, nonce: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(domain.len() + 32);
    payload.extend_from_slice(domain);
    payload.extend_from_slice(&user_id.to_be_bytes());
    payload.extend_from_slice(&iota_id.to_be_bytes());
    payload.extend_from_slice(&nonce.to_be_bytes());
    payload
}

pub async fn attach_begin(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    value.require_sender_i64()?;
    let Some(user_id) = value
        .get_data(DataType::UserId)
        .as_signed_number()
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidUserId)
            .await;
    };
    if user_repo::get_by_user_id(UserId::from(user_id))
        .await
        .is_err()
    {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorNotFound)
            .await;
    }
    let requester = value.require_sender_i64()?;
    let nonce =
        connection
            .state()
            .issue_challenge(AccountChallengeOperation::Attach, user_id, requester);
    connection
        .send(
            &CommunicationValue::new(CommunicationType::AttachUserChallenge)
                .with_id(value.require_id()?)
                .add_typed_default(DataType::UserId, DataValue::SignedNumber(user_id.into()))
                .add_typed_default(DataType::ServerNonce, DataValue::SignedNumber(nonce.into())),
        )
        .await
}

pub async fn attach_complete(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    value.require_sender_i64()?;
    let Some(user_id) = value
        .get_data(DataType::UserId)
        .as_signed_number()
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidUserId)
            .await;
    };
    let requester = value.require_sender_i64()?;
    let Some(nonce) = value
        .get_data(DataType::ServerNonce)
        .as_signed_number()
        .and_then(|v| u64::try_from(v).ok())
    else {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorInvalidChallenge,
            )
            .await;
    };
    let signature = value.get_data(DataType::Signature).as_bytes();
    let pq_signature = value.get_data(DataType::PqSignature).as_bytes();
    let (Some(signature), Some(pq_signature)) = (signature, pq_signature) else {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorInvalidChallenge,
            )
            .await;
    };
    if !connection.state().consume_challenge(
        AccountChallengeOperation::Attach,
        user_id,
        requester,
        nonce,
    ) {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorInvalidChallenge,
            )
            .await;
    }
    let Ok(user) = user_repo::get_by_user_id(UserId::from(user_id)).await else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorNotFound)
            .await;
    };
    let payload = lifecycle_payload(b"tensamin:user-attach:v1\0", user_id, requester, nonce);
    if verify_ed25519(&user.public_key.sig_cl_public_key, &payload, &signature).is_err()
        || verify_ml_dsa(&user.public_key.sig_pq_public_key, &payload, &pq_signature).is_err()
    {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorNotAuthenticated,
            )
            .await;
    }
    let previous_iota = user.iota_id;
    match user_repo::change_iota_id(user.id, Some(IotaId::from(requester))).await {
        Ok(()) => {
            log_event!(
                connection.id_i64(),
                PrintType::Omega,
                LogLevel::Info,
                "account.attached",
                "request_id={} user_id={} iota_id={}",
                value.require_id()?,
                user_id,
                requester
            );
            if let Some(iota) = previous_iota.filter(|id| id.0 != requester) {
                if let Err(error) = crate::transport::omikron_manager::publish_iota_user_snapshot(
                    &connection.state(),
                    iota.0,
                )
                .await
                {
                    log_event!(
                        connection.id_i64(),
                        PrintType::Omega,
                        LogLevel::Warn,
                        "account.snapshot_publish_failed",
                        "request_id={} user_id={} iota_id={} error={}",
                        value.require_id()?,
                        user_id,
                        iota.0,
                        error
                    );
                }
            }
            if let Err(error) = crate::transport::omikron_manager::publish_iota_user_snapshot(
                &connection.state(),
                requester,
            )
            .await
            {
                log_event!(
                    connection.id_i64(),
                    PrintType::Omega,
                    LogLevel::Warn,
                    "account.snapshot_publish_failed",
                    "request_id={} user_id={} iota_id={} error={}",
                    value.require_id()?,
                    user_id,
                    requester,
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
            log_event!(
                connection.id_i64(),
                PrintType::Omega,
                LogLevel::Error,
                "account.attach_failed",
                "request_id={} user_id={} iota_id={} error={}",
                value.require_id()?,
                user_id,
                requester,
                error
            );
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorInternal)
                .await
        }
    }
}

async fn complete_delete(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
    user_id: UserId,
) -> OmikronResult<()> {
    match user_repo::delete_user_with_pending_erasure(user_id).await {
        Ok(iota_id) => {
            log_event!(
                connection.id_i64(),
                PrintType::Omega,
                LogLevel::Info,
                "account.deleted",
                "request_id={} user_id={} iota_id={:?}",
                value.require_id()?,
                user_id.0,
                iota_id.map(|id| id.0)
            );
            let cleanup_pending = iota_id.is_some();
            let target_omikrons = connection
                .state()
                .presence
                .routes_for_user(user_id.0)
                .into_iter()
                .map(|route| route.omikron_id)
                .collect::<std::collections::HashSet<_>>();
            connection.state().presence.remove_user_sessions(user_id.0);
            if let Some(iota_id) = iota_id {
                if let Err(error) = crate::transport::omikron_manager::publish_iota_user_snapshot(
                    &connection.state(),
                    iota_id.0,
                )
                .await
                {
                    log_event!(
                        connection.id_i64(),
                        PrintType::Omega,
                        LogLevel::Warn,
                        "account.snapshot_publish_failed",
                        "request_id={} user_id={} iota_id={} error={}",
                        value.require_id()?,
                        user_id.0,
                        iota_id.0,
                        error
                    );
                }
                crate::transport::omikron_manager::deliver_pending_erasures(
                    &connection.state(),
                    iota_id.0,
                )
                .await;
            }
            let response_result = connection
                .clone()
                .send(
                    &CommunicationValue::new(CommunicationType::Success)
                        .with_id(value.require_id()?)
                        .add_typed_default(
                            DataType::CleanupPending,
                            DataValue::Bool(cleanup_pending),
                        ),
                )
                .await;
            let invalidation = CommunicationValue::new(CommunicationType::DeleteUser)
                .without_id()
                .add_typed_default(DataType::UserId, DataValue::SignedNumber(user_id.0.into()));
            for omikron_id in target_omikrons {
                if let Some(target) = connection.state().connected_omikron(omikron_id) {
                    if let Err(error) = target.send_message(&invalidation).await {
                        log_event!(
                            connection.id_i64(),
                            PrintType::Omega,
                            LogLevel::Warn,
                            "account.invalidation_delivery_failed",
                            "request_id={} user_id={} target_omikron_id={} error={}",
                            value.require_id()?,
                            user_id.0,
                            omikron_id,
                            error
                        );
                    }
                }
            }
            response_result
        }
        Err(crate::error::OmegaError::NotFound) => {
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorNotFound)
                .await
        }
        Err(error) => {
            log_event!(
                connection.id_i64(),
                PrintType::Omega,
                LogLevel::Error,
                "account.delete_failed",
                "request_id={} user_id={} error={}",
                value.require_id()?,
                user_id.0,
                error
            );
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorInternal)
                .await
        }
    }
}

pub async fn delete_credential_begin(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    value.require_sender_i64()?;
    let Some(user_id) = value
        .get_data(DataType::UserId)
        .as_signed_number()
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidUserId)
            .await;
    };
    if user_repo::get_by_user_id(UserId::from(user_id))
        .await
        .is_err()
    {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorNotFound)
            .await;
    }
    let requester = value.require_sender_i64()?;
    let nonce =
        connection
            .state()
            .issue_challenge(AccountChallengeOperation::Delete, user_id, requester);
    connection
        .send(
            &CommunicationValue::new(CommunicationType::DeleteUserCredentialChallenge)
                .with_id(value.require_id()?)
                .add_typed_default(DataType::UserId, DataValue::SignedNumber(user_id.into()))
                .add_typed_default(DataType::ServerNonce, DataValue::SignedNumber(nonce.into())),
        )
        .await
}

pub async fn delete_credential_complete(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    value.require_sender_i64()?;
    let Some(user_id) = value
        .get_data(DataType::UserId)
        .as_signed_number()
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidUserId)
            .await;
    };
    let requester = value.require_sender_i64()?;
    let Some(nonce) = value
        .get_data(DataType::ServerNonce)
        .as_signed_number()
        .and_then(|v| u64::try_from(v).ok())
    else {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorInvalidChallenge,
            )
            .await;
    };
    let (Some(signature), Some(pq_signature)) = (
        value.get_data(DataType::Signature).as_bytes(),
        value.get_data(DataType::PqSignature).as_bytes(),
    ) else {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorInvalidChallenge,
            )
            .await;
    };
    if !connection.state().consume_challenge(
        AccountChallengeOperation::Delete,
        user_id,
        requester,
        nonce,
    ) {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorInvalidChallenge,
            )
            .await;
    }
    let Ok(user) = user_repo::get_by_user_id(UserId::from(user_id)).await else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorNotFound)
            .await;
    };
    let payload = lifecycle_payload(b"tensamin:user-delete:v1\0", user_id, requester, nonce);
    if verify_ed25519(&user.public_key.sig_cl_public_key, &payload, &signature).is_err()
        || verify_ml_dsa(&user.public_key.sig_pq_public_key, &payload, &pq_signature).is_err()
    {
        return connection
            .send_error_response(
                value.require_id()?,
                CommunicationType::ErrorNotAuthenticated,
            )
            .await;
    }
    complete_delete(connection, value, user.id).await
}

pub async fn erase_hosted_user_data_ack(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    value.require_sender_i64()?;
    let Some(user_id) = value
        .get_data(DataType::UserId)
        .as_signed_number()
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidUserId)
            .await;
    };
    let iota_id = IotaId::from(value.require_sender_i64()?);
    match user_repo::acknowledge_pending_erasure(UserId::from(user_id), iota_id).await {
        Ok(true) => {
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::Success)
                        .with_id(value.require_id()?),
                )
                .await
        }
        Ok(false) => {
            connection
                .send_error_response(
                    value.require_id()?,
                    CommunicationType::ErrorNotAuthenticated,
                )
                .await
        }
        Err(_) => {
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorInternal)
                .await
        }
    }
}

pub async fn acknowledge_iota_user_provisioning(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    value.require_id()?;
    let Some(user_id) = value
        .get_data(DataType::UserId)
        .as_signed_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| *id > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidUserId)
            .await;
    };
    let iota_id = IotaId::from(value.require_sender_i64()?);
    let Some(invitation_id) = value
        .get_data(DataType::InvitationId)
        .as_signed_number()
        .and_then(|id| i64::try_from(id).ok())
        .filter(|id| *id > 0)
    else {
        return connection
            .send_error_response(value.require_id()?, CommunicationType::ErrorInvalidData)
            .await;
    };
    match crate::db::invitation_repo::acknowledge(invitation_id, UserId::from(user_id), iota_id)
        .await
    {
        Ok(true) => {
            connection
                .send(
                    &CommunicationValue::new(CommunicationType::Success)
                        .with_id(value.require_id()?),
                )
                .await
        }
        Ok(false) => {
            connection
                .send_error_response(
                    value.require_id()?,
                    CommunicationType::ErrorNotAuthenticated,
                )
                .await
        }
        Err(_) => {
            connection
                .send_error_response(value.require_id()?, CommunicationType::ErrorInternal)
                .await
        }
    }
}
