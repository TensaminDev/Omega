use super::super::connection::{
    OmikronConnection, OmikronResult, OptionalDataValueCompat, RequiredMtpFields,
};
use crate::{db::invitation_repo, error::OmegaError, models::IotaId};
use mtp::{
    codec::{CommunicationType, CommunicationValue, DataType, DataValue, TypeMap},
    crypto::PublicKeyBundle,
};
use std::sync::Arc;

pub async fn manage(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let iota_id = IotaId::from(value.require_sender_i64()?);
    let authority_value = value.get_data(DataType::InvitationAuthority);
    let authority = authority_value.as_str().unwrap_or("omega");
    if authority != "omega" {
        return send_error(
            connection,
            request_id,
            CommunicationType::ErrorInvalidData,
            "local Iota invitations are not implemented",
        )
        .await;
    }
    let result = match value.get_comm_type_enum() {
        Some(CommunicationType::CreateUserInvitation) => create(&value, iota_id).await,
        Some(CommunicationType::ListUserInvitations) => list(iota_id).await,
        Some(CommunicationType::RevokeUserInvitation) => {
            revoke(&value, iota_id, &connection.state()).await
        }
        _ => Err(OmegaError::Validation(
            "unknown invitation operation".into(),
        )),
    };
    match result {
        Ok(response) => connection.send(&response.with_id(request_id)).await,
        Err(error) => {
            let kind = match error {
                OmegaError::NotFound => CommunicationType::ErrorNotFound,
                OmegaError::AuthenticationFailed | OmegaError::NotAuthenticated => {
                    CommunicationType::ErrorNotAuthenticated
                }
                OmegaError::Validation(_) | OmegaError::Invitation(_) => {
                    CommunicationType::ErrorInvalidData
                }
                _ => CommunicationType::ErrorInternal,
            };
            send_error(connection, request_id, kind, invitation_error_type(&error)).await
        }
    }
}

pub async fn redeem(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let request_id = value.require_id()?;
    let invitation_id = signed_i64(&value, DataType::InvitationId);
    let iota_id = signed_i64(&value, DataType::IotaId).map(IotaId::from);
    let result = match (value.get_comm_type_enum(), invitation_id, iota_id) {
        (Some(CommunicationType::GetUserInvitationStatus), Some(invitation_id), Some(iota_id)) => {
            invitation_repo::state(invitation_id, iota_id).await
        }
        (Some(CommunicationType::RedeemUserInvitation), Some(invitation_id), Some(iota_id)) => {
            let credential_value = value.get_data(DataType::InvitationToken);
            let credential = credential_value.as_str();
            let username_value = value.get_data(DataType::Username);
            let username = username_value.as_str();
            let public_key = value
                .get_data(DataType::PublicKey)
                .as_str()
                .and_then(|encoded| PublicKeyBundle::from_base64(encoded).ok());
            let reset_token_value = value.get_data(DataType::ResetToken);
            let reset_token = reset_token_value.as_str().filter(|token| !token.is_empty());
            let Some((credential, username, public_key, reset_token)) = credential
                .zip(username)
                .zip(public_key)
                .zip(reset_token)
                .map(|(((credential, username), public_key), reset_token)| {
                    (credential, username, public_key, reset_token)
                })
            else {
                return send_error(
                    connection,
                    request_id,
                    CommunicationType::ErrorInvalidData,
                    "missing invitation credential, username, public key, or reset token",
                )
                .await;
            };
            invitation_repo::redeem(
                invitation_id,
                iota_id,
                credential,
                value.get_data(DataType::InvitationPassword).as_str(),
                username,
                public_key,
                reset_token,
            )
            .await
        }
        _ => Err(OmegaError::Validation(
            "invalid invitation redemption operation".into(),
        )),
    };
    match result {
        Ok(result) => {
            if result.state == invitation_repo::InvitationState::Provisioning {
                crate::transport::omikron_manager::deliver_pending_user_provisioning(
                    &connection.state(),
                    result.iota_id.0,
                )
                .await;
            }
            connection
                .send(
                    &CommunicationValue::new(match value.get_comm_type_enum() {
                        Some(CommunicationType::GetUserInvitationStatus) => {
                            CommunicationType::GetUserInvitationStatus
                        }
                        _ => CommunicationType::RedeemUserInvitation,
                    })
                    .with_id(request_id)
                    .add_typed_default(
                        DataType::InvitationId,
                        DataValue::SignedNumber(result.invitation_id.into()),
                    )
                    .add_typed_default(
                        DataType::IotaId,
                        DataValue::SignedNumber(result.iota_id.0.into()),
                    )
                    .add_typed_default(
                        DataType::UserId,
                        DataValue::SignedNumber(result.user_id.0.into()),
                    )
                    .add_typed_default(
                        DataType::InvitationState,
                        DataValue::Str(result.state.as_str().into()),
                    )
                    .add_typed_default(
                        DataType::InvitationRevision,
                        DataValue::SignedNumber(result.revision.into()),
                    ),
                )
                .await
        }
        Err(error) => {
            let kind = match error {
                OmegaError::NotFound => CommunicationType::ErrorNotFound,
                OmegaError::AuthenticationFailed | OmegaError::NotAuthenticated => {
                    CommunicationType::ErrorNotAuthenticated
                }
                OmegaError::Validation(_) | OmegaError::Invitation(_) => {
                    CommunicationType::ErrorInvalidData
                }
                _ => CommunicationType::ErrorInternal,
            };
            send_error(connection, request_id, kind, invitation_error_type(&error)).await
        }
    }
}

async fn create(
    value: &CommunicationValue,
    iota_id: IotaId,
) -> crate::error::Result<CommunicationValue> {
    let lifetime_seconds =
        signed_i64(value, DataType::InvitationLifetimeSeconds).unwrap_or(7 * 24 * 60 * 60);
    let created = invitation_repo::create(
        iota_id,
        lifetime_seconds,
        value.get_data(DataType::InvitationPassword).as_str(),
        value.get_data(DataType::InvitationLabel).as_str(),
    )
    .await?;
    Ok(
        CommunicationValue::new(CommunicationType::CreateUserInvitation)
            .add_typed_default(
                DataType::InvitationId,
                DataValue::SignedNumber(created.invitation_id.into()),
            )
            .add_typed_default(DataType::InvitationToken, DataValue::Str(created.raw_token))
            .add_typed_default(
                DataType::InvitationExpiresAt,
                DataValue::SignedNumber(created.expires_at_ms.into()),
            )
            .add_typed_default(
                DataType::InvitationCreatedAt,
                DataValue::SignedNumber(created.created_at_ms.into()),
            )
            .add_typed_default(DataType::InvitationState, DataValue::Str("pending".into()))
            .add_typed_default(
                DataType::InvitationRevision,
                DataValue::SignedNumber(1.into()),
            )
            .add_typed_default(
                DataType::InvitationAuthority,
                DataValue::Str("omega".into()),
            )
            .add_typed_default(DataType::Link, DataValue::Str(created.short_url)),
    )
}

async fn list(iota_id: IotaId) -> crate::error::Result<CommunicationValue> {
    let invitations = invitation_repo::list(iota_id).await?;
    let type_map = TypeMap::latest();
    let encoded = invitations
        .into_iter()
        .map(|invitation| {
            let mut fields = Vec::new();
            let mut push = |kind: DataType, value: DataValue| {
                if let Some(id) = kind.try_to_id(&type_map) {
                    fields.push((id, value));
                }
            };
            push(
                DataType::InvitationAuthority,
                DataValue::Str("omega".into()),
            );
            push(
                DataType::InvitationId,
                DataValue::SignedNumber(invitation.invitation_id.into()),
            );
            push(
                DataType::IotaId,
                DataValue::SignedNumber(invitation.iota_id.into()),
            );
            push(
                DataType::InvitationCreatedAt,
                DataValue::SignedNumber(invitation.created_at_ms.into()),
            );
            push(
                DataType::InvitationExpiresAt,
                DataValue::SignedNumber(invitation.expires_at_ms.into()),
            );
            push(
                DataType::InvitationState,
                DataValue::Str(invitation.state.as_str().into()),
            );
            push(
                DataType::InvitationRevision,
                DataValue::SignedNumber(invitation.revision.into()),
            );
            push(
                DataType::InvitationPasswordProtected,
                DataValue::Bool(invitation.password_protected),
            );
            if let Some(label) = invitation.label {
                push(DataType::InvitationLabel, DataValue::Str(label));
            }
            if let Some(user_id) = invitation.redeemed_user_id {
                push(DataType::UserId, DataValue::SignedNumber(user_id.into()));
            }
            DataValue::Container(fields)
        })
        .collect();
    Ok(
        CommunicationValue::new(CommunicationType::ListUserInvitations)
            .add_typed_default(DataType::Invitations, DataValue::Array(encoded)),
    )
}

async fn revoke(
    value: &CommunicationValue,
    iota_id: IotaId,
    state: &crate::state::OmegaState,
) -> crate::error::Result<CommunicationValue> {
    let invitation_id = signed_i64(value, DataType::InvitationId)
        .ok_or_else(|| OmegaError::Validation("missing invitation ID".into()))?;
    let result = invitation_repo::revoke(invitation_id, iota_id).await?;
    crate::transport::omikron_manager::deliver_pending_erasures(state, iota_id.0).await;
    Ok(CommunicationValue::new(CommunicationType::Success)
        .add_typed_default(
            DataType::InvitationId,
            DataValue::SignedNumber(invitation_id.into()),
        )
        .add_typed_default(
            DataType::InvitationRevision,
            DataValue::SignedNumber(result.revision.into()),
        )
        .add_typed_default(
            DataType::InvitationState,
            DataValue::Str(result.state.as_str().into()),
        )
        .add_typed_default(DataType::Result, DataValue::Bool(result.applied)))
}

fn signed_i64(value: &CommunicationValue, data_type: DataType) -> Option<i64> {
    value
        .get_data(data_type)
        .as_signed_number()
        .and_then(|value| i64::try_from(value).ok())
}

async fn send_error(
    connection: Arc<OmikronConnection>,
    request_id: u32,
    kind: CommunicationType,
    message: &str,
) -> OmikronResult<()> {
    connection
        .send(
            &CommunicationValue::new(kind)
                .with_id(request_id)
                .add_typed_default(DataType::ErrorType, DataValue::Str(message.into())),
        )
        .await
}

fn invitation_error_type(error: &OmegaError) -> &str {
    match error {
        OmegaError::Invitation(kind) => kind,
        OmegaError::NotFound
        | OmegaError::AuthenticationFailed
        | OmegaError::NotAuthenticated
        | OmegaError::Validation(_) => "invitation_invalid",
        _ => "invitation_internal",
    }
}
