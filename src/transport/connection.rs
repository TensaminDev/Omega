pub(crate) use super::omikron_connection::{OmikronConnection, OmikronResult};

use mtp::codec::{CommunicationType, CommunicationValue, DataType, DataValue};

pub(crate) trait RequiredMtpFields {
    /// Require a nonzero request correlation ID. `None` and `Some(0)` are
    /// distinct MTP wire states, but both are invalid for Omega requests.
    fn require_id(&self) -> OmikronResult<u32>;
    /// Require a nonzero authenticated peer identity.
    fn require_sender(&self) -> OmikronResult<u64>;
    /// Require a nonzero application routing target.
    fn require_receiver(&self) -> OmikronResult<u64>;
    fn require_sender_i64(&self) -> OmikronResult<i64>;
    fn require_receiver_i64(&self) -> OmikronResult<i64>;
}

impl RequiredMtpFields for CommunicationValue {
    fn require_id(&self) -> OmikronResult<u32> {
        self.id().filter(|id| *id != 0).ok_or_else(|| {
            crate::OmegaError::Validation("MTP message is missing request id".into())
        })
    }

    fn require_sender(&self) -> OmikronResult<u64> {
        self.sender()
            .filter(|sender| *sender != 0)
            .ok_or_else(|| crate::OmegaError::Validation("MTP message is missing sender".into()))
    }

    fn require_receiver(&self) -> OmikronResult<u64> {
        self.receiver()
            .filter(|receiver| *receiver != 0)
            .ok_or_else(|| crate::OmegaError::Validation("MTP message is missing receiver".into()))
    }

    fn require_sender_i64(&self) -> OmikronResult<i64> {
        positive_i64(self.require_sender()?, "sender")
    }

    fn require_receiver_i64(&self) -> OmikronResult<i64> {
        positive_i64(self.require_receiver()?, "receiver")
    }
}

pub(crate) fn positive_i64(value: impl TryInto<i128>, field: &str) -> OmikronResult<i64> {
    let value = value
        .try_into()
        .map_err(|_| crate::OmegaError::Validation(format!("invalid {field}")))?;
    let value = i64::try_from(value)
        .map_err(|_| crate::OmegaError::Validation(format!("invalid {field}")))?;
    if value <= 0 {
        return Err(crate::OmegaError::Validation(format!("invalid {field}")));
    }
    Ok(value)
}

#[derive(Clone, Copy)]
enum SenderPolicy {
    None,
    Present,
    UserRoute,
    IotaRoute,
    IotaDataRoute,
}

fn operation_policy(message_type: CommunicationType) -> Option<(bool, SenderPolicy)> {
    use CommunicationType::*;
    use SenderPolicy as Sender;
    Some(match message_type {
        PushNotification => (false, Sender::None),
        ChangeUserData | DeleteUser | GetNotifications | ReadNotification | ClientChanged
        | StateSubscribe => (true, Sender::UserRoute),
        DeleteIota
        | AttachUserBegin
        | AttachUserComplete
        | DeleteUserCredentialBegin
        | DeleteUserCredentialComplete
        | ReleaseUserFromIota
        | CreateUserInvitation
        | ListUserInvitations
        | RevokeUserInvitation
        | EraseHostedUserDataAck
        | AcknowledgeIotaUserProvision => (true, Sender::IotaRoute),
        GetRegister | CompleteRegisterUser => (true, Sender::IotaDataRoute),
        GetUserData => (true, Sender::Present),
        ShortenLink
        | GetIotaData
        | GetStates
        | CompleteRegisterIota
        | RedeemUserInvitation
        | GetUserInvitationStatus
        | UserConnected
        | UserDisconnected
        | IotaConnected
        | IotaDisconnected
        | SyncClientIotaStatus => (true, Sender::None),
        _ => return None,
    })
}

pub(crate) fn validate_dispatch_fields(value: &CommunicationValue) -> OmikronResult<()> {
    let message_type = value
        .get_comm_type_enum()
        .ok_or_else(|| crate::OmegaError::Validation("unknown communication type".into()))?;
    let (requires_id, sender_policy) = operation_policy(message_type).ok_or_else(|| {
        crate::OmegaError::Validation(format!("no authorization policy for {message_type:?}"))
    })?;
    if requires_id {
        value.require_id()?;
    }
    if matches!(
        sender_policy,
        SenderPolicy::Present | SenderPolicy::UserRoute | SenderPolicy::IotaRoute
    ) {
        value.require_sender()?;
    }
    Ok(())
}

pub(crate) fn require_user_sender(
    connection: &OmikronConnection,
    value: &CommunicationValue,
) -> OmikronResult<i64> {
    require_user_sender_route(&connection.state().presence, connection.id_i64(), value)
}

fn require_user_sender_route(
    presence: &crate::sql::user_online_tracker::PresenceTracker,
    omikron_id: i64,
    value: &CommunicationValue,
) -> OmikronResult<i64> {
    let user_id = value.require_sender_i64()?;
    if !presence.owns_user_route(user_id, omikron_id) {
        return Err(crate::OmegaError::NotAuthenticated);
    }
    Ok(user_id)
}

pub(crate) fn require_iota_sender(
    connection: &OmikronConnection,
    value: &CommunicationValue,
) -> OmikronResult<i64> {
    require_iota_sender_route(&connection.state().presence, connection.id_i64(), value)
}

pub(crate) fn require_iota_data_route(
    connection: &OmikronConnection,
    iota_id: i64,
) -> OmikronResult<()> {
    require_iota_route(&connection.state().presence, connection.id_i64(), iota_id)
}

fn require_iota_route(
    presence: &crate::sql::user_online_tracker::PresenceTracker,
    omikron_id: i64,
    iota_id: i64,
) -> OmikronResult<()> {
    if !presence.owns_iota_route(iota_id, omikron_id) {
        return Err(crate::OmegaError::NotAuthenticated);
    }
    Ok(())
}

fn require_iota_sender_route(
    presence: &crate::sql::user_online_tracker::PresenceTracker,
    omikron_id: i64,
    value: &CommunicationValue,
) -> OmikronResult<i64> {
    let iota_id = value.require_sender_i64()?;
    if !presence.owns_iota_route(iota_id, omikron_id) {
        return Err(crate::OmegaError::NotAuthenticated);
    }
    Ok(iota_id)
}

pub(crate) fn validate_dispatch_sender(
    connection: &OmikronConnection,
    value: &CommunicationValue,
) -> OmikronResult<()> {
    let message_type = value
        .get_comm_type_enum()
        .ok_or_else(|| crate::OmegaError::Validation("unknown communication type".into()))?;
    let (_, sender_policy) = operation_policy(message_type).ok_or_else(|| {
        crate::OmegaError::Validation(format!("no authorization policy for {message_type:?}"))
    })?;
    match sender_policy {
        SenderPolicy::None => {}
        SenderPolicy::Present => {
            value.require_sender()?;
        }
        SenderPolicy::UserRoute => {
            require_user_sender(connection, value)?;
        }
        SenderPolicy::IotaRoute => {
            require_iota_sender(connection, value)?;
        }
        SenderPolicy::IotaDataRoute => {
            if let Some(raw) = value.get_data(DataType::IotaId).as_number() {
                let iota_id = positive_i64(raw, "iota_id")?;
                require_iota_data_route(connection, iota_id)?;
            }
        }
    }
    Ok(())
}

pub(crate) trait OptionalDataValueCompat {
    fn as_number(&self) -> Option<i128>;
    fn as_signed_number(&self) -> Option<i128>;
    fn as_str(&self) -> Option<&str>;
    fn as_bytes(&self) -> Option<Vec<u8>>;
}

impl OptionalDataValueCompat for Option<&DataValue> {
    fn as_number(&self) -> Option<i128> {
        self.and_then(|value| value.as_number())
    }

    fn as_signed_number(&self) -> Option<i128> {
        self.and_then(|value| value.as_signed_number())
    }

    fn as_str(&self) -> Option<&str> {
        self.and_then(|value| value.as_str())
    }

    fn as_bytes(&self) -> Option<Vec<u8>> {
        self.and_then(|value| value.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RequiredMtpFields, require_iota_route, require_iota_sender_route,
        require_user_sender_route, validate_dispatch_fields,
    };
    use crate::sql::user_online_tracker::PresenceTracker;
    use mtp::codec::{CommunicationType, CommunicationValue};

    #[test]
    fn every_dispatched_operation_has_a_policy() {
        let source = include_str!("omikron_connection.rs");
        let dispatch = source
            .split("async fn dispatch(")
            .nth(1)
            .unwrap()
            .split("pub(crate) async fn send(")
            .next()
            .unwrap();
        for branch in dispatch.split("Some(CommunicationType::").skip(1) {
            let name = branch.split(')').next().unwrap();
            let message_type = mtp::codec::CommunicationType::from_name(name).unwrap();
            assert!(
                super::operation_policy(message_type).is_some(),
                "missing policy for {name}"
            );
        }
    }

    #[test]
    fn missing_request_id_is_rejected_without_a_zero_fallback() {
        let value = CommunicationValue::new(CommunicationType::GetUserData).without_id();
        assert!(value.require_id().is_err());
        assert!(validate_dispatch_fields(&value).is_err());
    }

    #[test]
    fn zero_routing_values_are_rejected_by_omega_contract() {
        let value = CommunicationValue::new(CommunicationType::GetUserData)
            .with_id(0)
            .with_sender(0)
            .with_receiver(0);

        assert!(value.require_id().is_err());
        assert!(value.require_sender().is_err());
        assert!(value.require_receiver().is_err());
        assert!(validate_dispatch_fields(&value).is_err());
    }

    #[test]
    fn security_sensitive_messages_require_sender() {
        for message_type in [
            CommunicationType::GetUserData,
            CommunicationType::ChangeUserData,
            CommunicationType::DeleteUser,
            CommunicationType::AttachUserBegin,
            CommunicationType::AttachUserComplete,
            CommunicationType::DeleteUserCredentialBegin,
            CommunicationType::DeleteUserCredentialComplete,
            CommunicationType::EraseHostedUserDataAck,
            CommunicationType::AcknowledgeIotaUserProvision,
            CommunicationType::ReleaseUserFromIota,
            CommunicationType::DeleteIota,
            CommunicationType::CreateUserInvitation,
            CommunicationType::ListUserInvitations,
            CommunicationType::RevokeUserInvitation,
            CommunicationType::GetNotifications,
            CommunicationType::ReadNotification,
            CommunicationType::ClientChanged,
            CommunicationType::StateSubscribe,
        ] {
            let value = CommunicationValue::new(message_type).with_id(1);
            assert!(value.require_sender().is_err());
            assert!(validate_dispatch_fields(&value).is_err());
        }
    }

    #[test]
    fn client_changed_requires_request_identity() {
        let value = CommunicationValue::new(CommunicationType::ClientChanged).without_id();
        assert!(validate_dispatch_fields(&value).is_err());
    }

    #[test]
    fn push_notification_uses_its_logical_sender_field() {
        let value = CommunicationValue::new(CommunicationType::PushNotification);
        assert!(validate_dispatch_fields(&value).is_ok());
    }

    #[test]
    fn mutating_sender_cannot_use_another_omikrons_route() {
        let presence = PresenceTracker::default();
        presence.connect_iota(11, 42);
        presence.track_session(7, 3, 42, 11);
        let user_mutation = CommunicationValue::new(CommunicationType::ChangeUserData)
            .with_id(1)
            .with_sender(7);
        let iota_mutation = CommunicationValue::new(CommunicationType::DeleteIota)
            .with_id(2)
            .with_sender(11);

        assert!(require_user_sender_route(&presence, 42, &user_mutation).is_ok());
        assert!(require_user_sender_route(&presence, 43, &user_mutation).is_err());
        assert!(require_iota_sender_route(&presence, 42, &iota_mutation).is_ok());
        assert!(require_iota_sender_route(&presence, 43, &iota_mutation).is_err());
    }

    #[test]
    fn typed_registration_iota_must_belong_to_source_omikron() {
        let presence = PresenceTracker::default();
        presence.connect_iota(11, 42);

        assert!(require_iota_route(&presence, 42, 11).is_ok());
        assert!(require_iota_route(&presence, 43, 11).is_err());
    }
}
