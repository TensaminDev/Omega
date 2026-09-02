pub(crate) use super::omikron_connection::{OmikronConnection, OmikronResult};

use mtp::codec::{CommunicationValue, DataValue};

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

pub(crate) fn validate_dispatch_fields(value: &CommunicationValue) -> OmikronResult<()> {
    let Some(message_type) = value.get_comm_type_enum() else {
        return Err(crate::OmegaError::Validation(
            "MTP message has an unknown communication type".into(),
        ));
    };

    if !matches!(
        message_type,
        mtp::codec::CommunicationType::ClientChanged
            | mtp::codec::CommunicationType::PushNotification
    ) {
        value.require_id()?;
    }

    if matches!(
        message_type,
        mtp::codec::CommunicationType::GetUserData
            | mtp::codec::CommunicationType::ChangeUserData
            | mtp::codec::CommunicationType::DeleteUser
            | mtp::codec::CommunicationType::AttachUserBegin
            | mtp::codec::CommunicationType::AttachUserComplete
            | mtp::codec::CommunicationType::DeleteUserCredentialBegin
            | mtp::codec::CommunicationType::DeleteUserCredentialComplete
            | mtp::codec::CommunicationType::EraseHostedUserDataAck
            | mtp::codec::CommunicationType::AcknowledgeIotaUserProvision
            | mtp::codec::CommunicationType::ReleaseUserFromIota
            | mtp::codec::CommunicationType::DeleteIota
            | mtp::codec::CommunicationType::GetNotifications
            | mtp::codec::CommunicationType::ReadNotification
            | mtp::codec::CommunicationType::StateSubscribe
    ) {
        value.require_sender()?;
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
    use super::{RequiredMtpFields, validate_dispatch_fields};
    use mtp::codec::{CommunicationType, CommunicationValue};

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
            CommunicationType::ReleaseUserFromIota,
            CommunicationType::DeleteIota,
            CommunicationType::GetNotifications,
            CommunicationType::ReadNotification,
            CommunicationType::StateSubscribe,
        ] {
            let value = CommunicationValue::new(message_type).with_id(1);
            assert!(value.require_sender().is_err());
            assert!(validate_dispatch_fields(&value).is_err());
        }
    }

    #[test]
    fn optional_client_changed_id_remains_optional() {
        let value = CommunicationValue::new(CommunicationType::ClientChanged).without_id();
        assert!(validate_dispatch_fields(&value).is_ok());
    }

    #[test]
    fn push_notification_uses_its_logical_sender_field() {
        let value = CommunicationValue::new(CommunicationType::PushNotification);
        assert!(validate_dispatch_fields(&value).is_ok());
    }
}
