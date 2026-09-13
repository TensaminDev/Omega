use super::omikron_manager;
use crate::{log_err, state::OmegaState, util::logger::PrintType};
use mtp::codec::{CommunicationType, CommunicationValue, RelayError, forward_relay_frame};
use std::{
    convert::TryFrom,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use thiserror::Error;

const TARGET_KIND_MASK: u64 = 0xC000_0000_0000_0000;
const TARGET_ID_MASK: u64 = (1_u64 << 48) - 1;
const USER_TARGET_KIND: u64 = 0x4000_0000_0000_0000;
const IOTA_TARGET_KIND: u64 = 0x8000_0000_0000_0000;
static NEXT_RELAY_FRAME_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteTarget {
    User(u64),
    Iota(u64),
}

impl RouteTarget {
    pub fn from_wire_id(value: u64) -> Option<Self> {
        let id = value & TARGET_ID_MASK;
        if id == 0 || value & !(TARGET_KIND_MASK | TARGET_ID_MASK) != 0 {
            return None;
        }
        match value & TARGET_KIND_MASK {
            USER_TARGET_KIND => Some(Self::User(id)),
            IOTA_TARGET_KIND => Some(Self::Iota(id)),
            _ => None,
        }
    }
}

#[derive(Debug, Error)]
pub enum RelayRouteError {
    #[error("relay has no destination Iota")]
    MissingDestinationIota,
    #[error("relay has an outer sender")]
    OuterSenderPresent,
    #[error("relay has invalid route target {0}")]
    InvalidDestinationTarget(u64),
    #[error("relay destination Iota is outside Omega's ID range")]
    DestinationIotaOutOfRange,
    #[error("destination Iota is offline")]
    IotaOffline,
    #[error("destination Omikron is offline")]
    OmikronOffline,
    #[error("relay route resolves back to source Omikron")]
    RouteLoop,
    #[error(transparent)]
    Relay(#[from] RelayError),
    #[error("sending relay to destination Omikron failed: {0}")]
    Send(String),
}

pub fn ensure_relay_frame_id(frame: CommunicationValue) -> CommunicationValue {
    if frame.id().is_some_and(|id| id != 0) {
        return frame;
    }
    let id = NEXT_RELAY_FRAME_ID.fetch_add(1, Ordering::Relaxed).max(1);
    frame.with_id(id)
}

pub fn error_response_type(error: &RelayRouteError) -> CommunicationType {
    match error {
        RelayRouteError::IotaOffline | RelayRouteError::OmikronOffline => {
            CommunicationType::ErrorNoIota
        }
        RelayRouteError::Send(_) => CommunicationType::ErrorInternal,
        RelayRouteError::MissingDestinationIota
        | RelayRouteError::OuterSenderPresent
        | RelayRouteError::InvalidDestinationTarget(_)
        | RelayRouteError::DestinationIotaOutOfRange
        | RelayRouteError::RouteLoop
        | RelayRouteError::Relay(_) => CommunicationType::ErrorInvalidData,
    }
}

pub async fn route_from_omikron(
    state: &OmegaState,
    source_omikron_id: i64,
    frame: CommunicationValue,
) -> Result<CommunicationValue, RelayRouteError> {
    let frame = ensure_relay_frame_id(frame);
    if !frame.is_type(CommunicationType::Relay) {
        return Err(RelayRouteError::Relay(RelayError::NotRelay));
    }
    if frame.sender().is_some() {
        return Err(RelayRouteError::OuterSenderPresent);
    }
    let Some(destination_wire_id) = frame.receiver() else {
        log_err!(
            source_omikron_id,
            PrintType::Omega,
            "Relay routing failed: missing destination Iota"
        );
        return Err(RelayRouteError::MissingDestinationIota);
    };
    let Some(RouteTarget::Iota(destination_iota)) = RouteTarget::from_wire_id(destination_wire_id)
    else {
        return Err(RelayRouteError::InvalidDestinationTarget(
            destination_wire_id,
        ));
    };
    let destination_iota_i64 =
        i64::try_from(destination_iota).map_err(|_| RelayRouteError::DestinationIotaOutOfRange)?;
    let frame = forward_relay_frame(&frame, destination_wire_id)?;

    let Some(destination_omikron) =
        omikron_manager::get_iota_primary_omikron_connection(state, destination_iota_i64)
    else {
        log_err!(
            source_omikron_id,
            PrintType::Omega,
            "Relay destination Iota {} is offline",
            destination_iota
        );
        return Err(RelayRouteError::IotaOffline);
    };
    if destination_omikron == source_omikron_id {
        log_err!(
            source_omikron_id,
            PrintType::Omega,
            "Relay route loop for destination Iota {} and Omikron {}",
            destination_iota,
            destination_omikron
        );
        return Err(RelayRouteError::RouteLoop);
    }
    let Some(connection) = state.connected_omikron(destination_omikron) else {
        log_err!(
            source_omikron_id,
            PrintType::Omega,
            "Relay destination Iota {} resolves to disconnected Omikron {}",
            destination_iota,
            destination_omikron
        );
        return Err(RelayRouteError::OmikronOffline);
    };
    let response = connection
        .await_response(&frame, Duration::from_secs(20))
        .await
        .map_err(|error| {
            log_err!(
                source_omikron_id,
                PrintType::Omega,
                "Relay send to destination Iota {} via Omikron {} failed: {}",
                destination_iota,
                destination_omikron,
                error
            );
            RelayRouteError::Send(error.to_string())
        })?;
    if response.is_type(CommunicationType::Success) {
        Ok(response)
    } else {
        Err(RelayRouteError::Send(format!(
            "destination Omikron rejected the Relay with {}",
            response.get_type()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mtp::codec::DataValue;

    fn wire(target: RouteTarget) -> u64 {
        let (kind, id) = match target {
            RouteTarget::User(id) => (USER_TARGET_KIND, id),
            RouteTarget::Iota(id) => (IOTA_TARGET_KIND, id),
        };
        kind | id
    }

    fn relay_frame() -> CommunicationValue {
        CommunicationValue::new(CommunicationType::Relay)
            .without_sender()
            .with_receiver(wire(RouteTarget::Iota(42)))
            .with_payload(DataValue::Bytes(vec![1, 2, 3, 4]))
    }

    #[test]
    fn forwarding_preserves_relay_payload_and_next_hop() {
        let frame = relay_frame().with_receiver(wire(RouteTarget::Iota(7)));
        let result = forward_relay_frame(&frame, wire(RouteTarget::Iota(42)));
        assert!(result.is_ok());
        let Ok(forwarded) = result else { return };
        assert_eq!(forwarded.receiver(), Some(wire(RouteTarget::Iota(42))));
        assert_eq!(forwarded.sender(), None);
        assert_eq!(forwarded.payload(), frame.payload());
        assert!(
            forwarded
                .id()
                .is_some_and(|id| id != frame.id().unwrap_or(0))
        );
    }

    #[tokio::test]
    async fn outer_sender_is_rejected_before_route_lookup() {
        let frame = relay_frame().with_sender(9);
        let result = route_from_omikron(&crate::state::OmegaState::test_state(), 1, frame).await;
        assert!(matches!(result, Err(RelayRouteError::OuterSenderPresent)));
    }

    #[tokio::test]
    async fn missing_destination_is_rejected_before_route_lookup() {
        let frame = relay_frame().without_receiver();
        let result = route_from_omikron(&crate::state::OmegaState::test_state(), 1, frame).await;
        assert!(matches!(
            result,
            Err(RelayRouteError::MissingDestinationIota)
        ));
    }

    #[tokio::test]
    async fn user_route_target_is_rejected_by_opaque_omega_router() {
        let frame = relay_frame().with_receiver(wire(RouteTarget::User(42)));
        let result = route_from_omikron(&crate::state::OmegaState::test_state(), 1, frame).await;
        assert!(matches!(
            result,
            Err(RelayRouteError::InvalidDestinationTarget(_))
        ));
    }

    #[tokio::test]
    async fn unavailable_destination_is_reported_as_iota_offline() {
        let result =
            route_from_omikron(&crate::state::OmegaState::test_state(), 1, relay_frame()).await;
        assert!(matches!(result, Err(RelayRouteError::IotaOffline)));
    }
}
