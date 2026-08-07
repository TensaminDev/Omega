use std::collections::BTreeSet;

const OMIKRON_PREFIX: &str = "omikron;caps=";
const OMEGA_PREFIX: &str = "omega;caps=";
const SET_USER_STATE: &str = "set_user_state_v1";
const STATE_SUBSCRIBE: &str = "state_subscribe_v1";
const SESSION_SNAPSHOT: &str = "session_snapshot_v1";
const CLIENT_STATE_PUSH: &str = "client_state_push_v1";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerCapabilities {
    pub set_user_state_v1: bool,
    pub state_subscribe_v1: bool,
    pub session_snapshot_v1: bool,
    pub client_state_push_v1: bool,
}

impl PeerCapabilities {
    /// A missing descriptor is the legacy protocol: tuple route snapshots,
    /// GetStates-only subscription refreshes, and ClientChanged pushes.
    pub fn from_identification_description(description: Option<&str>) -> Result<Self, ()> {
        parse_capabilities(description, OMIKRON_PREFIX)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OmegaCapabilities {
    pub set_user_state_v1: bool,
    pub state_subscribe_v1: bool,
    pub session_snapshot_v1: bool,
    pub client_state_push_v1: bool,
}

impl OmegaCapabilities {
    pub fn current() -> Self {
        Self {
            set_user_state_v1: true,
            state_subscribe_v1: true,
            session_snapshot_v1: true,
            client_state_push_v1: true,
        }
    }

    pub fn identification_description(&self) -> String {
        let mut names = Vec::new();
        if self.set_user_state_v1 {
            names.push(SET_USER_STATE);
        }
        if self.state_subscribe_v1 {
            names.push(STATE_SUBSCRIBE);
        }
        if self.session_snapshot_v1 {
            names.push(SESSION_SNAPSHOT);
        }
        if self.client_state_push_v1 {
            names.push(CLIENT_STATE_PUSH);
        }
        format!("{OMEGA_PREFIX}{}", names.join(","))
    }
}

fn parse_capabilities(description: Option<&str>, prefix: &str) -> Result<PeerCapabilities, ()> {
    let Some(description) = description else {
        return Ok(PeerCapabilities::default());
    };
    if description == "omikron" {
        return Ok(PeerCapabilities::default());
    }
    let Some(capabilities) = description.strip_prefix(prefix) else {
        return Err(());
    };
    let mut seen = BTreeSet::new();
    for capability in capabilities.split(',') {
        if capability.is_empty() || !seen.insert(capability) {
            return Err(());
        }
    }
    if seen.iter().any(|capability| {
        !matches!(
            *capability,
            SET_USER_STATE | STATE_SUBSCRIBE | SESSION_SNAPSHOT | CLIENT_STATE_PUSH
        )
    }) {
        return Err(());
    }
    Ok(PeerCapabilities {
        set_user_state_v1: seen.contains(SET_USER_STATE),
        state_subscribe_v1: seen.contains(STATE_SUBSCRIBE),
        session_snapshot_v1: seen.contains(SESSION_SNAPSHOT),
        client_state_push_v1: seen.contains(CLIENT_STATE_PUSH),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_capabilities_are_parsed() {
        let capabilities = PeerCapabilities::from_identification_description(Some(
            "omikron;caps=set_user_state_v1,state_subscribe_v1,session_snapshot_v1,client_state_push_v1",
        )).unwrap();
        assert!(capabilities.set_user_state_v1);
        assert!(capabilities.state_subscribe_v1);
        assert!(capabilities.session_snapshot_v1);
        assert!(capabilities.client_state_push_v1);
    }

    #[test]
    fn invalid_capability_values_fail_identification() {
        assert!(
            PeerCapabilities::from_identification_description(Some("omikron;caps=unsupported"))
                .is_err()
        );
    }

    #[test]
    fn legacy_peer_has_no_version_specific_features() {
        let capabilities = PeerCapabilities::from_identification_description(None).unwrap();
        assert!(!capabilities.set_user_state_v1);
        assert!(!capabilities.state_subscribe_v1);
        assert!(!capabilities.session_snapshot_v1);
        assert!(!capabilities.client_state_push_v1);
        assert_eq!(
            PeerCapabilities::from_identification_description(Some("omikron")),
            Ok(PeerCapabilities::default())
        );
    }

    #[test]
    fn reconnecting_with_the_same_identification_is_stable() {
        let description = Some(
            "omikron;caps=set_user_state_v1,state_subscribe_v1,session_snapshot_v1,client_state_push_v1",
        );
        assert_eq!(
            PeerCapabilities::from_identification_description(description),
            PeerCapabilities::from_identification_description(description)
        );
    }

    #[test]
    fn omega_capability_description_is_distinct_from_omikron_capabilities() {
        let description = OmegaCapabilities::current().identification_description();
        assert!(description.starts_with(OMEGA_PREFIX));
        assert!(PeerCapabilities::from_identification_description(Some(&description)).is_err());
    }
}
