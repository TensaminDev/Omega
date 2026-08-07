use crate::sql::user_online_tracker::PresenceTracker;
use std::sync::Arc;

pub struct OmegaState {
    pub presence: Arc<PresenceTracker>,
}

impl Default for OmegaState {
    fn default() -> Self {
        Self {
            presence: Arc::new(PresenceTracker::default()),
        }
    }
}

impl OmegaState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[cfg(test)]
mod tests {
    use super::OmegaState;
    use crate::sql::connection_status::UserStatus;

    #[test]
    fn state_instances_have_independent_presence_trackers() {
        let first = OmegaState::new();
        let second = OmegaState::new();

        first.presence.track_iota_connection(11, 42, true);

        assert!(first.presence.has_iota_route(11));
        assert!(!second.presence.has_iota_route(11));
        assert_eq!(first.presence.primary_iota_route(11), Some(42));
        assert_eq!(second.presence.primary_iota_route(11), None);
    }

    #[test]
    fn two_session_private_and_public_presence_flow_is_authoritative() {
        let state = OmegaState::new();
        state.presence.set_preference(7, UserStatus::user_online);
        state.presence.set_preference(8, UserStatus::user_online);
        state.presence.track_iota_connection(11, 42, true);
        state.presence.track_session(7, 100, 42, 11);
        state.presence.track_session(7, 101, 42, 11);
        state.presence.replace_subscription(7, 100, 42, vec![8]);

        assert_eq!(
            state.presence.resolve_public_state(8, 11),
            UserStatus::user_offline
        );
        state.presence.set_preference(8, UserStatus::user_invisible);
        assert_eq!(
            state.presence.resolve_public_state(8, 11),
            UserStatus::user_offline
        );
        assert_eq!(
            state.presence.resolve_private_state(8),
            UserStatus::user_invisible
        );

        state.presence.remove_session(7, 100, 42);
        assert!(state.presence.owns_session(7, 101, 42));
        state.presence.remove_session(7, 101, 42);
        assert_eq!(
            state.presence.resolve_public_state(7, 11),
            UserStatus::user_offline
        );
    }
}
