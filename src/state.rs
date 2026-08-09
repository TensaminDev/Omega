use crate::sql::user_online_tracker::PresenceTracker;
use std::sync::Arc;
use dashmap::DashMap;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AccountChallengeOperation { Attach, Delete }

#[derive(Clone, Debug)]
pub struct AccountChallenge {
    pub operation: AccountChallengeOperation,
    pub user_id: i64,
    pub requester_iota_id: i64,
    pub nonce: u64,
    pub created_at: Instant,
}

pub struct OmegaState {
    pub presence: Arc<PresenceTracker>,
    challenges: DashMap<(AccountChallengeOperation, i64, i64), AccountChallenge>,
}

impl Default for OmegaState {
    fn default() -> Self {
        Self {
            presence: Arc::new(PresenceTracker::default()),
            challenges: DashMap::new(),
        }
    }
}

impl OmegaState {
    pub fn issue_challenge(&self, operation: AccountChallengeOperation, user_id: i64, requester_iota_id: i64) -> u64 {
        let nonce = rand::random::<u64>();
        self.challenges.insert((operation, user_id, requester_iota_id), AccountChallenge { operation, user_id, requester_iota_id, nonce, created_at: Instant::now() });
        nonce
    }

    pub fn consume_challenge(&self, operation: AccountChallengeOperation, user_id: i64, requester_iota_id: i64, nonce: u64) -> bool {
        self.challenges.remove(&(operation, user_id, requester_iota_id)).is_some_and(|(_, value)|
            value.nonce == nonce && value.created_at.elapsed() <= Duration::from_secs(120))
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
