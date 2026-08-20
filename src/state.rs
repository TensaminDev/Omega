use crate::{
    config::OmegaConfig, identity::OmegaIdentity, sql::user_online_tracker::PresenceTracker,
};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AccountChallengeOperation {
    Attach,
    Delete,
}

#[derive(Clone, Debug)]
pub struct AccountChallenge {
    pub nonce: u64,
    pub created_at: Instant,
}

pub struct OmegaState {
    pub presence: Arc<PresenceTracker>,
    pub identity: Arc<OmegaIdentity>,
    pub(crate) config: Arc<OmegaConfig>,
    pub(crate) global_handler_limit: Arc<Semaphore>,
    pub(crate) omikron_handler_concurrency: usize,
    challenges: DashMap<(AccountChallengeOperation, i64, i64), AccountChallenge>,
}

impl OmegaState {
    fn with_handler_limits(identity: OmegaIdentity, config: OmegaConfig) -> Self {
        let omikron_handler_concurrency = config.dispatch.omikron_handler_concurrency;
        let global_handler_concurrency = config.dispatch.global_handler_concurrency;
        Self {
            presence: Arc::new(PresenceTracker::default()),
            identity: Arc::new(identity),
            config: Arc::new(config),
            global_handler_limit: Arc::new(Semaphore::new(global_handler_concurrency)),
            omikron_handler_concurrency,
            challenges: DashMap::new(),
        }
    }

    pub fn new(identity: OmegaIdentity, config: OmegaConfig) -> Arc<Self> {
        Arc::new(Self::with_handler_limits(identity, config))
    }

    #[cfg(test)]
    fn test_state() -> Arc<Self> {
        use mtp::crypto::Keyring;

        let mut config = OmegaConfig::default();
        config.dispatch.omikron_handler_concurrency = 4;
        config.dispatch.global_handler_concurrency = 8;
        Arc::new(Self::with_handler_limits(
            OmegaIdentity::from_keyring(Keyring::generate()),
            config,
        ))
    }

    pub fn issue_challenge(
        &self,
        operation: AccountChallengeOperation,
        user_id: i64,
        requester_iota_id: i64,
    ) -> u64 {
        let nonce = rand::random::<u64>();
        self.challenges.insert(
            (operation, user_id, requester_iota_id),
            AccountChallenge {
                nonce,
                created_at: Instant::now(),
            },
        );
        nonce
    }

    pub fn consume_challenge(
        &self,
        operation: AccountChallengeOperation,
        user_id: i64,
        requester_iota_id: i64,
        nonce: u64,
    ) -> bool {
        self.challenges
            .remove(&(operation, user_id, requester_iota_id))
            .is_some_and(|(_, value)| {
                value.nonce == nonce && value.created_at.elapsed() <= Duration::from_secs(120)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::OmegaState;
    use crate::sql::connection_status::UserStatus;

    #[test]
    fn state_instances_have_independent_presence_trackers() {
        let first = OmegaState::test_state();
        let second = OmegaState::test_state();

        first.presence.connect_iota(11, 42);

        assert!(first.presence.has_iota_route(11));
        assert!(!second.presence.has_iota_route(11));
        assert_eq!(first.presence.primary_iota_route(11), Some(42));
        assert_eq!(second.presence.primary_iota_route(11), None);
    }

    #[test]
    fn two_session_private_and_public_presence_flow_is_authoritative() {
        let state = OmegaState::test_state();
        state.presence.set_preference(7, UserStatus::user_online);
        state.presence.set_preference(8, UserStatus::user_online);
        state.presence.connect_iota(11, 42);
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
