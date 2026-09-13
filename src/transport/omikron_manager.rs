use crate::db::user_repo;
use crate::state::OmegaState;
use crate::transport::connection::OmikronConnection;
use crate::transport::omikron_connection::OmikronResult;
use mtp::codec::{CommunicationType, CommunicationValue, DataType, DataValue};
use rand::prelude::IteratorRandom;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::interval;

pub async fn add_omikron(conn: Arc<OmikronConnection>) {
    let id = match conn.clone().get_omikron_id().await {
        Some(id) => id,
        _ => {
            conn.close().await;
            return;
        }
    };

    let state = conn.state();
    if let Some(old) = state.register_omikron(id, &conn) {
        old.close().await;
    }
    let _ = flush_iota_snapshot_outbox(&state).await;
}

pub async fn remove_omikron(omikron_id: i64, connection: &Arc<OmikronConnection>) -> bool {
    connection.state().remove_omikron(omikron_id, connection)
}

pub fn get_iota_primary_omikron_connection(state: &OmegaState, iota_id: i64) -> Option<i64> {
    state.presence.primary_iota_route(iota_id)
}

pub async fn get_all_connections(
    state: &OmegaState,
) -> Result<std::collections::HashMap<i64, std::collections::HashMap<i64, Vec<i64>>>, ()> {
    let mut result = state.presence.connection_routes();
    let iota_ids = state
        .presence
        .all_iota_routes()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let users = user_repo::get_users_by_iota_ids(&iota_ids)
        .await
        .map_err(|_| ())?;
    for user in users {
        if let Some(iota_id) = user.iota_id {
            for omikron_id in state
                .presence
                .iota_connections(iota_id.0)
                .unwrap_or_default()
            {
                if let Some(users) = result
                    .get_mut(&omikron_id)
                    .and_then(|iotas| iotas.get_mut(&iota_id.0))
                {
                    users.push(user.id.0);
                }
            }
        }
    }
    for iotas in result.values_mut() {
        for users in iotas.values_mut() {
            users.sort_unstable();
            users.dedup();
        }
    }
    Ok(result)
}

pub async fn send_state_batch(
    state: &OmegaState,
    omikron_id: i64,
    notifications: Vec<CommunicationValue>,
) -> OmikronResult<()> {
    let connection = state
        .connected_omikron(omikron_id)
        .ok_or(crate::error::OmegaError::NotConnected)?;
    connection.send_messages(&notifications).await
}

pub async fn get_random_omikron(state: &OmegaState) -> Result<Arc<OmikronConnection>, ()> {
    let keys = state.connected_omikron_ids();

    if let Some(key) = keys.into_iter().choose(&mut rand::rng())
        && let Some(connection) = state.connected_omikron(key)
    {
        return Ok(connection);
    }

    Err(())
}

pub async fn send_to_user(state: &OmegaState, user_id: i64, cv: &CommunicationValue) {
    for user_route in state.presence.routes_for_user(user_id) {
        if let Some(connection) = state.connected_omikron(user_route.omikron_id) {
            let _ = connection.send_message(cv).await;
        }
    }
}

/*
 * Publish each Iota membership snapshot to every live relay route. Each
 * Omikron keeps a local authorization index, so sending only a primary route
 * leaves the remaining relays stale after registration or migration.
 */
pub async fn publish_iota_user_snapshot(state: &OmegaState, iota_id: i64) -> OmikronResult<()> {
    let omikron_ids = state
        .presence
        .iota_connections(iota_id)
        .ok_or(crate::error::OmegaError::NotConnected)?;
    let users = user_repo::get_users_by_iota_id(crate::models::IotaId::from(iota_id)).await?;
    let user_ids = users
        .into_iter()
        .map(|user| DataValue::SignedNumber(user.id.0.into()))
        .collect();
    let snapshot = CommunicationValue::new(CommunicationType::IotaUserData)
        .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()))
        .add_typed_default(DataType::UserIds, DataValue::Array(user_ids));
    let mut failures = Vec::new();
    for omikron_id in omikron_ids {
        match state.connected_omikron(omikron_id) {
            Some(connection) => {
                if let Err(error) = connection.send(&snapshot).await {
                    failures.push(format!("Omikron {omikron_id}: {error}"));
                }
            }
            None => failures.push(format!("Omikron {omikron_id}: connection disappeared")),
        }
    }
    if !failures.is_empty() {
        return Err(crate::error::OmegaError::Transport(format!(
            "Iota snapshot delivery failed: {}",
            failures.join(", ")
        )));
    }
    Ok(())
}

pub async fn flush_iota_snapshot_outbox(state: &OmegaState) -> OmikronResult<()> {
    for snapshot in user_repo::pending_iota_snapshots().await? {
        match publish_iota_user_snapshot(state, snapshot.iota_id.0).await {
            Ok(()) => {
                if let Err(error) = user_repo::complete_iota_snapshot(&snapshot).await {
                    crate::log_in!(
                        crate::util::logger::PrintType::General,
                        "Could not complete Iota snapshot outbox entry for {}: {}",
                        snapshot.iota_id.0,
                        error
                    );
                }
            }
            Err(error) => {
                crate::log_in!(
                    crate::util::logger::PrintType::General,
                    "Could not publish Iota snapshot for {}: {}",
                    snapshot.iota_id.0,
                    error
                );
            }
        }
    }
    Ok(())
}

pub fn spawn_iota_snapshot_outbox_worker(state: Arc<OmegaState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut retry = interval(Duration::from_secs(30));
        loop {
            retry.tick().await;
            if let Err(error) = flush_iota_snapshot_outbox(&state).await {
                crate::log_in!(
                    crate::util::logger::PrintType::General,
                    "Could not load Iota snapshot outbox: {}",
                    error
                );
            }
        }
    })
}

pub async fn deliver_pending_erasures(state: &OmegaState, iota_id: i64) {
    let Ok(users) =
        user_repo::pending_erasures_for_iota(crate::models::IotaId::from(iota_id)).await
    else {
        return;
    };
    let Some(omikron_id) = get_iota_primary_omikron_connection(state, iota_id) else {
        return;
    };
    let Some(connection) = state.connected_omikron(omikron_id) else {
        return;
    };
    for user_id in users {
        let request = CommunicationValue::new(CommunicationType::EraseHostedUserData)
            .add_typed_default(DataType::UserId, DataValue::SignedNumber(user_id.0.into()))
            .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()));
        if let Err(error) = connection.clone().send(&request).await {
            crate::log_in!(
                crate::util::logger::PrintType::General,
                "Iota user erasure delivery failed for Iota {}: {}",
                iota_id,
                error
            );
        }
    }
}

/* Provisioning retries until the destination Iota has persisted public
 * account metadata and acknowledges it. Credentials never enter this path. */
pub async fn deliver_pending_user_provisioning(state: &OmegaState, iota_id: i64) {
    let Ok(users) =
        user_repo::pending_iota_user_provisioning(crate::models::IotaId::from(iota_id)).await
    else {
        return;
    };
    let Some(omikron_id) = get_iota_primary_omikron_connection(state, iota_id) else {
        return;
    };
    let Some(connection) = state.connected_omikron(omikron_id) else {
        return;
    };
    for user in users {
        let Ok(public_key) = user.public_key.try_to_base64() else {
            continue;
        };
        let request = CommunicationValue::new(CommunicationType::ProvisionIotaUser)
            .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()))
            .add_typed_default(
                DataType::UserId,
                DataValue::SignedNumber(user.user_id.0.into()),
            )
            .add_typed_default(DataType::Username, DataValue::Str(user.username))
            .add_typed_default(DataType::PublicKey, DataValue::Str(public_key))
            .add_typed_default(
                DataType::InvitationId,
                DataValue::SignedNumber(user.invitation_id.into()),
            )
            .add_typed_default(
                DataType::InvitationRevision,
                DataValue::SignedNumber(user.invitation_revision.into()),
            );
        if let Err(error) = connection.clone().send(&request).await {
            crate::log_in!(
                crate::util::logger::PrintType::General,
                "Iota user provisioning delivery failed for Iota {}: {}",
                iota_id,
                error
            );
        }
    }
}

pub fn spawn_user_provisioning_outbox_worker(state: Arc<OmegaState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut retry = interval(Duration::from_secs(30));
        loop {
            retry.tick().await;
            match user_repo::pending_provisioning_iota_ids().await {
                Ok(iota_ids) => {
                    for iota_id in iota_ids {
                        if get_iota_primary_omikron_connection(&state, iota_id.0).is_some() {
                            deliver_pending_user_provisioning(&state, iota_id.0).await;
                        }
                    }
                }
                Err(error) => crate::log_in!(
                    crate::util::logger::PrintType::General,
                    "Could not load Iota provisioning outbox: {}",
                    error
                ),
            }
        }
    })
}

pub fn spawn_user_erasure_outbox_worker(state: Arc<OmegaState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut retry = interval(Duration::from_secs(30));
        loop {
            retry.tick().await;
            match user_repo::pending_erasure_iota_ids().await {
                Ok(iota_ids) => {
                    for iota_id in iota_ids {
                        if get_iota_primary_omikron_connection(&state, iota_id.0).is_some() {
                            deliver_pending_erasures(&state, iota_id.0).await;
                        }
                    }
                }
                Err(error) => crate::log_in!(
                    crate::util::logger::PrintType::General,
                    "Could not load Iota erasure outbox: {}",
                    error
                ),
            }
        }
    })
}
