use crate::sql;
use crate::sql::connection_status::ConnectionType;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct UserStatus {
    pub connection_type: ConnectionType,
    pub omikron_id: i64,
}

// IotaID -> Vec<OmikronID>
static IOTA_OMIKRON_CONNECTIONS: Lazy<Arc<RwLock<HashMap<i64, Vec<i64>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

// UserID -> UserStatus
static USER_STATUS_MAP: Lazy<Arc<RwLock<HashMap<i64, UserStatus>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

pub async fn track_iota_connection(iota_id: i64, omikron_id: i64) {
    let mut iota_map = IOTA_OMIKRON_CONNECTIONS.write().await;
    let connections = iota_map.entry(iota_id).or_default();
    if !connections.contains(&omikron_id) {
        connections.push(omikron_id);
    }
}

pub async fn untrack_iota_connection(iota_id: i64, omikron_id: i64) -> bool {
    let mut iota_map = IOTA_OMIKRON_CONNECTIONS.write().await;
    if let Some(connections) = iota_map.get_mut(&iota_id) {
        connections.retain(|&id| id != omikron_id);
        if connections.is_empty() {
            iota_map.remove(&iota_id);
            return true; // Iota is now offline
        }
    }
    false
}

pub async fn get_iota_omikron_connections(iota_id: i64) -> Option<Vec<i64>> {
    let iota_map = IOTA_OMIKRON_CONNECTIONS.read().await;
    iota_map.get(&iota_id).cloned()
}

pub async fn track_user_status(user_id: i64, status: ConnectionType, omikron_id: i64) {
    let mut user_map = USER_STATUS_MAP.write().await;
    user_map.insert(
        user_id,
        UserStatus {
            connection_type: status,
            omikron_id,
        },
    );
}

pub async fn get_user_status(user_id: i64) -> Option<UserStatus> {
    let user_map = USER_STATUS_MAP.read().await;
    user_map.get(&user_id).cloned()
}

pub async fn untrack_user(user_id: i64) {
    let mut user_map = USER_STATUS_MAP.write().await;
    user_map.remove(&user_id);
}

pub async fn untrack_many_users(user_ids: &[i64]) {
    let mut user_map = USER_STATUS_MAP.write().await;
    for user_id in user_ids {
        user_map.remove(user_id);
    }
}

pub async fn untrack_omikron(omikron_id: i64) {
    let mut iota_map = IOTA_OMIKRON_CONNECTIONS.write().await;
    let mut user_map = USER_STATUS_MAP.write().await;

    let mut offline_iotas = Vec::new();

    iota_map.retain(|iota_id, connections| {
        connections.retain(|id| *id != omikron_id);
        if connections.is_empty() {
            offline_iotas.push(*iota_id);
            false
        } else {
            true
        }
    });

    user_map.retain(|_, status| status.omikron_id != omikron_id);

    for iota_id in offline_iotas {
        if let Ok(users) = sql::sql::get_users_by_iota_id(iota_id).await {
            for user in users {
                user_map.remove(&user.0);
            }
        }
    }
}
