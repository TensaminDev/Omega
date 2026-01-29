use crate::sql;
use crate::sql::connection_status::ConnectionType;
use dashmap::DashMap;
use once_cell::sync::Lazy;

#[derive(Debug, Clone)]
pub struct UserStatus {
    pub connection_type: ConnectionType,
    pub omikron_id: i64,
}

// IotaID -> Primary OmikronID
static IOTA_PRIMARY_OMIKRON_CONNECTION: Lazy<DashMap<i64, i64>> = Lazy::new(DashMap::new);

// IotaID -> Vec<OmikronID>
static IOTA_OMIKRON_CONNECTIONS: Lazy<DashMap<i64, Vec<i64>>> = Lazy::new(DashMap::new);

// UserID -> UserStatus
static USER_STATUS_MAP: Lazy<DashMap<i64, UserStatus>> = Lazy::new(DashMap::new);

pub fn track_iota_connection(iota_id: i64, omikron_id: i64, primary: bool) {
    let mut entry = IOTA_OMIKRON_CONNECTIONS
        .entry(iota_id)
        .or_insert_with(Vec::new);

    if !entry.contains(&omikron_id) {
        entry.push(omikron_id);
    }

    if primary {
        IOTA_PRIMARY_OMIKRON_CONNECTION.insert(iota_id, omikron_id);
    }
}

pub fn untrack_iota_connection(iota_id: i64, omikron_id: i64) -> bool {
    if let Some(mut connections) = IOTA_OMIKRON_CONNECTIONS.get_mut(&iota_id) {
        connections.retain(|&id| id != omikron_id);

        if IOTA_PRIMARY_OMIKRON_CONNECTION
            .get(&iota_id)
            .map(|p| *p == omikron_id)
            .unwrap_or(false)
        {
            IOTA_PRIMARY_OMIKRON_CONNECTION.remove(&iota_id);
        }

        if connections.is_empty() {
            IOTA_OMIKRON_CONNECTIONS.remove(&iota_id);
            return true;
        }
    }
    false
}

pub fn get_iota_primary_omikron_connection(iota_id: i64) -> Option<i64> {
    IOTA_PRIMARY_OMIKRON_CONNECTION.get(&iota_id).map(|v| *v)
}

pub fn get_iota_omikron_connections(iota_id: i64) -> Option<Vec<i64>> {
    IOTA_OMIKRON_CONNECTIONS.get(&iota_id).map(|v| v.clone())
}

pub fn track_user_status(user_id: i64, status: ConnectionType, omikron_id: i64) {
    USER_STATUS_MAP.insert(
        user_id,
        UserStatus {
            connection_type: status,
            omikron_id,
        },
    );
}

pub fn get_user_status(user_id: i64) -> Option<UserStatus> {
    USER_STATUS_MAP.get(&user_id).map(|v| v.clone())
}

pub fn untrack_user(user_id: i64) {
    USER_STATUS_MAP.remove(&user_id);
}

pub fn untrack_many_users(user_ids: &[i64]) {
    for user_id in user_ids {
        USER_STATUS_MAP.remove(user_id);
    }
}

pub async fn untrack_omikron(omikron_id: i64) {
    let mut offline_iotas = Vec::new();

    IOTA_OMIKRON_CONNECTIONS.retain(|iota_id, connections| {
        connections.retain(|id| *id != omikron_id);

        if IOTA_PRIMARY_OMIKRON_CONNECTION
            .get(iota_id)
            .map(|p| *p == omikron_id)
            .unwrap_or(false)
        {
            IOTA_PRIMARY_OMIKRON_CONNECTION.remove(iota_id);
        }

        if connections.is_empty() {
            offline_iotas.push(*iota_id);
            false
        } else {
            true
        }
    });

    USER_STATUS_MAP.retain(|_, status| status.omikron_id != omikron_id);

    for iota_id in offline_iotas {
        if let Ok(users) = sql::sql::get_users_by_iota_id(iota_id).await {
            for user in users {
                USER_STATUS_MAP.remove(&user.0);
            }
        }
    }
}
