use crate::db::user_repo;
use crate::models::IotaId;
use crate::sql::connection_status::UserStatus;
use dashmap::DashMap;
use once_cell::sync::Lazy;

#[derive(Debug, Clone)]
pub struct UserConnection {
    pub connection_type: UserStatus,
    pub omikron_id: i64,
}

// IotaID -> Primary OmikronID
static IOTA_PRIMARY_OMIKRON_CONNECTION: Lazy<DashMap<i64, i64>> = Lazy::new(DashMap::new);

// IotaID -> Vec<OmikronID>
static IOTA_OMIKRON_CONNECTIONS: Lazy<DashMap<i64, Vec<i64>>> = Lazy::new(DashMap::new);

// UserID -> UserStatus
static USER_STATUS_MAP: Lazy<DashMap<i64, UserConnection>> = Lazy::new(DashMap::new);

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
    let mut replacement = None;
    let mut connections_empty = false;
    if let Some(mut entry) = IOTA_OMIKRON_CONNECTIONS.get_mut(&iota_id) {
        entry.retain(|&id| id != omikron_id);
        connections_empty = entry.is_empty();
        replacement = entry.first().copied();
    }

    if connections_empty {
        IOTA_OMIKRON_CONNECTIONS.remove_if(&iota_id, |_, connections| connections.is_empty());
    }

    if IOTA_PRIMARY_OMIKRON_CONNECTION
        .get(&iota_id)
        .is_some_and(|primary| *primary == omikron_id)
    {
        match replacement {
            Some(omikron_id) => {
                IOTA_PRIMARY_OMIKRON_CONNECTION.insert(iota_id, omikron_id);
            }
            None => {
                IOTA_PRIMARY_OMIKRON_CONNECTION.remove(&iota_id);
            }
        }
    }

    connections_empty
}

pub fn get_iota_primary_omikron_connection(iota_id: i64) -> Option<i64> {
    IOTA_PRIMARY_OMIKRON_CONNECTION.get(&iota_id).map(|v| *v)
}

pub fn get_iota_omikron_connections(iota_id: i64) -> Option<Vec<i64>> {
    IOTA_OMIKRON_CONNECTIONS.get(&iota_id).map(|v| v.clone())
}

pub async fn get_all_connections()
-> Result<std::collections::HashMap<i64, std::collections::HashMap<i64, Vec<i64>>>, ()> {
    let mut result: std::collections::HashMap<i64, std::collections::HashMap<i64, Vec<i64>>> =
        std::collections::HashMap::new();

    for entry in IOTA_OMIKRON_CONNECTIONS.iter() {
        let iota_id = *entry.key();
        for omikron_id in entry.value().iter() {
            result
                .entry(*omikron_id)
                .or_insert_with(std::collections::HashMap::new)
                .entry(iota_id)
                .or_insert_with(Vec::new);
        }
    }

    for entry in IOTA_OMIKRON_CONNECTIONS.iter() {
        let iota_id = *entry.key();
        let omikron_ids = entry.value().clone();

        if let Ok(users) = user_repo::get_users_by_iota_id(IotaId::from(iota_id)).await {
            for user in users {
                let user_id = user.id.0;
                if let Some(conn) = USER_STATUS_MAP.get(&user_id) {
                    let user_omikron_id = conn.omikron_id;
                    if omikron_ids.contains(&user_omikron_id) {
                        if let Some(iota_map) = result.get_mut(&user_omikron_id) {
                            if let Some(user_vec) = iota_map.get_mut(&iota_id) {
                                user_vec.push(user_id);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(result)
}

pub fn track_user_status(user_id: i64, status: UserStatus, omikron_id: i64) {
    USER_STATUS_MAP.insert(
        user_id,
        UserConnection {
            connection_type: status,
            omikron_id,
        },
    );
}

pub fn untrack_user_status(user_id: i64, omikron_id: i64) {
    USER_STATUS_MAP.remove_if(&user_id, |_, connection| {
        connection.omikron_id == omikron_id
    });
}

pub fn get_user_status(user_id: i64) -> Option<UserConnection> {
    USER_STATUS_MAP.get(&user_id).map(|v| v.clone())
}

pub fn untrack_many_users(user_ids: &[i64]) {
    for user_id in user_ids {
        USER_STATUS_MAP.remove(user_id);
    }
}

pub async fn untrack_omikron(omikron_id: i64) {
    let mut offline_iotas = Vec::new();
    let mut primary_replacements = Vec::new();

    for mut entry in IOTA_OMIKRON_CONNECTIONS.iter_mut() {
        let iota_id = *entry.key();
        entry.retain(|&id| id != omikron_id);
        if entry.is_empty() {
            offline_iotas.push(iota_id);
        } else if IOTA_PRIMARY_OMIKRON_CONNECTION
            .get(&iota_id)
            .is_some_and(|primary| *primary == omikron_id)
        {
            primary_replacements.push((iota_id, entry[0]));
        }
    }

    for iota_id in &offline_iotas {
        IOTA_OMIKRON_CONNECTIONS.remove_if(iota_id, |_, connections| connections.is_empty());
        IOTA_PRIMARY_OMIKRON_CONNECTION.remove(iota_id);
    }

    for (iota_id, replacement) in primary_replacements {
        IOTA_PRIMARY_OMIKRON_CONNECTION.insert(iota_id, replacement);
    }

    USER_STATUS_MAP.retain(|_, status| status.omikron_id != omikron_id);

    for iota_id in offline_iotas {
        if let Ok(users) = user_repo::get_users_by_iota_id(IotaId::from(iota_id)).await {
            for user in users {
                USER_STATUS_MAP.remove(&user.id.0);
            }
        }
    }
}
