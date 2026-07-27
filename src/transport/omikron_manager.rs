use crate::transport::connection::OmikronConnection;
use dashmap::DashMap;
use mtp::codec::CommunicationValue;
use once_cell::sync::Lazy;
use rand::prelude::IteratorRandom;
use std::sync::Arc;

pub static OMIKRON_CONNECTIONS: Lazy<DashMap<i64, Arc<OmikronConnection>>> =
    Lazy::new(|| DashMap::new());

pub async fn add_omikron(conn: Arc<OmikronConnection>) {
    let id = match conn.clone().get_omikron_id().await {
        Some(id) => id,
        _ => {
            conn.close().await;
            return;
        }
    };

    if let Some(old) = OMIKRON_CONNECTIONS.insert(id, conn.clone()) {
        old.close().await;
    }
}

pub async fn remove_omikron(omikron_id: i64, connection: &Arc<OmikronConnection>) -> bool {
    OMIKRON_CONNECTIONS
        .remove_if(&omikron_id, |_, current| Arc::ptr_eq(current, connection))
        .is_some()
}

pub fn get_connected_omikron(omikron_id: i64) -> Option<Arc<OmikronConnection>> {
    OMIKRON_CONNECTIONS
        .get(&omikron_id)
        .map(|connection| connection.clone())
}

pub async fn get_random_omikron() -> Result<Arc<OmikronConnection>, ()> {
    let keys: Vec<_> = OMIKRON_CONNECTIONS.iter().map(|e| *e.key()).collect();

    if let Some(key) = keys.into_iter().choose(&mut rand::rng()) {
        if let Some(connection) = get_connected_omikron(key) {
            return Ok(connection);
        }
    }

    Err(())
}

pub async fn send_to_user(user_id: i64, cv: &CommunicationValue) {
    if let Some(user_conn) = crate::sql::user_online_tracker::get_user_status(user_id) {
        if let Some(omikron_conn) = OMIKRON_CONNECTIONS.get(&user_conn.omikron_id) {
            let _ = omikron_conn.value().clone().send_message(cv).await;
        }
    }
}
