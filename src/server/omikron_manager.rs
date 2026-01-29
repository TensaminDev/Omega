use crate::{log_in, server::omikron_connection::OmikronConnection, util::logger::PrintType};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use rand::prelude::IteratorRandom;
use std::sync::Arc;

pub static OMIKRON_CONNECTIONS: Lazy<DashMap<i64, Arc<OmikronConnection>>> =
    Lazy::new(|| DashMap::new());

pub async fn add_omikron(omikron_conn: Arc<OmikronConnection>) {
    OMIKRON_CONNECTIONS.insert(omikron_conn.get_omikron_id().await, omikron_conn);
}

pub async fn remove_omikron(omikron_id: i64) {
    OMIKRON_CONNECTIONS.remove(&omikron_id);
}

pub async fn get_random_omikron() -> Result<Arc<OmikronConnection>, ()> {
    log_in!(0, PrintType::Iota, "{}", OMIKRON_CONNECTIONS.len());
    let mut rng = rand::thread_rng();
    if let Some((_, val)) = OMIKRON_CONNECTIONS.clone().into_iter().choose(&mut rng) {
        return Ok(val);
    } else {
        return Err(());
    }
}

pub async fn get_omikron(omikron_id: i64) -> Option<Arc<OmikronConnection>> {
    if let Some(omikron) = OMIKRON_CONNECTIONS.get(&omikron_id) {
        Some(omikron.clone())
    } else {
        None
    }
}
