use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

static IOTA_OMIKRON_MAP: Lazy<Arc<RwLock<HashMap<i64, i64>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

pub async fn track_iota_omikron(iota: i64, omikron: i64) {
    let mut c = IOTA_OMIKRON_MAP.write().await;
    c.insert(iota, omikron);
}

pub async fn get_omikron_for_iota(iota: i64) -> Option<i64> {
    let c = IOTA_OMIKRON_MAP.read().await;
    c.get(&iota).cloned()
}

pub async fn untrack_iota(iota: i64) {
    let mut c = IOTA_OMIKRON_MAP.write().await;
    c.remove(&iota);
}
