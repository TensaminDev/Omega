use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

static USER_OMIKRON_MAP: Lazy<Arc<RwLock<HashMap<i64, i64>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

pub async fn track_user_omikron(user: i64, omikron: i64) {
    let mut c = USER_OMIKRON_MAP.write().await;
    c.insert(user, omikron);
}

pub async fn get_omikron_for_user(user: i64) -> Option<i64> {
    let c = USER_OMIKRON_MAP.read().await;
    c.get(&user).cloned()
}

pub async fn untrack_user(user: i64) {
    let mut c = USER_OMIKRON_MAP.write().await;
    c.remove(&user);
}

pub async fn untrack_by_omikron(omikron: i64) {
    let mut c = USER_OMIKRON_MAP.write().await;
    c.retain(|_, v| *v != omikron);
}
