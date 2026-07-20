use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::net::IpAddr;
use std::{collections::VecDeque, time::Instant};
use tokio::time::interval;

static REQUESTS: Lazy<DashMap<(IpAddr, String), VecDeque<Instant>>> = Lazy::new(DashMap::new);
static CONFIG: Lazy<crate::config::RateLimitConfig> =
    Lazy::new(crate::config::RateLimitConfig::from_env);
const MAX_TRACKED_CLIENT_BUCKETS: usize = 100_000;

pub fn allow(remote_addr: IpAddr, path: &str) -> bool {
    let key = if path.contains("register") {
        "registration"
    } else {
        "general"
    };
    let map_key = (remote_addr, key.to_string());
    if REQUESTS.len() >= MAX_TRACKED_CLIENT_BUCKETS {
        cleanup_expired();
        if REQUESTS.len() >= MAX_TRACKED_CLIENT_BUCKETS && !REQUESTS.contains_key(&map_key) {
            return false;
        }
    }
    let limit = if key == "registration" {
        CONFIG.registration_requests
    } else {
        CONFIG.general_requests
    };
    let now = Instant::now();
    let mut entries = REQUESTS.entry(map_key).or_default();
    while entries
        .front()
        .is_some_and(|time| now.duration_since(*time) >= CONFIG.window)
    {
        entries.pop_front();
    }
    if entries.len() >= limit {
        return false;
    }
    entries.push_back(now);
    true
}

pub fn spawn_cleanup_task() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        let mut ticker = interval(CONFIG.window);
        loop {
            ticker.tick().await;
            cleanup_expired();
        }
    })
}

fn cleanup_expired() {
    let now = Instant::now();
    REQUESTS.retain(|_, entries| {
        while entries
            .front()
            .is_some_and(|time| now.duration_since(*time) >= CONFIG.window)
        {
            entries.pop_front();
        }
        !entries.is_empty()
    });
}

#[cfg(test)]
mod tests {
    use super::allow;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn tracks_clients_independently() {
        let first: IpAddr = "192.0.2.10"
            .parse()
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let second: IpAddr = "192.0.2.11"
            .parse()
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(allow(first, "/api/get/user/1"));
        assert!(allow(second, "/api/get/user/1"));
    }
}
