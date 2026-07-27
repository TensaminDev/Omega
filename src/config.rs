use std::{env, time::Duration};

#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    pub window: Duration,
    pub general_requests: usize,
    pub registration_requests: usize,
    pub transport_connections: usize,
    pub transport_connections_per_ip: usize,
}

pub const fn cors_origin() -> &'static str {
    "*"
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(60),
            general_requests: 120,
            registration_requests: 20,
            transport_connections: 512,
            transport_connections_per_ip: 32,
        }
    }
}

impl RateLimitConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            window: env_duration("RATE_LIMIT_WINDOW_SECONDS", defaults.window),
            general_requests: env_usize("RATE_LIMIT_GENERAL_REQUESTS", defaults.general_requests),
            registration_requests: env_usize(
                "RATE_LIMIT_REGISTRATION_REQUESTS",
                defaults.registration_requests,
            ),
            transport_connections: env_usize(
                "RATE_LIMIT_TRANSPORT_CONNECTIONS",
                defaults.transport_connections,
            ),
            transport_connections_per_ip: env_usize(
                "RATE_LIMIT_TRANSPORT_CONNECTIONS_PER_IP",
                defaults.transport_connections_per_ip,
            ),
        }
    }
}

fn env_usize(name: &str, fallback: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn env_duration(name: &str, fallback: Duration) -> Duration {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_secs)
        .unwrap_or(fallback)
}
