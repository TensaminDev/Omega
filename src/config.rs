use std::{env, time::Duration};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    pub window: Duration,
    pub general_requests: usize,
    pub registration_requests: usize,
    pub transport_connections: usize,
    pub transport_connections_per_ip: usize,
}

#[derive(Clone, Debug)]
pub struct DispatchConfig {
    pub relay_max_concurrent: usize,
    pub relay_bytes_per_window: u64,
    pub relay_window: Duration,
    pub omikron_handler_concurrency: usize,
    pub global_handler_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub max_message_size: u64,
    pub handshake_max_message_size: u64,
}

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

#[derive(Clone, Debug)]
pub struct OmegaConfig {
    pub rate_limits: RateLimitConfig,
    pub database: DatabaseConfig,
    pub dispatch: DispatchConfig,
    pub transport: TransportConfig,
    pub max_short_link_target_bytes: usize,
    pub max_short_links_per_omikron: i64,
    pub short_link_insert_attempts: usize,
    pub logger_queue_capacity: usize,
}

#[derive(Clone, Debug)]
pub struct PublicUrlConfig {
    pub omega_base_url: String,
    pub client_base_url: String,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid configuration value for {name}: {value}")]
    InvalidValue { name: String, value: String },
    #[error("configuration value for {name} is not valid Unicode")]
    InvalidUnicode { name: String },
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
    pub fn from_env() -> Result<Self, ConfigError> {
        let defaults = Self::default();
        Ok(Self {
            window: env_duration("RATE_LIMIT_WINDOW_SECONDS", defaults.window)?,
            general_requests: env_usize("RATE_LIMIT_GENERAL_REQUESTS", defaults.general_requests)?,
            registration_requests: env_usize(
                "RATE_LIMIT_REGISTRATION_REQUESTS",
                defaults.registration_requests,
            )?,
            transport_connections: env_usize(
                "RATE_LIMIT_TRANSPORT_CONNECTIONS",
                defaults.transport_connections,
            )?,
            transport_connections_per_ip: env_usize(
                "RATE_LIMIT_TRANSPORT_CONNECTIONS_PER_IP",
                defaults.transport_connections_per_ip,
            )?,
        })
    }

    pub fn from_env_or_default() -> Self {
        Self::from_env().unwrap_or_default()
    }
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            relay_max_concurrent: 16,
            relay_bytes_per_window: 64 * 1024 * 1024,
            relay_window: Duration::from_secs(60),
            omikron_handler_concurrency: 32,
            global_handler_concurrency: 256,
        }
    }
}

impl Default for OmegaConfig {
    fn default() -> Self {
        Self {
            rate_limits: RateLimitConfig::default(),
            database: DatabaseConfig {
                max_connections: 200,
                min_connections: 0,
                acquire_timeout: Duration::from_secs(10),
                idle_timeout: Duration::from_secs(600),
                max_lifetime: Duration::from_secs(1800),
            },
            dispatch: DispatchConfig::default(),
            transport: TransportConfig {
                max_message_size: 16 * 1024 * 1024,
                handshake_max_message_size: 64 * 1024,
            },
            max_short_link_target_bytes: 2048,
            max_short_links_per_omikron: 10000,
            short_link_insert_attempts: 16,
            logger_queue_capacity: 1024,
        }
    }
}

impl OmegaConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let defaults = Self::default();
        PublicUrlConfig::from_env()?;
        let config = Self {
            rate_limits: RateLimitConfig::from_env()?,
            database: DatabaseConfig {
                max_connections: u32::try_from(env_usize(
                    "OMEGA_DB_MAX_CONNECTIONS",
                    defaults.database.max_connections as usize,
                )?)
                .map_err(|_| ConfigError::InvalidValue {
                    name: "OMEGA_DB_MAX_CONNECTIONS".into(),
                    value: "out of range".into(),
                })?,
                min_connections: u32::try_from(env_usize_allow_zero(
                    "OMEGA_DB_MIN_CONNECTIONS",
                    defaults.database.min_connections as usize,
                )?)
                .map_err(|_| ConfigError::InvalidValue {
                    name: "OMEGA_DB_MIN_CONNECTIONS".into(),
                    value: "out of range".into(),
                })?,
                acquire_timeout: env_duration(
                    "OMEGA_DB_ACQUIRE_TIMEOUT_SECONDS",
                    defaults.database.acquire_timeout,
                )?,
                idle_timeout: env_duration(
                    "OMEGA_DB_IDLE_TIMEOUT_SECONDS",
                    defaults.database.idle_timeout,
                )?,
                max_lifetime: env_duration(
                    "OMEGA_DB_MAX_LIFETIME_SECONDS",
                    defaults.database.max_lifetime,
                )?,
            },
            dispatch: DispatchConfig {
                relay_max_concurrent: env_usize(
                    "OMEGA_RELAY_MAX_CONCURRENT",
                    defaults.dispatch.relay_max_concurrent,
                )?,
                relay_bytes_per_window: env_u64(
                    "OMEGA_RELAY_BYTES_PER_WINDOW",
                    defaults.dispatch.relay_bytes_per_window,
                )?,
                relay_window: env_duration(
                    "OMEGA_RELAY_WINDOW_SECONDS",
                    defaults.dispatch.relay_window,
                )?,
                omikron_handler_concurrency: env_usize(
                    "OMEGA_OMIKRON_HANDLER_CONCURRENCY",
                    defaults.dispatch.omikron_handler_concurrency,
                )?,
                global_handler_concurrency: env_usize(
                    "OMEGA_GLOBAL_HANDLER_CONCURRENCY",
                    defaults.dispatch.global_handler_concurrency,
                )?,
            },
            transport: TransportConfig {
                max_message_size: env_u64(
                    "OMEGA_TRANSPORT_MAX_MESSAGE_SIZE",
                    defaults.transport.max_message_size,
                )?,
                handshake_max_message_size: env_u64(
                    "OMEGA_TRANSPORT_HANDSHAKE_MAX_MESSAGE_SIZE",
                    defaults.transport.handshake_max_message_size,
                )?,
            },
            max_short_link_target_bytes: env_usize(
                "OMEGA_SHORT_LINK_MAX_TARGET_BYTES",
                defaults.max_short_link_target_bytes,
            )?,
            max_short_links_per_omikron: i64::try_from(env_usize(
                "OMEGA_SHORT_LINK_MAX_PER_OMIKRON",
                usize::try_from(defaults.max_short_links_per_omikron).unwrap(),
            )?)
            .map_err(|_| ConfigError::InvalidValue {
                name: "OMEGA_SHORT_LINK_MAX_PER_OMIKRON".into(),
                value: "out of range".into(),
            })?,
            short_link_insert_attempts: env_usize(
                "OMEGA_SHORT_LINK_INSERT_ATTEMPTS",
                defaults.short_link_insert_attempts,
            )?,
            logger_queue_capacity: env_usize(
                "LOGGER_QUEUE_CAPACITY",
                defaults.logger_queue_capacity,
            )?,
        };
        if config.database.min_connections > config.database.max_connections {
            return Err(ConfigError::InvalidValue {
                name: "OMEGA_DB_MIN_CONNECTIONS".into(),
                value: "exceeds maximum".into(),
            });
        }
        Ok(config)
    }
}

impl Default for PublicUrlConfig {
    fn default() -> Self {
        Self {
            omega_base_url: "https://omega.tensamin.net".into(),
            client_base_url: "https://tensamin.net".into(),
        }
    }
}

impl PublicUrlConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let defaults = Self::default();
        Ok(Self {
            omega_base_url: env_public_base_url("PUBLIC_OMEGA_BASE_URL", &defaults.omega_base_url)?,
            client_base_url: env_public_base_url(
                "PUBLIC_CLIENT_BASE_URL",
                &defaults.client_base_url,
            )?,
        })
    }
}

pub fn public_urls() -> Result<PublicUrlConfig, ConfigError> {
    PublicUrlConfig::from_env()
}

fn env_usize_allow_zero(name: &str, fallback: usize) -> Result<usize, ConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|_| ConfigError::InvalidValue {
                name: name.into(),
                value,
            }),
        Err(env::VarError::NotPresent) => Ok(fallback),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidUnicode { name: name.into() }),
    }
}

fn env_usize(name: &str, fallback: usize) -> Result<usize, ConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| ConfigError::InvalidValue {
                name: name.into(),
                value,
            }),
        Err(env::VarError::NotPresent) => Ok(fallback),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidUnicode { name: name.into() }),
    }
}

fn env_u64(name: &str, fallback: u64) -> Result<u64, ConfigError> {
    match env::var(name) {
        Ok(value) => parse_positive_u64(name, value),
        Err(env::VarError::NotPresent) => Ok(fallback),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidUnicode { name: name.into() }),
    }
}

fn parse_positive_u64(name: &str, value: String) -> Result<u64, ConfigError> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ConfigError::InvalidValue {
            name: name.into(),
            value,
        })
}

fn env_duration(name: &str, fallback: Duration) -> Result<Duration, ConfigError> {
    env_usize(name, fallback.as_secs() as usize).map(|value| Duration::from_secs(value as u64))
}

fn env_public_base_url(name: &str, fallback: &str) -> Result<String, ConfigError> {
    let value = match env::var(name) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => fallback.to_owned(),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(ConfigError::InvalidUnicode { name: name.into() });
        }
    };
    let normalized = value.trim_end_matches('/');
    let valid = normalized.parse::<http::Uri>().ok().is_some_and(|uri| {
        matches!(uri.scheme_str(), Some("http" | "https"))
            && uri.authority().is_some()
            && matches!(uri.path(), "" | "/")
            && uri.query().is_none()
    });
    if !valid {
        return Err(ConfigError::InvalidValue {
            name: name.into(),
            value,
        });
    }
    Ok(normalized.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{env_public_base_url, parse_positive_u64};

    #[test]
    fn validates_public_base_urls() {
        assert_eq!(
            env_public_base_url("UNSET_TEST_URL", "https://example.com/").unwrap(),
            "https://example.com"
        );
        assert!(env_public_base_url("UNSET_TEST_URL", "example.com").is_err());
        assert!(env_public_base_url("UNSET_TEST_URL", "https://example.com/path").is_err());
    }

    #[test]
    fn rejects_zero_transport_limit() {
        assert!(parse_positive_u64("OMEGA_TRANSPORT_MAX_MESSAGE_SIZE", "0".into()).is_err());
    }
}
