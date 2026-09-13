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
    pub omikron_handler_concurrency: usize,
    pub global_handler_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub max_message_size: u64,
    pub handshake_max_message_size: u64,
}

#[derive(Clone, Debug)]
pub struct OmegaConfig {
    pub rate_limits: RateLimitConfig,
    pub dispatch: DispatchConfig,
    pub transport: TransportConfig,
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
            omikron_handler_concurrency: 32,
            global_handler_concurrency: 256,
        }
    }
}

impl Default for OmegaConfig {
    fn default() -> Self {
        Self {
            rate_limits: RateLimitConfig::default(),
            dispatch: DispatchConfig::default(),
            transport: TransportConfig {
                max_message_size: 16 * 1024 * 1024,
                handshake_max_message_size: 64 * 1024,
            },
            logger_queue_capacity: 1024,
        }
    }
}

impl OmegaConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let defaults = Self::default();
        PublicUrlConfig::from_env()?;
        Ok(Self {
            rate_limits: RateLimitConfig::from_env()?,
            dispatch: DispatchConfig {
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
            logger_queue_capacity: env_usize(
                "LOGGER_QUEUE_CAPACITY",
                defaults.logger_queue_capacity,
            )?,
        })
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
