mod api;
mod config;
mod db;
pub mod error;
mod identity;
mod models;
mod server;
mod sql;
mod state;
mod transport;
mod util;

pub use error::{OmegaError, Result};

use crate::db::initialize;
use crate::state::OmegaState;
use crate::transport::omikron_connection;
use crate::util::file_util::get_directory;
use crate::util::logger::PrintType;
use crate::util::logger::startup;
use crate::{config::OmegaConfig, server::middleware};
use dotenv::from_path;
use rustls::crypto::aws_lc_rs::default_provider;
use std::env;
use std::path::Path;
use std::time::Duration;
use tokio::time::interval;
use zeroize::Zeroizing;

#[tokio::main]
async fn main() {
    if default_provider().install_default().is_err() {
        println!("Error loading Provider");
        return;
    }
    let _ = from_path(Path::new(&get_directory()).join(".env"));
    startup();
    log_in!("Incoming messages");
    log_out!("Outgoing messages");

    let identity_secret = match env::var("OMEGA_IDENTITY_SECRET") {
        Ok(secret) if !secret.is_empty() => secret,
        Ok(_) => {
            log!("[FATAL] OMEGA_IDENTITY_SECRET must not be empty");
            return;
        }
        Err(error) => {
            log!("[FATAL] Unable to load OMEGA_IDENTITY_SECRET: {}", error);
            return;
        }
    };
    let identity_secret = Zeroizing::new(identity_secret);
    let config = match OmegaConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            log!("[FATAL] Omega configuration is invalid: {}", error);
            return;
        }
    };
    if middleware::initialize_config(config.rate_limits.clone()).is_err() {
        log!("[FATAL] Omega rate-limit configuration was initialized more than once");
        return;
    }
    let identity = match identity::OmegaIdentity::load_or_create(identity_secret.as_bytes()) {
        Ok(identity) => identity,
        Err(error) => {
            log!("[FATAL] Omega identity initialization failed: {}", error);
            return;
        }
    };
    drop(identity_secret);
    let state = OmegaState::new(identity, config);

    log!("Started");
    log!("  .env");
    if let Err(e) = initialize().await {
        log!("[FATAL] Database initialization failed: {}", e);
        log!(
            "[FATAL] Please ensure the database is running and the .env file is configured correctly."
        );
        return;
    } else {
        log!("  DB");
    }
    let rate_limit_cleanup = crate::server::middleware::spawn_cleanup_task();
    let short_link_cleanup = tokio::spawn(async {
        let mut ticker = interval(Duration::from_secs(24 * 60 * 60));
        loop {
            ticker.tick().await;
            if let Err(error) = crate::db::short_link_repo::delete_expired().await {
                log_err!(
                    0,
                    PrintType::General,
                    "Short-link cleanup failed: {}",
                    error
                );
            }
        }
    });
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(443);

    tokio::select! {
        result = omikron_connection::start(port, state) => {
            if let Err(e) = result {
                log_err!(0, PrintType::General, "Server error: {:?}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            log!("Shutting down on signal...");
        }
    }
    rate_limit_cleanup.abort();
    short_link_cleanup.abort();
}
