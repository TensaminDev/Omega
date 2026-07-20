mod api;
mod config;
mod db;
pub mod error;
mod models;
mod server;
mod sql;
mod transport;
mod util;

pub use error::{OmegaError, Result};

use crate::db::initialize;
use crate::transport::omikron_connection;
use crate::util::file_util::get_directory;
use crate::util::logger::PrintType;
use crate::util::logger::startup;
use dotenv::from_path;
use mtp::crypto::Keyring;
use mtp::files::{load_keyring_raw, save_keyring_raw, save_public_key_bundle};
use once_cell::sync::Lazy;
use rustls::crypto::aws_lc_rs::default_provider;
use std::env;
use std::path::Path;
use std::time::Duration;
use tokio::time::interval;

const KEYRING_PATH: &str = "./omega.mk";

static KEYRING: Lazy<Keyring> = Lazy::new(|| {
    load_keyring_raw(KEYRING_PATH).unwrap_or_else(|_| {
        let kr = Keyring::generate();
        if let Err(error) = save_keyring_raw(&kr, KEYRING_PATH) {
            eprintln!("Failed to save generated keyring: {error}");
        }
        if let Err(error) = save_public_key_bundle(&kr.public_key_bundle(), KEYRING_PATH) {
            eprintln!("Failed to save generated public key bundle: {error}");
        }
        eprintln!("Generated new keyring at {}", KEYRING_PATH);
        kr
    })
});

pub fn get_keyring() -> &'static Keyring {
    &KEYRING
}
pub fn load_keyring() -> Keyring {
    Keyring::from_bytes(&KEYRING.to_bytes()).unwrap_or_else(|error| {
        eprintln!("Failed to clone keyring: {error}");
        Keyring::generate()
    })
}

#[tokio::main]
async fn main() {
    if let Err(_) = default_provider().install_default() {
        println!("Error loading Provider");
        return;
    }
    let _ = from_path(Path::new(&get_directory()).join(".env"));
    startup();
    log_in!("Incoming messages");
    log_out!("Outgoing messages");

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
        result = omikron_connection::start(port) => {
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
