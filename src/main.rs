mod notifications;
mod server;
mod sql;
mod transport;
mod util;

use crate::notifications::tauri;
use crate::sql::sql::initialize_db;
use crate::sql::sql::print_users;
use crate::transport::omikron_connection;
use crate::util::file_util::get_directory;
use crate::util::logger::PrintType;
use crate::util::logger::startup;
use base64::Engine as _;
use dotenv::from_path;
use mtp_crypto::{Keyring, PublicKeyBundle};
use once_cell::sync::Lazy;
use rustls::crypto::aws_lc_rs::default_provider;
use std::env;
use std::path::Path;

static KEYRING_ENV: Lazy<String> = Lazy::new(|| env::var("KEYRING").unwrap());
fn load_keyring() -> Keyring {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&*KEYRING_ENV)
        .expect("Invalid KEYRING env var: not valid base64");
    Keyring::from_bytes(&bytes).expect("Invalid KEYRING env var: failed to deserialize")
}
pub fn get_keyring() -> &'static Keyring {
    static KEYRING: Lazy<Keyring> = Lazy::new(load_keyring);
    &KEYRING
}
pub fn get_public_key_bundle() -> PublicKeyBundle {
    get_keyring().public_key_bundle()
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

    let omikron_port: u16 = env::var("OMIKRON_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9187);

    let omikron_handle = tokio::spawn(async move {
        match omikron_connection::start(omikron_port).await {
            Err(e) => log_err!(0, PrintType::General, "{:?}", e),
            _ => {}
        }
    });

    let tauri_handle = tokio::spawn(async move {
        match tauri::start(9189).await {
            Err(e) => log_err!(0, PrintType::General, "{:?}", e),
            _ => {}
        }
    });

    log!("Started");
    log!("  .env");
    if let Err(e) = initialize_db().await {
        log!("[FATAL] Database initialization failed: {}", e);
        log!(
            "[FATAL] Please ensure the database is running and the .env file is configured correctly."
        );
        return;
    } else {
        log!("  DB");
    }
    if let Err(e) = print_users().await {
        log!("[ERROR] Failed to print users: {}", e);
    } else {
        log!("  Users");
    }

    let api_port: u16 = env::var("API_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9188);

    tokio::select! {
        result = server::server::start(api_port) => {
            if let Err(e) = result {
                log_err!(0, PrintType::General, "Server error: {:?}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            log!("Shutting down on signal...");
        }
    }

    omikron_handle.abort();
    tauri_handle.abort();
}
