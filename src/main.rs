mod server;
mod sql;
mod transport;
mod util;

use crate::sql::sql::initialize_db;
use crate::sql::sql::print_users;
use crate::transport::omikron_connection;
use crate::util::file_util::get_directory;
use crate::util::logger::PrintType;
use crate::util::logger::startup;
use dotenv::from_path;
use mtp::files::{load_keyring as load_keyring_file, save_keyring, save_public_key_bundle};
use mtp_crypto::Keyring;
use once_cell::sync::Lazy;
use rustls::crypto::aws_lc_rs::default_provider;
use std::env;
use std::path::Path;

const KEYRING_PATH: &str = "./omega.mk";

static KEYRING: Lazy<Keyring> = Lazy::new(|| {
    load_keyring_file(KEYRING_PATH).unwrap_or_else(|_| {
        let kr = Keyring::generate();
        save_keyring(&kr, KEYRING_PATH).expect("Failed to save generated keyring");
        save_public_key_bundle(&kr.public_key_bundle(), KEYRING_PATH)
            .expect("Failed to save generated public key bundle");
        eprintln!("Generated new keyring at {}", KEYRING_PATH);
        kr
    })
});

pub fn get_keyring() -> &'static Keyring {
    &KEYRING
}
pub fn load_keyring() -> Keyring {
    Keyring::from_bytes(&KEYRING.to_bytes()).unwrap()
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
}
