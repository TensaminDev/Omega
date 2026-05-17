mod notifications;
mod server;
mod sql;
mod transport;
mod util;

use crate::notifications::tauri;
use crate::sql::sql::initialize_db;
use crate::sql::sql::print_users;
use crate::transport::omikron_connection;
use crate::util::crypto_helper::load_public_key;
use crate::util::crypto_helper::load_secret_key;
use crate::util::logger::PrintType;
use crate::util::logger::startup;
use dotenv::dotenv;
use once_cell::sync::Lazy;
use rustls::crypto::aws_lc_rs::default_provider;
use std::env;

static PRIVATE_KEY: Lazy<String> = Lazy::new(|| env::var("PRIVATE_KEY").unwrap());
pub fn get_private_key() -> x448::Secret {
    load_secret_key(&*PRIVATE_KEY).unwrap()
}
static PUBLIC_KEY: Lazy<String> = Lazy::new(|| env::var("PUBLIC_KEY").unwrap());
pub fn get_public_key() -> x448::PublicKey {
    load_public_key(&*PUBLIC_KEY).unwrap()
}

#[tokio::main]
async fn main() {
    if let Err(_) = default_provider().install_default() {
        println!("Error loading Provider");
        return;
    }
    dotenv().ok();
    startup();
    log_in!("Incoming messages");
    log_out!("Outgoing messages");

    let omikron_port: u16 = env::var("OMIKRON_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9187);

    tokio::spawn(async move {
        match omikron_connection::start(omikron_port).await {
            Err(e) => log_err!(0, PrintType::General, "{:?}", e),
            _ => {}
        }
    });

    tokio::spawn(async move {
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

    let _ = server::server::start(api_port).await;

    tokio::signal::ctrl_c().await.unwrap();
}
