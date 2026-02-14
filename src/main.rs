mod data;
mod server;
mod sql;
mod util;

use crate::sql::sql::initialize_db;
use crate::sql::sql::print_users;
use crate::util::crypto_helper::load_public_key;
use crate::util::crypto_helper::load_secret_key;
use crate::util::logger::startup;
use dotenv::dotenv;
use once_cell::sync::Lazy;
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
    dotenv().ok();
    startup();
    log_in!("Incoming messages");
    log_out!("Outgoing messages");
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
    server::server::start(9187, 9188).await;
    log!("  API Server on 9187");
    log!("  WS Server on 9188");
    tokio::signal::ctrl_c().await.unwrap();
}
