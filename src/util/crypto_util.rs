version = "0.1.0"
dependencies = [
 "aes",
 "aes-gcm",
 "ansi_term",
 "async-tungstenite",
 "axum",
 "base64 0.22.1",
 "block-modes",
 "bytes",
 "cbc",
 "chacha20poly1305",
 "chrono",
 "cmake",
 "crossterm",
 "futures-util",
 "hex",
 "hkdf",
 "http 1.4.0",
 "hyper",
 "json",

[[package]]
name = "block-modes"
version = "0.9.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "9e2211b0817f061502a8dd9f11a37e879e79763e3c698d2418cf824d8cb2f21e"

[[package]]
name = "block-padding"
version = "0.3.3"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "a8894febbff9f758034a5b8e12d87918f56dfc64a8e1fe757d65e29041538d93"
dependencies = [
 "generic-array",
]

[[package]]
name = "blocking"
version = "1.6.2"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "cbc"
version = "0.1.2"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "26b52a9543ae338f279b96b0b9fed9c8093744685043739079ce85cd58f289a6"
dependencies = [
 "cipher",
]

[[package]]
name = "cc"
version = "1.2.52"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "chacha20"
version = "0.9.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "c3613f74bd2eac03dad61bd53dbe620703d4371614fe0bc3b9f04dd36fe4e818"
dependencies = [
 "cfg-if",
 "cipher",
 "cpufeatures",
]

[[package]]
name = "chacha20poly1305"
version = "0.10.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "10cd79432192d1c0f4e1a0fef9527696cc039165d729fb41b3f4f4f354c2dc35"
dependencies = [
 "aead",
 "chacha20",
 "cipher",
 "poly1305",
 "zeroize",
]

[[package]]
name = "chrono"
version = "0.4.42"
source = "registry+https://github.com/rust-lang/crates.io-index"
 "crypto-common",
 "inout",
 "zeroize",
]

[[package]]

[[package]]
name = "hkdf"
version = "0.12.4"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "7b5f8eb2ad728638ea2c7d47a21db23b7b58a72ed6a38256b8a1849f15fbbdf7"
dependencies = [
 "hmac",
]

[[package]]
name = "hmac"
version = "0.12.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "879f10e63c20629ecabbb64a8010319738c66a5cd0c29b02d63d272b03751d01"
dependencies = [
 "block-padding",
 "generic-array",
]


[[package]]
name = "poly1305"
version = "0.8.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "8159bd90725d2df49889a078b54f4f79e87f1f8a8444194cdca81d38f5393abf"
dependencies = [
 "cpufeatures",
 "opaque-debug",
 "universal-hash",
]

[[package]]
name = "polyval"
version = "0.6.2"
source = "registry+https://github.com/rust-lang/crates.io-index"
aes-gcm = "0.10.3"
tokio-native-tls = "0.3.1"
hkdf = "0.12.4"
chacha20poly1305 = "0.10.1"
block-modes = "0.9.1"
cbc = "0.1.2"
aes = "0.8.4"

    get_user_data,
    get_iota_data,
    iota_user_data,

    change_user_data,
    change_iota_data,
            "getuserdata" => CommunicationType::get_user_data,
            "getiotadata" => CommunicationType::get_iota_data,
            "iotauserdata" => CommunicationType::iota_user_data,

            "changeuserdata" => CommunicationType::change_user_data,
            "changeiotadata" => CommunicationType::change_iota_data,
#[derive(Debug, Clone)]
pub struct CommunicationValue {
    pub id: Uuid,
    pub comm_type: CommunicationType,
    pub sender: i64,
    pub receiver: i64,
    pub data: HashMap<DataTypes, JsonValue>,
    id: Uuid,
    comm_type: CommunicationType,
    sender: i64,
    receiver: i64,
    data: HashMap<DataTypes, JsonValue>,
}

#[allow(dead_code)]
    }

    pub(crate) fn is_type(&self, p0: CommunicationType) -> bool {
    pub fn get_type(&self) -> CommunicationType {
        self.comm_type.clone()
    }
    pub fn is_type(&self, p0: CommunicationType) -> bool {
        self.comm_type == p0
    }
    pub fn to_json(&self) -> JsonValue {
use json::{JsonValue, number::Number};
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use std::{collections::HashMap, env, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock, mpsc},
    get_private_key, log, log_in, log_out,
    rho::rho_manager::{self, RHO_CONNECTIONS},
    util::crypto_helper::{decrypt, load_public_key},
    util::crypto_helper::{decrypt_b64, secret_key_to_base64},
    util::logger::PrintType,
};
use crate::{log_err, util::crypto_helper::secret_key_to_base64};
use crate::{log_err, util::crypto_helper::load_public_key};

pub static WAITING_TASKS: Lazy<
    DashMap<Uuid, Box<dyn Fn(Arc<OmegaConnection>, CommunicationValue) -> bool + Send + Sync>>,
> = Lazy::new(DashMap::new);

/// Flag to ensure the connection loop is only started once.
static CONNECTION_LOOP_STARTED: AtomicBool = AtomicBool::new(false);

static GENERIC_TASK: Lazy<
    Mutex<Option<Box<dyn Fn(Arc<OmegaConnection>, CommunicationValue) -> bool + Send + Sync>>>,
> = Lazy::new(|| Mutex::new(None));

static OMEGA_CONNECTION: Lazy<Arc<OmegaConnection>> = Lazy::new(|| {
    let conn = Arc::new(OmegaConnection::new());
    if !CONNECTION_LOOP_STARTED.swap(true, Ordering::SeqCst) {
        let conn_clone = conn.clone();
        tokio::spawn(async move {
            conn_clone.connect_internal(0).await;
        });
    }
    let conn_clone = conn.clone();
    tokio::spawn(async move {
        conn_clone.connect_internal(0).await;
    });
    conn
});

    }
    pub fn connect(self: Arc<OmegaConnection>) {
        if !CONNECTION_LOOP_STARTED.swap(true, Ordering::SeqCst) {
            let cloned_self = self.clone();
            tokio::spawn(async move {
                cloned_self.connect_internal(0).await;
            });
        }
        let cloned_self = self.clone();
        tokio::spawn(async move {
            cloned_self.connect_internal(0).await;
        });
    }
    async fn connect_internal(self: Arc<OmegaConnection>, mut retry: usize) {
        loop {
                                            })?;

                                        let server_pub_key_obj = load_public_key(server_pub_key).ok_or("Failed to load public key".to_string())?;
                                        let server_pub_key_obj = load_public_key(server_pub_key).unwrap();

                                        let decrypted_challenge = decrypt(
                                            get_private_key(),
                                            server_pub_key_obj,
                                        let decrypted_challenge = decrypt_b64(
                                            &secret_key_to_base64(&get_private_key()),
                                            server_pub_key,
                                            challenge,
                                        )
                                        .map_err(|e| {
                                                }

                                                if let Some(accepted) = final_cv.get_data(DataTypes::accepted).and_then(|v| v.as_bool()) {
                                                    if !accepted {
                                                        log_err!(PrintType::Omega, "Omega did not accept identification.");
                                                        return false;
                                                    }
                                                } else {
                                                    log_err!(PrintType::Omega, "Omega response did not contain 'accepted' field.");
                                                    return false;
                                                }

                                                tokio::spawn(async move {
                                                    let mut connected_iota_ids: Vec<JsonValue> = Vec::new();
                                                    let mut connected_user_ids: Vec<JsonValue> = Vec::new();
                    }
                    let msg_id = cv.get_id();
                    log_in!(PrintType::Omikron, "{}", &cv.to_json().to_string());
                    log_in!(PrintType::Omega, "{}", &cv.to_json().to_string());
                    // Handle waiting tasks
                    if let Some(task) = WAITING_TASKS.remove(&msg_id) {
                        if (task.1)(self.clone(), cv.clone()) {
                            // continue in the read_loop
                            continue;
                        }
                    } else {
                        // Handle generic task
                        let generic_task_option = GENERIC_TASK.lock().await;
                        if let Some(generic_task) = generic_task_option.as_ref() {
                            if generic_task(self.clone(), cv.clone()) {
                                // continue in the read_loop
                            }
                        }
                    }
                }
                #[allow(non_snake_case)]
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Close(_))) | None => continue,
                Some(Err(_)) => break,
                _ => {}
            }
use async_tungstenite::tungstenite::Message;
use async_tungstenite::{WebSocketReceiver, WebSocketSender};
use json::JsonValue;
use json::number::Number;
use rand::Rng;
use rand::distributions::Alphanumeric;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::compat::Compat;
use tungstenite::Utf8Bytes;
use crate::calls::call_manager;
use crate::omega::omega_connection::{WAITING_TASKS, get_omega_connection};
use crate::util::crypto_helper::{load_public_key, public_key_to_base64};
use crate::util::crypto_util::{DataFormat, SecurePayload};
use crate::util::logger::PrintType;
use crate::{
    // calls::call_manager::CallManager,
    omega::omega_connection::OmegaConnection,
};
use crate::{log_in, log_out};
use crate::{get_private_key, get_public_key, log_in, log_out};

/// ClientConnection represents a WebSocket connection from a client device
pub struct ClientConnection {
    /// WebSocket session
    pub sender: Arc<RwLock<WebSocketSender<Compat<tokio::net::TcpStream>>>>,
    pub receiver: Arc<RwLock<WebSocketReceiver<Compat<tokio::net::TcpStream>>>>,
    /// User ID associated with this client
    pub user_id: Arc<RwLock<i64>>,
    /// Whether this connection has been identified/authenticated
    pub identified: Arc<RwLock<bool>>,
    /// Ping latency tracking
    identified: Arc<RwLock<bool>>,
    challenged: Arc<RwLock<bool>>,
    challenge: Arc<RwLock<String>>,
    pub ping: Arc<RwLock<i64>>,
    /// Weak reference to RhoConnection to avoid circular references
    pub rho_connection: Arc<RwLock<Option<Weak<RhoConnection>>>>,
    /// List of user IDs this client is interested in receiving updates about
    pub_key: Arc<RwLock<Option<Vec<u8>>>>,
    pub rho_connection: Arc<RwLock<Option<Arc<RhoConnection>>>>,
    pub interested_users: Arc<RwLock<Vec<i64>>>,
}

            user_id: Arc::new(RwLock::new(0)),
            identified: Arc::new(RwLock::new(false)),
            challenged: Arc::new(RwLock::new(false)),
            challenge: Arc::new(RwLock::new(String::new())),
            ping: Arc::new(RwLock::new(-1)),
            pub_key: Arc::new(RwLock::new(None)),
            rho_connection: Arc::new(RwLock::new(None)),
            interested_users: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Set the RhoConnection reference
    pub async fn set_rho_connection(&self, rho_connection: Weak<RhoConnection>) {
        let mut rho_ref = self.rho_connection.write().await;
        *rho_ref = Some(rho_connection);
    }

    /// Get RhoConnection if available
    pub async fn get_rho_connection(&self) -> Option<Arc<RhoConnection>> {
        let rho_ref = self.rho_connection.read().await;
        if let Some(weak_ref) = rho_ref.as_ref() {
            weak_ref.upgrade()
        } else {
            None
        }
        self.rho_connection.read().await.clone()
    }

    /// Send a string message to the client
        tokio::spawn(async move {
            let cv = CommunicationValue::from_json(&message);
            if cv.is_type(CommunicationType::ping) {
                self.handle_ping(cv).await;
                return;
            }
            log_in!(PrintType::Client, "{}", &cv.to_json().to_string());
            let identified = *self.identified.read().await;
            let challenged = *self.challenged.read().await;

            // Handle identification
            if cv.is_type(CommunicationType::identification) && !self.is_identified().await {
                self.handle_identification(Arc::clone(&self), cv).await;
            if !identified && cv.is_type(CommunicationType::identification) {
                let user_id = cv
                    .get_data(DataTypes::user_id)
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                if user_id == 0 {
                    log_out!(PrintType::Client, "Invalid USER ID");
                    self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                        .await;
                    self.close().await;
                    return;
                }

                *self.user_id.write().await = user_id;

                let get_pub_key_msg = CommunicationValue::new(CommunicationType::get_user_data)
                    .with_id(cv.get_id())
                    .add_data(DataTypes::user_id, JsonValue::from(user_id));

                let response_cv = get_omega_connection()
                    .await_response(&get_pub_key_msg, Some(Duration::from_secs(20)))
                    .await;

                if let Ok(response_cv) = response_cv {
                    if !response_cv.is_type(CommunicationType::get_user_data) {
                        self.send_error_response(&cv.get_id(), CommunicationType::error_internal)
                            .await;
                        self.close().await;
                        return;
                    }

                    let base64_pub = response_cv
                        .get_data(DataTypes::public_key)
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    let pub_key = match load_public_key(base64_pub) {
                        Some(pk) => pk,
                        None => {
                            self.send_error_response(
                                &cv.get_id(),
                                CommunicationType::error_invalid_public_key,
                            )
                            .await;
                            self.close().await;
                            return;
                        }
                    };

                    *self.pub_key.write().await = Some(pub_key.as_bytes().to_vec());

                    let challenge: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(32)
                        .map(char::from)
                        .collect();

                    *self.challenge.write().await = challenge.clone();

                    let encrypted_challenge =
                        SecurePayload::new(challenge, DataFormat::Raw, get_private_key())
                            .unwrap()
                            .encrypt_x448(pub_key)
                            .unwrap()
                            .export(DataFormat::Base64);

                    *self.identified.write().await = true;

                    let challenge_msg = CommunicationValue::new(CommunicationType::challenge)
                        .with_id(cv.get_id())
                        .add_data_str(
                            DataTypes::public_key,
                            public_key_to_base64(&get_public_key()),
                        )
                        .add_data_str(DataTypes::challenge, encrypted_challenge);

                    self.send_message(&challenge_msg).await;
                } else {
                    self.send_error_response(&cv.get_id(), CommunicationType::error_internal)
                        .await;
                    self.close().await;
                    return;
                }

                return;
            }

            if identified && !challenged && cv.is_type(CommunicationType::challenge_response) {
                let client_response = cv
                    .get_data(DataTypes::challenge)
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if client_response == *self.challenge.read().await {
                    *self.challenged.write().await = true;

                    let user_id = self.get_user_id().await;

                    let rho_connection = match rho_manager::get_rho_con_for_user(user_id).await {
                        Some(rho) => rho,
                        None => {
                            self.send_error_response(
                                &cv.get_id(),
                                CommunicationType::error_no_iota,
                            )
                            .await;
                            return;
                        }
                    };

                    // Set identification data
                    {
                        let mut user_id_guard = self.user_id.write().await;
                        *user_id_guard = user_id;
                    }
                    {
                        let mut identified_guard = self.identified.write().await;
                        *identified_guard = true;
                    }
                    *self.rho_connection.write().await = Some(Arc::clone(&rho_connection));

                    let response =
                        CommunicationValue::new(CommunicationType::identification_response)
                            .with_id(cv.get_id());
                    self.send_message(&response).await;
                } else {
                    self.send_error_response(
                        &cv.get_id(),
                        CommunicationType::error_not_authenticated,
                    )
                    .await;
                    self.close().await;
                    return;
                }
                return;
            }

            if !self.is_identified().await {
                self.send_error_response(&cv.get_id(), CommunicationType::error_not_authenticated)
                    .await;
                self.close().await;
                return;
            }

    }

    /// Handle identification message
    async fn handle_identification(&self, sarc: Arc<ClientConnection>, cv: CommunicationValue) {
        // Extract user ID
        let user_id: i64 = match cv.get_data(DataTypes::user_id) {
            Some(id_str) => id_str.as_i64().unwrap_or(0),
            None => {
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_user_id)
                    .await;
                return;
            }
        };

        // Validate private key
        if let Some(private_key_hash) = cv.get_data(DataTypes::private_key_hash) {
            println!("private_key_hash: {}", private_key_hash);
            let is_valid = true; // NO VALIDATION,
            // SWAP TO AUTH VIA CHALLENGE
            // auth_connector::is_private_key_valid(user_id, &private_key_hash.to_string()).await;

            if !is_valid {
                println!("Invalid private key");
                self.send_error_response(
                    &cv.get_id(),
                    CommunicationType::error_invalid_private_key,
                )
                .await;
                return;
            }
        } else {
            log_in!(PrintType::Client, "Missing private key");
            self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_private_key)
                .await;
            return;
        }

        // Find RhoConnection for this user
        let rho_connection = match rho_manager::get_rho_con_for_user(user_id).await {
            Some(rho) => rho,
            None => {
                self.send_error_response(&cv.get_id(), CommunicationType::error_no_iota)
                    .await;
                return;
            }
        };

        // Set identification data
        {
            let mut user_id_guard = self.user_id.write().await;
            *user_id_guard = user_id;
        }
        {
            let mut identified_guard = self.identified.write().await;
            *identified_guard = true;
        }

        self.set_rho_connection(Arc::downgrade(&rho_connection))
            .await;

        rho_connection.add_client_connection(Arc::from(sarc)).await;

        let response = CommunicationValue::new(CommunicationType::identification_response)
            .with_id(cv.get_id());
        self.send_message(&response).await;
    }

    /// Handle ping message
    async fn handle_ping(&self, cv: CommunicationValue) {
        // Update our ping if provided
            user_id: Arc::clone(&self.user_id),
            identified: Arc::clone(&self.identified),
            challenged: Arc::clone(&self.challenged),
            challenge: Arc::clone(&self.challenge),
            ping: Arc::clone(&self.ping),
            pub_key: Arc::clone(&self.pub_key),
            rho_connection: Arc::clone(&self.rho_connection),
            interested_users: Arc::clone(&self.interested_users),
        }
use crate::util::crypto_helper::load_public_key;
use crate::util::crypto_helper::public_key_to_base64;
use crate::util::crypto_util::DataFormat;
use crate::util::crypto_util::SecurePayload;
use crate::util::logger::PrintType;
use async_tungstenite::WebSocketReceiver;
use async_tungstenite::WebSocketSender;
};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_util::compat::Compat;
use tungstenite::Utf8Bytes;
use uuid::Uuid;
use warp::filters::method::get;
use x448::PublicKey;

use super::{rho_connection::RhoConnection, rho_manager};
        }

        log_in!(PrintType::Iota, "{}", cv.to_json().to_string());

        let identified = *self.identified.read().await;
        let challenged = *self.challenged.read().await;

                .unwrap_or(0);
            if iota_id == 0 {
                log_out!(PrintType::Iota, "Invalid IOTA ID");
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                    .await;
                self.close().await;
            }

            let user_ids_json = cv
                .get_data(DataTypes::user_ids)
                .unwrap_or(&JsonValue::Null)
                .clone();
            let mut user_ids = Vec::new();
            if let JsonValue::Array(ids) = user_ids_json {
                for id_val in ids {
                    if let Some(id) = id_val.as_i64() {
                        user_ids.push(id);
                    }
                }
            }

            *self.iota_id.write().await = iota_id;
            *self.user_ids.write().await = user_ids;

            let get_pub_key_msg = CommunicationValue::new(CommunicationType::get_iota_data)
                .with_id(cv.get_id())

                let encrypted_challenge =
                    encrypt(get_private_key(), pub_key, &challenge).unwrap_or_default();
                    SecurePayload::new(&challenge, DataFormat::Base64, get_private_key())
                        .unwrap()
                        .encrypt_x448(pub_key)
                        .unwrap()
                        .export(DataFormat::Base64);

                *self.identified.write().await = true;


                let iota_id = self.get_iota_id().await;
                let user_ids = self.get_user_ids().await;

                let mut validated_user_ids: Vec<i64> = Vec::new();
                for user_id in user_ids {
                    validated_user_ids.push(user_id);
                }

                if rho_manager::contains_iota(iota_id).await {
                    if let Some(existing_rho) = rho_manager::get_rho_by_iota(iota_id).await {
                }

                // Inform Omega & Verify Users
                let iota_users_cv = get_omega_connection()
                    .await_response(
                        &CommunicationValue::new(CommunicationType::iota_connected).add_data(
                            DataTypes::iota_id,
                            JsonValue::from(self.get_iota_id().await),
                        ),
                        Some(Duration::from_secs(20)),
                    )
                    .await;

                let mut user_ids: Vec<i64> = Vec::new();
                if let Ok(iota_users_cv) = iota_users_cv {
                    if !iota_users_cv.is_type(CommunicationType::iota_user_data) {
                        log_err!(
                            PrintType::Omikron,
                            "Invalid communication type {:?}",
                            iota_users_cv.get_type()
                        );
                        return;
                    }
                    let val_user_ids = iota_users_cv.get_data(DataTypes::user_ids).unwrap().clone();

                    match val_user_ids {
                        JsonValue::Array(arr) => {
                            for item in arr {
                                if let JsonValue::Number(_) = item {
                                    user_ids.push(item.as_i64().unwrap_or(0));
                                }
                            }
                        }
                        _ => {}
                    }
                } else {
                    log_err!(PrintType::Omikron, "Failed to retrieve user IDs");
                }
                log_in!(PrintType::General, "User IDs: {:?}", user_ids.clone());

                *self.user_ids.write().await = user_ids.clone();
                let rho_connection =
                    Arc::new(RhoConnection::new(self.clone(), validated_user_ids.clone()).await);
                    Arc::new(RhoConnection::new(self.clone(), user_ids.clone()).await);

                self.set_rho_connection(Arc::downgrade(&rho_connection))
                    .await;

                let mut str = String::new();
                for id in &validated_user_ids {
                for id in &user_ids {
                    str.push_str(&format!(",{}", id));
                }
                if !str.is_empty() {
                        .with_id(cv.get_id())
                        .add_data_str(DataTypes::accepted_ids, str)
                        .add_data_str(DataTypes::accepted, validated_user_ids.len().to_string()),
                        .add_data_str(DataTypes::accepted, user_ids.len().to_string()),
                )
                .await;
            } else {
        }
    }
    pub async fn await_response(
        &self,
        cv: &CommunicationValue,
        timeout_duration: Option<Duration>,
    ) -> Result<CommunicationValue, String> {
        let (tx, mut rx) = mpsc::channel(1);
        let msg_id = cv.get_id();

        let task_tx = tx.clone();
        self.waiting_tasks.insert(
            msg_id,
            Box::new(move |_, response_cv| {
                let inner_tx = task_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = inner_tx.send(response_cv).await {
                        log_err!(
                            PrintType::Iota,
                            "Failed to send response back to awaiter: {}",
                            e
                        );
                    }
                });
                true
            }),
        );

        self.send_message(cv).await;

        let timeout = timeout_duration.unwrap_or(Duration::from_secs(10));

        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(response_cv)) => Ok(response_cv),
            Ok(None) => Err("Failed to receive response, channel was closed.".to_string()),
            Err(_) => {
                self.waiting_tasks.remove(&msg_id);
                Err(format!(
                    "Request timed out after {} seconds.",
                    timeout.as_secs()
                ))
            }
        }
    }
}

impl std::fmt::Debug for IotaConnection {
        };

        // Notify OmegaConnection about the new Iota
        OmegaConnection::connect_iota(rho_connection.get_iota_id().await, user_ids).await;

        rho_connection
    }

        let connections = self.client_connections.read().await;
        for connection in connections.iter() {
            if connection.get_user_id().await == cv.receiver {
            if connection.get_user_id().await == cv.get_receiver() {
                connection.send_message(&cv).await;
            }
        }
pub mod config_util;
pub mod crypto_helper;
pub mod crypto_util;
pub mod file_util;
pub mod logger;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STD};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use std::fmt;
use x448::{PublicKey, Secret};

// --- Custom Errors ---
#[derive(Debug)]
pub enum SecurePayloadError {
    InvalidBase64,
    InvalidHex,
    EncryptionError,
    DecryptionError,
    InvalidKeyLength,
}

// --- Data Format Enum ---
#[derive(Clone, Copy, Debug)]
pub enum DataFormat {
    Raw,
    Base64,
    Hex,
}

// --- Main Class Structure ---
pub struct SecurePayload {
    /// The internal canonical representation is always raw bytes.
    inner_data: Vec<u8>,
    /// The private key of the user associated with this payload instance.
    private_key: Secret,
}

impl Clone for SecurePayload {
    fn clone(&self) -> Self {
        Self {
            inner_data: self.inner_data.clone(),
            private_key: Secret::from_bytes(self.private_key.as_bytes()).unwrap(),
        }
    }
}

impl SecurePayload {
    /// Clear Constructor: Takes data in any format and the user's private key.
    pub fn new<S, T: AsRef<[u8]>>(
        data: T,
        format: DataFormat,
        private_key: S,
    ) -> Result<Self, SecurePayloadError>
    where
        S: Into<Secret>,
    {
        let raw_data = match format {
            DataFormat::Raw => data.as_ref().to_vec(),
            DataFormat::Base64 => BASE64_STD
                .decode(data.as_ref())
                .map_err(|_| SecurePayloadError::InvalidBase64)?,
            DataFormat::Hex => {
                hex::decode(data.as_ref()).map_err(|_| SecurePayloadError::InvalidHex)?
            }
        };

        Ok(Self {
            inner_data: raw_data,
            private_key: private_key.into(),
        })
    }

    /// Helper to get the public key associated with this instance's private key.
    pub fn get_public_key(&self) -> [u8; 56] {
        *PublicKey::from(&self.private_key).as_bytes()
    }

    /// Exports the internal data to the requested format
    pub fn export(&self, format: DataFormat) -> String {
        match format.into() {
            DataFormat::Raw => String::from_utf8_lossy(&self.inner_data).to_string(),
            DataFormat::Base64 => BASE64_STD.encode(&self.inner_data),
            DataFormat::Hex => hex::encode(&self.inner_data),
        }
    }

    /// Access raw bytes directly
    pub fn get_bytes(&self) -> &[u8] {
        &self.inner_data
    }

    /// Returns the SHA-256 Hash of the data in the requested format
    pub fn get_hash(&self, format: DataFormat) -> String {
        let mut hasher = Sha256::new();
        hasher.update(&self.inner_data);
        let result = hasher.finalize();

        match format {
            DataFormat::Raw => String::from_utf8_lossy(&result).to_string(),
            DataFormat::Base64 => BASE64_STD.encode(result),
            DataFormat::Hex => hex::encode(result),
        }
    }

    /// Encrypts the held data for a specific recipient using AES-256-GCM.
    /// The message will contain ONLY the ciphertext.
    pub fn encrypt_x448<S>(&self, public_key: S) -> Result<SecurePayload, SecurePayloadError>
    where
        S: Into<PublicKey>,
    {
        let peer_pub = public_key.into();
        let shared_secret = self.private_key.as_diffie_hellman(&peer_pub).unwrap();

        println!(
            "Encryption Shared Secret (Hex): {}",
            hex::encode(shared_secret.as_bytes())
        );

        // 3. Key & Nonce Derivation (HKDF)
        // We derive 32 bytes for the key and 12 bytes for a deterministic nonce.
        let hkdf = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());
        let mut okm = [0u8; 44]; // 32 (Key) + 12 (Nonce)
        hkdf.expand(b"x448-aes-gcm-no-overhead", &mut okm)
            .map_err(|_| SecurePayloadError::EncryptionError)?;

        let key = &okm[..32];
        let nonce_bytes = &okm[32..];

        // 4. Encrypt with AES-256-GCM
        let cipher = Aes256Gcm::new(key.into());
        let nonce = Nonce::from_slice(nonce_bytes);

        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: &self.inner_data,
                    aad: &[],
                },
            )
            .map_err(|_| SecurePayloadError::EncryptionError)?;

        // 5. Result is ONLY the ciphertext. No key or nonce is packed.
        Ok(SecurePayload {
            inner_data: ciphertext,
            private_key: Secret::from_bytes(self.private_key.as_bytes()).unwrap(),
        })
    }

    /// Decrypts the held data providing the sender's public key manually.
    pub fn decrypt_to_format(
        &self,
        peer_public_key_bytes: &[u8; 56],
        output_format: DataFormat,
    ) -> Result<String, SecurePayloadError> {
        let decrypted_instance = self.decrypt_x448(peer_public_key_bytes)?;
        Ok(decrypted_instance.export(output_format))
    }

    /// Decrypts the held data using the internal Private Key and the provided Peer Public Key.
    pub fn decrypt_x448(
        &self,
        peer_public_key_bytes: &[u8; 56],
    ) -> Result<SecurePayload, SecurePayloadError> {
        // 1. Perform Exchange
        let peer_pub = PublicKey::from_bytes(peer_public_key_bytes).unwrap();
        let shared_secret = self.private_key.as_diffie_hellman(&peer_pub).unwrap();

        // LOGGING: Shared Secret
        println!(
            "Decryption Shared Secret (Hex): {}",
            hex::encode(shared_secret.as_bytes())
        );

        // 2. Key & Nonce Derivation (Must match encryption exactly)
        let hkdf = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());
        let mut okm = [0u8; 44];
        hkdf.expand(b"x448-aes-gcm-no-overhead", &mut okm)
            .map_err(|_| SecurePayloadError::DecryptionError)?;

        let key = &okm[..32];
        let nonce_bytes = &okm[32..];

        // 3. Decrypt with AES-256-GCM
        let cipher = Aes256Gcm::new(key.into());
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext = cipher
            .decrypt(
                nonce,
                Payload {
                    msg: &self.inner_data,
                    aad: &[],
                },
            )
            .map_err(|_| SecurePayloadError::DecryptionError)?;

        Ok(SecurePayload {
            inner_data: plaintext,
            private_key: Secret::from_bytes(self.private_key.as_bytes()).unwrap(),
        })
    }
}
