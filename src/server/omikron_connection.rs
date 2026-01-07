use crate::data::communication::{CommunicationType, CommunicationValue, DataTypes};
use crate::sql::connection_status::ConnectionType;
use crate::sql::sql::{self, get_by_user_id, get_by_username, get_iota_by_id, get_omikron_by_id};
use crate::sql::user_online_tracker::{self};
use crate::util::crypto_helper::encrypt;
use crate::util::logger::PrintType;
use crate::{get_private_key, log_out};
use crate::{get_public_key, log_in};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use dashmap::DashMap;
use futures::SinkExt;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use json::JsonValue;
use json::number::Number;
use rand::Rng;
use rand::distributions::Alphanumeric;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message;
use tungstenite::Utf8Bytes;
use uuid::Uuid;
use x448::PublicKey;

pub struct OmikronConnection {
    pub sender: Arc<RwLock<SplitSink<WebSocketStream<TokioIo<Upgraded>>, Message>>>,
    pub receiver: Arc<RwLock<SplitStream<WebSocketStream<TokioIo<Upgraded>>>>>,
    pub omikron_id: Arc<RwLock<i64>>,
    pub pub_key: Arc<RwLock<Option<Vec<u8>>>>,
    identified: Arc<RwLock<bool>>,
    challenged: Arc<RwLock<bool>>,
    challenge: Arc<RwLock<String>>,
    pub ping: Arc<RwLock<i64>>,
    waiting_tasks: DashMap<
        Uuid,
        Box<dyn Fn(Arc<OmikronConnection>, CommunicationValue) -> bool + Send + Sync>,
    >,
}

impl OmikronConnection {
    pub fn new(
        sender: SplitSink<WebSocketStream<TokioIo<Upgraded>>, Message>,
        receiver: SplitStream<WebSocketStream<TokioIo<Upgraded>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            sender: Arc::new(RwLock::new(sender)),
            receiver: Arc::new(RwLock::new(receiver)),
            omikron_id: Arc::new(RwLock::new(0)),
            pub_key: Arc::new(RwLock::new(None)),
            identified: Arc::new(RwLock::new(false)),
            challenged: Arc::new(RwLock::new(false)),
            challenge: Arc::new(RwLock::new(String::new())),
            ping: Arc::new(RwLock::new(-1)),
            waiting_tasks: DashMap::new(),
        })
    }
    pub async fn send_message(&self, cv: &CommunicationValue) {
        let mut sender = self.sender.write().await;
        let message_text = Message::Text(Utf8Bytes::from(cv.to_json().to_string()));
        if !cv.is_type(CommunicationType::pong) {
            log_out!(
                *self.omikron_id.read().await,
                PrintType::Omikron,
                "{}",
                message_text
            );
        }
        sender.send(message_text).await.unwrap();
    }
    pub async fn get_omikron_id(&self) -> i64 {
        *self.omikron_id.read().await
    }
    pub async fn is_identified(&self) -> bool {
        *self.identified.read().await && *self.challenged.read().await
    }
    pub async fn get_public_key(&self) -> PublicKey {
        PublicKey::from_bytes(self.pub_key.read().await.as_ref().unwrap()).unwrap()
    }

    pub async fn handle_message(self: Arc<Self>, message: String) {
        let cv = CommunicationValue::from_json(&message);
        if cv.is_type(CommunicationType::ping) {
            self.handle_ping(cv).await;
            return;
        }

        log_in!(
            *self.omikron_id.read().await,
            PrintType::Omikron,
            "{}",
            message
        );

        if let Some((_, task)) = self.waiting_tasks.remove(&cv.get_id()) {
            let _ = task(self.clone(), cv.clone());
            return;
        }

        // If not yet identified
        if !self.is_identified().await {
            // handle identification
            if !*self.identified.read().await && cv.is_type(CommunicationType::identification) {
                let omikron_id = cv
                    .get_data(DataTypes::omikron)
                    .unwrap_or(&JsonValue::Null)
                    .as_i64()
                    .unwrap_or(0);
                match get_omikron_by_id(omikron_id).await {
                    Ok((public_key, _)) => {
                        // Generate Challenge, encrypt it and send it to the omikron
                        *self.omikron_id.write().await = omikron_id;

                        let challenge_str: String = rand::thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(32)
                            .map(char::from)
                            .collect();

                        *self.challenge.write().await = challenge_str.clone();

                        let user_public_key_bytes = match STANDARD.decode(&public_key) {
                            Ok(bytes) => bytes,
                            Err(_) => {
                                self.send_error_response(
                                    &cv.get_id(),
                                    CommunicationType::error_invalid_omikron_id,
                                )
                                .await;
                                return;
                            }
                        };
                        *self.pub_key.write().await = Some(user_public_key_bytes.clone());

                        let omikron_pub_key: PublicKey =
                            match PublicKey::from_bytes(&user_public_key_bytes) {
                                Some(key) => key,
                                _ => {
                                    self.send_error_response(
                                        &cv.get_id(),
                                        CommunicationType::error_invalid_public_key,
                                    )
                                    .await;
                                    return;
                                }
                            };

                        let encrypted_challenge =
                            encrypt(get_private_key(), omikron_pub_key, &challenge_str)
                                .unwrap_or("".to_string());

                        let response = CommunicationValue::new(CommunicationType::challenge)
                            .add_data_str(
                                DataTypes::public_key,
                                STANDARD.encode(get_public_key().as_bytes()),
                            )
                            .add_data_str(DataTypes::challenge, encrypted_challenge)
                            .with_id(cv.get_id());

                        self.send_message(&response).await;
                        *self.identified.write().await = true;
                        return;
                    }
                    Err(e) => {
                        self.send_message(
                            &CommunicationValue::new(CommunicationType::error_not_authenticated)
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::error_type, e.to_string()),
                        )
                        .await;

                        return;
                    }
                }
            }

            // Handle challenge response
            if *self.identified.read().await
                && !*self.challenged.read().await
                && cv.is_type(CommunicationType::challenge_response)
            {
                let client_response = cv
                    .get_data(DataTypes::challenge)
                    .unwrap_or(&JsonValue::Null)
                    .as_str()
                    .unwrap_or("");
                let expected_challenge = self.challenge.read().await.clone();

                if client_response == expected_challenge {
                    *self.challenged.write().await = true;

                    let response =
                        CommunicationValue::new(CommunicationType::identification_response)
                            .with_id(cv.get_id());
                    let _ = sql::set_omikron_active(self.get_omikron_id().await, true);
                    self.send_message(&response).await;
                } else {
                    self.send_error_response(
                        &cv.get_id(),
                        CommunicationType::error_invalid_challenge,
                    )
                    .await;
                    self.close().await;
                }
                return;
            }

            // if not identified && not identifying
            self.send_error_response(&cv.get_id(), CommunicationType::error_not_authenticated)
                .await;
            self.close().await;
            return;
        }

        let omikron_id = self.get_omikron_id().await;
        if cv.is_type(CommunicationType::user_connected) {
            if let Some(user_id) = cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()) {
                user_online_tracker::track_user_status(user_id, ConnectionType::Online, omikron_id)
                    .await;
            }
            return;
        }
        if cv.is_type(CommunicationType::user_disconnected) {
            if let Some(user_id) = cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()) {
                if let Some(status) = user_online_tracker::get_user_status(user_id).await {
                    user_online_tracker::track_user_status(
                        user_id,
                        ConnectionType::UserOffline,
                        status.omikron_id,
                    )
                    .await;
                }
            }
            return;
        }
        if cv.is_type(CommunicationType::iota_connected) {
            if let Some(iota_id) = cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64()) {
                user_online_tracker::track_iota_connection(iota_id, omikron_id).await;
                if let Ok(users) = sql::get_users_by_iota_id(iota_id).await {
                    for user in users {
                        user_online_tracker::track_user_status(
                            user.0,
                            ConnectionType::UserOffline,
                            omikron_id,
                        )
                        .await;
                    }
                }
            }
            return;
        }
        if cv.is_type(CommunicationType::iota_disconnected) {
            if let Some(iota_id) = cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64()) {
                let iota_offline =
                    user_online_tracker::untrack_iota_connection(iota_id, omikron_id).await;
                if iota_offline {
                    if let Ok(users) = sql::get_users_by_iota_id(iota_id).await {
                        let user_ids: Vec<i64> = users.iter().map(|u| u.0).collect();
                        user_online_tracker::untrack_many_users(&user_ids).await;
                    }
                }
            }
            return;
        }
        if cv.is_type(CommunicationType::sync_client_iota_status) {
            if let Some(json::JsonValue::Array(user_ids)) =
                cv.get_data(DataTypes::user_ids).cloned()
            {
                for user_id_json in user_ids {
                    if let Some(user_id) = user_id_json.as_i64() {
                        user_online_tracker::track_user_status(
                            user_id,
                            ConnectionType::Online,
                            omikron_id,
                        )
                        .await;
                    }
                }
            }
            if let Some(json::JsonValue::Array(iota_ids)) =
                cv.get_data(DataTypes::iota_ids).cloned()
            {
                for iota_id_json in iota_ids {
                    if let Some(iota_id) = iota_id_json.as_i64() {
                        user_online_tracker::track_iota_connection(iota_id, omikron_id).await;
                    }
                }
            }
            return;
        }
        if cv.is_type(CommunicationType::get_user_data) {
            if let Some(user_id) = cv.get_data(DataTypes::user_id).cloned() {
                if let Some(user_id) = user_id.as_i64() {
                    if let Ok((
                        id,
                        iota_id,
                        username,
                        display,
                        status,
                        about,
                        avatar,
                        sub_level,
                        sub_end,
                        public_key,
                        _,
                        _,
                    )) = get_by_user_id(user_id).await
                    {
                        let mut response =
                            CommunicationValue::new(CommunicationType::get_user_data)
                                .add_data_str(DataTypes::username, username)
                                .add_data_str(DataTypes::public_key, public_key)
                                .add_data(DataTypes::user_id, JsonValue::Number(Number::from(id)))
                                .add_data(
                                    DataTypes::iota_id,
                                    JsonValue::Number(Number::from(iota_id)),
                                )
                                .add_data_str(DataTypes::display, display)
                                .add_data_str(DataTypes::status, status)
                                .add_data_str(DataTypes::about, about)
                                .add_data_str(DataTypes::avatar, avatar)
                                .add_data(
                                    DataTypes::sub_level,
                                    JsonValue::Number(Number::from(sub_level)),
                                )
                                .add_data(
                                    DataTypes::sub_end,
                                    JsonValue::Number(Number::from(sub_end)),
                                );

                        let user_status = user_online_tracker::get_user_status(id).await;
                        let iota_connections =
                            user_online_tracker::get_iota_omikron_connections(iota_id)
                                .await
                                .unwrap_or_default();
                        response = response.add_data(
                            DataTypes::omikron_connections,
                            JsonValue::Array(
                                iota_connections
                                    .into_iter()
                                    .map(|id| JsonValue::Number(Number::from(id)))
                                    .collect(),
                            ),
                        );

                        if let Some(user_status) = user_status {
                            response = response.add_data(
                                DataTypes::online_status,
                                JsonValue::String(user_status.connection_type.to_string()),
                            );
                            response = response.add_data(
                                DataTypes::omikron_id,
                                JsonValue::Number(Number::from(user_status.omikron_id)),
                            );
                        } else {
                            response = response.add_data(
                                DataTypes::online_status,
                                JsonValue::String(ConnectionType::IotaOffline.to_string()),
                            );
                        }

                        self.send_message(&response).await;
                        return;
                    }
                }
            }
            if let Some(username) = cv.get_data(DataTypes::username).cloned() {
                if let Some(username) = username.as_str() {
                    if let Ok((
                        id,
                        iota_id,
                        username,
                        display,
                        status,
                        about,
                        avatar,
                        sub_level,
                        sub_end,
                        public_key,
                        _,
                        _,
                    )) = get_by_username(username).await
                    {
                        let mut response =
                            CommunicationValue::new(CommunicationType::get_user_data)
                                .add_data_str(DataTypes::username, username)
                                .add_data_str(DataTypes::public_key, public_key)
                                .add_data(DataTypes::user_id, JsonValue::Number(Number::from(id)))
                                .add_data(
                                    DataTypes::iota_id,
                                    JsonValue::Number(Number::from(iota_id)),
                                )
                                .add_data_str(DataTypes::display, display)
                                .add_data_str(DataTypes::status, status)
                                .add_data_str(DataTypes::about, about)
                                .add_data_str(DataTypes::avatar, avatar)
                                .add_data(
                                    DataTypes::sub_level,
                                    JsonValue::Number(Number::from(sub_level)),
                                )
                                .add_data(
                                    DataTypes::sub_end,
                                    JsonValue::Number(Number::from(sub_end)),
                                );

                        let user_status = user_online_tracker::get_user_status(id).await;
                        let iota_connections =
                            user_online_tracker::get_iota_omikron_connections(iota_id)
                                .await
                                .unwrap_or_default();

                        if let Some(user_status) = user_status {
                            response = response.add_data(
                                DataTypes::online_status,
                                JsonValue::String(user_status.connection_type.to_string()),
                            );
                            response = response.add_data(
                                DataTypes::omikron_id,
                                JsonValue::Number(Number::from(user_status.omikron_id)),
                            );
                        } else {
                            response = response.add_data(
                                DataTypes::online_status,
                                JsonValue::String(ConnectionType::IotaOffline.to_string()),
                            );
                        }
                        response = response.add_data(
                            DataTypes::omikron_connections,
                            JsonValue::Array(
                                iota_connections
                                    .iter()
                                    .map(|&id| JsonValue::Number(Number::from(id)))
                                    .collect(),
                            ),
                        );

                        self.send_message(&response).await;
                        return;
                    }
                }
            }
            let response =
                CommunicationValue::new(CommunicationType::error_not_found).with_id(cv.get_id());
            self.send_message(&response).await;

            return;
        }
        if cv.is_type(CommunicationType::get_iota_data) {
            if let Some(iota_id) = cv.get_data(DataTypes::iota_id).cloned() {
                if let Some(iota_id) = iota_id.as_i64() {
                    if let Ok((iota_id, public_key)) = get_iota_by_id(iota_id).await {
                        let mut response =
                            CommunicationValue::new(CommunicationType::get_iota_data)
                                .add_data_str(DataTypes::public_key, public_key)
                                .add_data(
                                    DataTypes::iota_id,
                                    JsonValue::Number(Number::from(iota_id)),
                                );

                        let iota_connections =
                            user_online_tracker::get_iota_omikron_connections(iota_id)
                                .await
                                .unwrap_or_default();

                        response = response.add_data(
                            DataTypes::omikron_connections,
                            JsonValue::Array(
                                iota_connections
                                    .iter()
                                    .map(|&id| JsonValue::Number(Number::from(id)))
                                    .collect(),
                            ),
                        );
                        self.send_message(&response).await;
                        return;
                    }
                }
            }
            if let Some(user_id) = cv.get_data(DataTypes::user_id).cloned() {
                if let Some(user_id) = user_id.as_i64() {
                    if let Ok((_, iota_id, _, _, _, _, _, _, _, _, _, _)) =
                        get_by_user_id(user_id).await
                    {
                        if let Ok((iota_id, public_key)) = get_iota_by_id(iota_id).await {
                            let mut response =
                                CommunicationValue::new(CommunicationType::get_iota_data)
                                    .add_data_str(DataTypes::public_key, public_key)
                                    .add_data(
                                        DataTypes::user_id,
                                        JsonValue::Number(Number::from(user_id)),
                                    )
                                    .add_data(
                                        DataTypes::iota_id,
                                        JsonValue::Number(Number::from(iota_id)),
                                    );

                            let iota_connections =
                                user_online_tracker::get_iota_omikron_connections(iota_id)
                                    .await
                                    .unwrap_or_default();

                            response = response.add_data(
                                DataTypes::omikron_connections,
                                JsonValue::Array(
                                    iota_connections
                                        .iter()
                                        .map(|&id| JsonValue::Number(Number::from(id)))
                                        .collect(),
                                ),
                            );

                            self.send_message(&response).await;
                            return;
                        }
                    }
                }
            }
            if let Some(username) = cv.get_data(DataTypes::username).cloned() {
                if let Some(username) = username.as_str() {
                    if let Ok((user_id, iota_id, _, _, _, _, _, _, _, _, _, _)) =
                        get_by_username(username).await
                    {
                        if let Ok((iota_id, public_key)) = get_iota_by_id(iota_id).await {
                            let mut response =
                                CommunicationValue::new(CommunicationType::get_iota_data)
                                    .add_data_str(DataTypes::public_key, public_key)
                                    .add_data(
                                        DataTypes::user_id,
                                        JsonValue::Number(Number::from(user_id)),
                                    )
                                    .add_data_str(DataTypes::username, username.to_string())
                                    .add_data(
                                        DataTypes::iota_id,
                                        JsonValue::Number(Number::from(iota_id)),
                                    );

                            let iota_connections =
                                user_online_tracker::get_iota_omikron_connections(iota_id)
                                    .await
                                    .unwrap_or_default();

                            response = response.add_data(
                                DataTypes::omikron_connections,
                                JsonValue::Array(
                                    iota_connections
                                        .iter()
                                        .map(|&id| JsonValue::Number(Number::from(id)))
                                        .collect(),
                                ),
                            );
                            self.send_message(&response).await;
                            return;
                        }
                    }
                }
            }
            let response =
                CommunicationValue::new(CommunicationType::error_not_found).with_id(cv.get_id());
            self.send_message(&response).await;

            return;
        }
        if cv.is_type(CommunicationType::change_user_data) {}
        if cv.is_type(CommunicationType::change_iota_data) {}
    }

    async fn send_error_response(&self, message_id: &Uuid, error_type: CommunicationType) {
        let error = CommunicationValue::new(error_type).with_id(*message_id);
        self.send_message(&error).await;
    }
    pub async fn close(&self) {
        let mut sender = self.sender.write().await;
        if self.is_identified().await {
            let _ = sql::set_omikron_active(self.get_omikron_id().await, false);
        }
        let _ = sender.close().await;
    }
    pub async fn handle_close(self: Arc<Self>) {
        if self.is_identified().await {
            let omikron_id = self.get_omikron_id().await;
            if omikron_id != 0 {
                user_online_tracker::untrack_omikron(omikron_id).await;
            }
        }
    }

    async fn handle_ping(&self, cv: CommunicationValue) {
        if let Some(last_ping) = cv.get_data(DataTypes::last_ping) {
            if let Ok(ping_val) = last_ping.to_string().parse::<i64>() {
                let mut ping_guard = self.ping.write().await;
                *ping_guard = ping_val;
            }
        }

        let response = CommunicationValue::new(CommunicationType::pong).with_id(cv.get_id());

        self.send_message(&response).await;
    }
}
