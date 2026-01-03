use crate::data::communication::{CommunicationType, CommunicationValue, DataTypes};
use crate::get_public_key;
use crate::sql::sql::get_omikron_by_id;
use crate::util::crypto_helper::encrypt;
use crate::{get_private_key, log_in_from, log_out_from};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use dashmap::DashMap;
use futures::SinkExt;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use json::JsonValue;
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
    pub async fn send_message(&self, message: &CommunicationValue) {
        let mut sender = self.sender.write().await;
        let message_text = Message::Text(Utf8Bytes::from(message.to_json().to_string()));
        log_out_from!(*self.omikron_id.read().await, "{}", message_text);
        sender.send(message_text).await.unwrap();
    }
    pub async fn get_user_id(&self) -> i64 {
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

        log_in_from!(*self.omikron_id.read().await, "{}", message);

        if !*self.identified.read().await && cv.is_type(CommunicationType::identification) {
            let omikron_id = cv
                .get_data(DataTypes::omikron)
                .unwrap_or(&JsonValue::Null)
                .as_i64()
                .unwrap_or(0);
            if let Ok((_, public_key, _)) = get_omikron_by_id(omikron_id).await {
                // Generate Challenge, encrypt it and send it to the omikron
                *self.omikron_id.write().await = omikron_id;
                *self.identified.write().await = true;

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
                            CommunicationType::error_invalid_user_id,
                        )
                        .await;
                        return;
                    }
                };
                *self.pub_key.write().await = Some(user_public_key_bytes.clone());

                let omikron_pub_key: PublicKey = match PublicKey::from_bytes(&user_public_key_bytes)
                {
                    Some(key) => key,
                    None => {
                        self.send_error_response(
                            &cv.get_id(),
                            CommunicationType::error_invalid_user_id,
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
                // prepare Challenge Response handling
                self.waiting_tasks.insert(
                    cv.get_id(),
                    Box::new(
                        |selfc: Arc<OmikronConnection>, cv: CommunicationValue| -> bool {
                            tokio::spawn(async move {
                                let client_challenge_response_b64 =
                                    match cv.get_data(DataTypes::challenge) {
                                        Some(data) => data.to_string(),
                                        None => {
                                            selfc
                                                .send_error_response(
                                                    &cv.get_id(),
                                                    CommunicationType::error,
                                                )
                                                .await;
                                            return;
                                        }
                                    };

                                let challenge_response_bytes =
                                    match STANDARD.decode(&client_challenge_response_b64) {
                                        Ok(bytes) => bytes,
                                        Err(_) => {
                                            selfc
                                                .send_error_response(
                                                    &cv.get_id(),
                                                    CommunicationType::error,
                                                )
                                                .await;
                                            return;
                                        }
                                    };

                                if challenge_response_bytes.len() < 12 {
                                    selfc
                                        .send_error_response(&cv.get_id(), CommunicationType::error)
                                        .await;
                                    return;
                                }

                                let client_response = cv
                                    .get_data(DataTypes::challenge)
                                    .unwrap_or(&JsonValue::Null)
                                    .as_str()
                                    .unwrap_or("");

                                let expected_challenge = selfc.challenge.read().await.clone();

                                if client_response != expected_challenge {
                                    selfc
                                        .send_error_response(
                                            &cv.get_id(),
                                            CommunicationType::error_invalid_challenge,
                                        )
                                        .await;
                                    selfc.close().await;
                                    return;
                                }

                                *selfc.challenged.write().await = true;

                                let response = CommunicationValue::new(
                                    CommunicationType::identification_response,
                                )
                                .with_id(cv.get_id());

                                selfc.send_message(&response).await;
                                return;
                            });
                            return true;
                        },
                    ),
                );
            } else {
                self.send_error_response(&cv.get_id(), CommunicationType::error_not_found)
                    .await;
            }
            return;
        }

        if self.waiting_tasks.contains_key(&cv.get_id()) {
            let (_, task) = self.waiting_tasks.remove(&cv.get_id()).unwrap();
            let _ = task(self.clone(), cv.clone());
        }

        if !self.is_identified().await {
            self.send_error_response(&cv.get_id(), CommunicationType::error_not_found)
                .await;
            return;
        }
    }

    async fn send_error_response(&self, message_id: &Uuid, error_type: CommunicationType) {
        let error = CommunicationValue::new(error_type).with_id(*message_id);
        self.send_message(&error).await;
    }
    pub async fn close(&self) {
        let mut sender = self.sender.write().await;
        let _ = sender.close().await;
    }
    pub async fn handle_close(self: Arc<Self>) {
        if self.is_identified().await {
            if self.get_user_id().await != 0 {}
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
