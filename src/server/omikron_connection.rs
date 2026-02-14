use crate::data::communication::{CommunicationType, CommunicationValue, DataTypes};
use crate::server::omikron_manager;
use crate::server::short_link::add_short_link;
use crate::server::socket::{WsSendMessage, WsSession};
use crate::sql::connection_status::UserStatus;
use crate::sql::sql::{self, get_by_user_id, get_by_username, get_iota_by_id, get_omikron_by_id};
use crate::sql::user_online_tracker::{self};
use crate::util::crypto_helper::encrypt;
use crate::util::logger::PrintType;
use crate::{get_private_key, get_public_key, log_in, log_out};
use actix::Addr;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use dashmap::DashMap;
use json::JsonValue;
use json::number::Number;
use rand::Rng;
use rand::distributions::Alphanumeric;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use x448::PublicKey;

pub struct OmikronConnection {
    pub ws_addr: Arc<RwLock<Addr<WsSession>>>,
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
    pub fn new(ws_addr: Addr<WsSession>) -> Arc<Self> {
        Arc::new(Self {
            ws_addr: Arc::new(RwLock::new(ws_addr)),
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
        let text = cv.to_json().to_string();

        if !cv.is_type(CommunicationType::pong) {
            log_out!(
                *self.omikron_id.read().await,
                PrintType::Omikron,
                "{}",
                text
            );
        }

        self.ws_addr.read().await.do_send(WsSendMessage(text));
    }

    pub async fn get_omikron_id(&self) -> i64 {
        *self.omikron_id.read().await
    }
    pub async fn is_identified(&self) -> bool {
        *self.identified.read().await && *self.challenged.read().await
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

        if !self.is_identified().await {
            let identified = *self.identified.read().await;
            let challenged = *self.challenged.read().await;

            if !identified && cv.is_type(CommunicationType::identification) {
                let omikron_id = cv
                    .get_data(DataTypes::omikron)
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                let (public_key, _) = match get_omikron_by_id(omikron_id).await {
                    Ok(v) => v,
                    Err(e) => {
                        self.send_message(
                            &CommunicationValue::new(CommunicationType::error_not_authenticated)
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::error_type, e.to_string()),
                        )
                        .await;
                        return;
                    }
                };

                let pub_key_bytes = match STANDARD.decode(&public_key) {
                    Ok(b) => b,
                    Err(_) => {
                        self.send_error_response(
                            &cv.get_id(),
                            CommunicationType::error_invalid_omikron_id,
                        )
                        .await;
                        return;
                    }
                };

                let omikron_pub_key = match PublicKey::from_bytes(&pub_key_bytes) {
                    Some(k) => k,
                    _ => {
                        self.send_error_response(
                            &cv.get_id(),
                            CommunicationType::error_invalid_public_key,
                        )
                        .await;
                        return;
                    }
                };

                let challenge: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(32)
                    .map(char::from)
                    .collect();

                *self.omikron_id.write().await = omikron_id;
                *self.challenge.write().await = challenge.clone();
                *self.pub_key.write().await = Some(pub_key_bytes);
                *self.identified.write().await = true;

                let encrypted =
                    encrypt(get_private_key(), omikron_pub_key, &challenge).unwrap_or_default();

                let response = CommunicationValue::new(CommunicationType::challenge)
                    .with_id(cv.get_id())
                    .add_data_str(
                        DataTypes::public_key,
                        STANDARD.encode(get_public_key().as_bytes()),
                    )
                    .add_data_str(DataTypes::challenge, encrypted);

                self.send_message(&response).await;
                return;
            }

            // ──────────────────────────────
            // Challenge response
            // ──────────────────────────────
            if identified && !challenged && cv.is_type(CommunicationType::challenge_response) {
                let client_response = cv
                    .get_data(DataTypes::challenge)
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if client_response == *self.challenge.read().await {
                    *self.challenged.write().await = true;
                    omikron_manager::add_omikron(self.clone()).await;
                    self.send_message(
                        &CommunicationValue::new(CommunicationType::identification_response)
                            .with_id(cv.get_id())
                            .add_data(DataTypes::accepted, JsonValue::Boolean(true)),
                    )
                    .await;
                    log_in!(PrintType::Omega, "Omikron Connected");
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

            self.send_error_response(&cv.get_id(), CommunicationType::error_not_authenticated)
                .await;
            self.close().await;
        }
        let omikron_id = self.get_omikron_id().await;

        if cv.is_type(CommunicationType::shorten_link) {
            if let Some(link) = cv.get_data(DataTypes::link) {
                if let Some(link) = link.as_str() {
                    if let Ok(short_link) = add_short_link(link).await {
                        let response = CommunicationValue::new(CommunicationType::shorten_link)
                            .with_id(cv.get_id())
                            .add_data(DataTypes::link, JsonValue::String(short_link));
                        self.send_message(&response).await;
                        return;
                    }
                }
            }
        }

        // ONLINE STATUS TRACKING
        if cv.is_type(CommunicationType::user_connected) {
            if let Some(user_id) = cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()) {
                user_online_tracker::track_user_status(user_id, UserStatus::online, omikron_id);
            }
            return;
        }
        if cv.is_type(CommunicationType::user_disconnected) {
            if let Some(user_id) = cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()) {
                if let Some(status) = user_online_tracker::get_user_status(user_id) {
                    user_online_tracker::track_user_status(
                        user_id,
                        UserStatus::user_offline,
                        status.omikron_id,
                    );
                }
            }
            return;
        }

        if cv.is_type(CommunicationType::iota_connected) {
            log_in!(PrintType::Omega, "IOTA connected");
            if let Some(iota_id) = cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64()) {
                user_online_tracker::track_iota_connection(iota_id, omikron_id, true);
                let mut user_ids = JsonValue::Array(Vec::new());
                if let Ok(users) = sql::get_users_by_iota_id(iota_id).await {
                    for user in users {
                        let _ = user_ids.push(JsonValue::from(user.0));
                        user_online_tracker::track_user_status(
                            user.0,
                            UserStatus::user_offline,
                            omikron_id,
                        );
                    }
                } else {
                    log_in!(PrintType::General, "SQL error, when loading users for IOTA");
                }
                self.send_message(
                    &CommunicationValue::new(CommunicationType::iota_user_data)
                        .with_id(cv.get_id())
                        .add_data(DataTypes::user_ids, user_ids),
                )
                .await;
            } else {
                log_in!(PrintType::General, "No IOTA ID found");
            }
            return;
        }
        if cv.is_type(CommunicationType::iota_disconnected) {
            if let Some(iota_id) = cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64()) {
                let iota_offline =
                    user_online_tracker::untrack_iota_connection(iota_id, omikron_id);
                if iota_offline {
                    if let Ok(users) = sql::get_users_by_iota_id(iota_id).await {
                        let user_ids: Vec<i64> = users.iter().map(|u| u.0).collect();
                        user_online_tracker::untrack_many_users(&user_ids);
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
                            UserStatus::online,
                            omikron_id,
                        );
                    }
                }
            }
            if let Some(json::JsonValue::Array(iota_ids)) =
                cv.get_data(DataTypes::iota_ids).cloned()
            {
                for iota_id_json in iota_ids {
                    if let Some(iota_id) = iota_id_json.as_i64() {
                        user_online_tracker::track_iota_connection(iota_id, omikron_id, true);
                    }
                }
            }
            return;
        }

        // DATA RETURN
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
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::username, username)
                                .add_data_str(DataTypes::public_key, public_key)
                                .add_data(DataTypes::user_id, JsonValue::Number(Number::from(id)))
                                .add_data(
                                    DataTypes::iota_id,
                                    JsonValue::Number(Number::from(iota_id)),
                                )
                                .add_data(
                                    DataTypes::sub_level,
                                    JsonValue::Number(Number::from(sub_level)),
                                )
                                .add_data(
                                    DataTypes::sub_end,
                                    JsonValue::Number(Number::from(sub_end)),
                                );

                        if let Some(display) = display {
                            response = response.add_data_str(DataTypes::display, display);
                        }
                        if let Some(status) = status {
                            response = response.add_data_str(DataTypes::status, status);
                        }
                        if let Some(about) = about {
                            response = response.add_data_str(DataTypes::about, about);
                        }
                        if let Some(avatar) = avatar {
                            response =
                                response.add_data_str(DataTypes::avatar, STANDARD.encode(avatar));
                        }

                        let user_status = user_online_tracker::get_user_status(id);
                        let iota_connections =
                            user_online_tracker::get_iota_omikron_connections(iota_id)
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
                                JsonValue::String(UserStatus::iota_offline.to_string()),
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
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::username, username)
                                .add_data_str(DataTypes::public_key, public_key)
                                .add_data(DataTypes::user_id, JsonValue::Number(Number::from(id)))
                                .add_data(
                                    DataTypes::iota_id,
                                    JsonValue::Number(Number::from(iota_id)),
                                )
                                .add_data(
                                    DataTypes::sub_level,
                                    JsonValue::Number(Number::from(sub_level)),
                                )
                                .add_data(
                                    DataTypes::sub_end,
                                    JsonValue::Number(Number::from(sub_end)),
                                );

                        if let Some(display) = display {
                            response = response.add_data_str(DataTypes::display, display);
                        }
                        if let Some(status) = status {
                            response = response.add_data_str(DataTypes::status, status);
                        }
                        if let Some(about) = about {
                            response = response.add_data_str(DataTypes::about, about);
                        }
                        if let Some(avatar) = avatar {
                            response =
                                response.add_data_str(DataTypes::avatar, STANDARD.encode(avatar));
                        }

                        let user_status = user_online_tracker::get_user_status(id);
                        let iota_connections =
                            user_online_tracker::get_iota_omikron_connections(iota_id)
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
                                JsonValue::String(UserStatus::iota_offline.to_string()),
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
            if let Some(iota_id) = cv.get_data(DataTypes::iota_id) {
                if let Some(iota_id) = iota_id.as_i64() {
                    if let Ok((iota_id, public_key)) = get_iota_by_id(iota_id).await {
                        let mut response =
                            CommunicationValue::new(CommunicationType::get_iota_data)
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::public_key, public_key)
                                .add_data(DataTypes::iota_id, JsonValue::from(iota_id));

                        let iota_connections =
                            user_online_tracker::get_iota_omikron_connections(iota_id)
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
                                    .with_id(cv.get_id())
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
                                    .with_id(cv.get_id())
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

        // REGISTERING
        if cv.is_type(CommunicationType::get_register) {
            let register_id = sql::get_register_id().await;
            let response = CommunicationValue::new(CommunicationType::get_register)
                .with_id(cv.get_id())
                .add_data(
                    DataTypes::user_id,
                    JsonValue::Number(Number::from(register_id)),
                );
            self.send_message(&response).await;
            return;
        }
        if cv.is_type(CommunicationType::complete_register_iota) {
            let iota_id_opt = cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64());

            if let Some(public_key) = cv.get_data(DataTypes::public_key).and_then(|v| v.as_str()) {
                if let Some(iota_id) = iota_id_opt {
                    match sql::register_complete_iota(iota_id, public_key.to_string()).await {
                        Ok(_) => {
                            let response = CommunicationValue::new(CommunicationType::success)
                                .with_id(cv.get_id());
                            self.send_message(&response).await;
                        }
                        Err(e) => {
                            self.send_message(
                                &CommunicationValue::new(CommunicationType::error)
                                    .with_id(cv.get_id())
                                    .add_data_str(DataTypes::error_type, e.to_string()),
                            )
                            .await;
                        }
                    }
                } else {
                    // New logic to create iota and return id
                    match sql::create_new_iota(public_key.to_string()).await {
                        Ok(new_iota_id) => {
                            let response =
                                CommunicationValue::new(CommunicationType::complete_register_iota)
                                    .with_id(cv.get_id())
                                    .add_data(DataTypes::iota_id, JsonValue::from(new_iota_id));
                            self.send_message(&response).await;
                        }
                        Err(e) => {
                            self.send_message(
                                &CommunicationValue::new(CommunicationType::error)
                                    .with_id(cv.get_id())
                                    .add_data_str(DataTypes::error_type, e.to_string()),
                            )
                            .await;
                        }
                    }
                }
            } else {
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                    .await;
            }
            return;
        }
        if cv.is_type(CommunicationType::complete_register_user) {
            if let (
                Some(user_id),
                Some(username),
                Some(public_key),
                Some(iota_id),
                Some(reset_token),
            ) = (
                cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()),
                cv.get_data(DataTypes::username).and_then(|v| v.as_str()),
                cv.get_data(DataTypes::public_key).and_then(|v| v.as_str()),
                cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64()),
                cv.get_data(DataTypes::reset_token).and_then(|v| v.as_str()),
            ) {
                match sql::register_complete_user(
                    user_id,
                    username.to_string(),
                    public_key.to_string(),
                    iota_id,
                    reset_token.to_string(),
                )
                .await
                {
                    Ok(_) => {
                        let response = CommunicationValue::new(CommunicationType::success)
                            .with_id(cv.get_id());
                        self.send_message(&response).await;
                    }
                    Err(e) => {
                        self.send_message(
                            &CommunicationValue::new(CommunicationType::error)
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::error_type, e.to_string()),
                        )
                        .await;
                    }
                }
            } else {
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                    .await;
            }
            return;
        }

        // CHANGING DATA
        if cv.is_type(CommunicationType::change_user_data) {
            if let Some(user_id) = cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()) {
                let mut success = true;
                let mut error_message = String::new();

                if let Some(username) = cv.get_data(DataTypes::username).and_then(|v| v.as_str()) {
                    if let Err(e) = sql::change_username(user_id, username.to_string()).await {
                        success = false;
                        error_message = e.to_string();
                    }
                }
                if let Some(display) = cv.get_data(DataTypes::display).and_then(|v| v.as_str()) {
                    if let Err(e) = sql::change_display_name(user_id, display.to_string()).await {
                        success = false;
                        error_message = e.to_string();
                    }
                }
                if let Some(avatar) = cv.get_data(DataTypes::avatar).and_then(|v| v.as_str()) {
                    if let Err(e) = sql::change_avatar(user_id, avatar.to_string()).await {
                        success = false;
                        error_message = e.to_string();
                    }
                }
                if let Some(about) = cv.get_data(DataTypes::about).and_then(|v| v.as_str()) {
                    if let Err(e) = sql::change_about(user_id, about.to_string()).await {
                        success = false;
                        error_message = e.to_string();
                    }
                }
                if let Some(status) = cv.get_data(DataTypes::status).and_then(|v| v.as_str()) {
                    if let Err(e) = sql::change_status(user_id, status.to_string()).await {
                        success = false;
                        error_message = e.to_string();
                    }
                }
                if let Some(public_key) =
                    cv.get_data(DataTypes::public_key).and_then(|v| v.as_str())
                {
                    if let Some(private_key_hash) = cv
                        .get_data(DataTypes::private_key_hash)
                        .and_then(|v| v.as_str())
                    {
                        if let Err(e) = sql::change_keys(
                            user_id,
                            public_key.to_string(),
                            private_key_hash.to_string(),
                        )
                        .await
                        {
                            success = false;
                            error_message = e.to_string();
                        }
                    } else {
                        success = false;
                        error_message =
                            "private_key_hash is required when changing public_key".to_string();
                    }
                }

                if success {
                    let response =
                        CommunicationValue::new(CommunicationType::success).with_id(cv.get_id());
                    self.send_message(&response).await;
                } else {
                    self.send_message(
                        &CommunicationValue::new(CommunicationType::error)
                            .with_id(cv.get_id())
                            .add_data_str(DataTypes::error_type, error_message),
                    )
                    .await;
                }
            } else {
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                    .await;
            }
            return;
        }
        if cv.is_type(CommunicationType::change_iota_data) {
            if let (Some(user_id), Some(iota_id), Some(reset_token), Some(new_token)) = (
                cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()),
                cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64()),
                cv.get_data(DataTypes::reset_token).and_then(|v| v.as_str()),
                cv.get_data(DataTypes::new_token).and_then(|v| v.as_str()),
            ) {
                match sql::get_by_user_id(user_id).await {
                    Ok(user) => {
                        let current_token = user.11;
                        if current_token == reset_token {
                            let mut success = true;
                            let mut error_message = String::new();
                            if let Err(e) = sql::change_iota_id(user_id, iota_id).await {
                                success = false;
                                error_message = e.to_string();
                            }
                            if success {
                                if let Err(e) =
                                    sql::change_token(user_id, new_token.to_string()).await
                                {
                                    success = false;
                                    error_message = e.to_string();
                                }
                            }

                            if success {
                                let response = CommunicationValue::new(CommunicationType::success)
                                    .with_id(cv.get_id());
                                self.send_message(&response).await;
                            } else {
                                self.send_message(
                                    &CommunicationValue::new(CommunicationType::error)
                                        .with_id(cv.get_id())
                                        .add_data_str(DataTypes::error_type, error_message),
                                )
                                .await;
                            }
                        } else {
                            self.send_error_response(
                                &cv.get_id(),
                                CommunicationType::error_invalid_challenge,
                            )
                            .await;
                        }
                    }
                    Err(_) => {
                        self.send_error_response(&cv.get_id(), CommunicationType::error_not_found)
                            .await;
                    }
                }
            } else {
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                    .await;
            }
            return;
        }
        if cv.is_type(CommunicationType::delete_user) {
            if let Some(user_id) = cv.get_data(DataTypes::user_id).and_then(|v| v.as_i64()) {
                match sql::delete_user(user_id).await {
                    Ok(_) => {
                        let response = CommunicationValue::new(CommunicationType::success)
                            .with_id(cv.get_id());
                        self.send_message(&response).await;
                    }
                    Err(e) => {
                        self.send_message(
                            &CommunicationValue::new(CommunicationType::error)
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::error_type, e.to_string()),
                        )
                        .await;
                    }
                }
            } else {
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                    .await;
            }
            return;
        }
        if cv.is_type(CommunicationType::delete_iota) {
            if let Some(iota_id) = cv.get_data(DataTypes::iota_id).and_then(|v| v.as_i64()) {
                match sql::delete_iota(iota_id).await {
                    Ok(_) => {
                        let response = CommunicationValue::new(CommunicationType::success)
                            .with_id(cv.get_id());
                        self.send_message(&response).await;
                    }
                    Err(e) => {
                        self.send_message(
                            &CommunicationValue::new(CommunicationType::error)
                                .with_id(cv.get_id())
                                .add_data_str(DataTypes::error_type, e.to_string()),
                        )
                        .await;
                    }
                }
            } else {
                self.send_error_response(&cv.get_id(), CommunicationType::error_invalid_data)
                    .await;
            }
            return;
        }

        // NOTIFICATIONS
        if cv.is_type(CommunicationType::get_notifications) {
            if let Some(JsonValue::Number(user_id)) = cv.get_data(DataTypes::user_id) {
                if let Ok(notifications) =
                    sql::get_notifications(user_id.as_fixed_point_i64(0).unwrap()).await
                {
                    let mut json_array = Vec::new();
                    for (sender, amount) in notifications {
                        let mut obj = JsonValue::new_object();
                        let _ = obj.insert("sender", JsonValue::from(sender));
                        let _ = obj.insert("amount", JsonValue::from(amount));
                        json_array.push(obj);
                    }
                    let response = CommunicationValue::new(CommunicationType::get_notifications)
                        .with_id(cv.get_id())
                        .add_array(DataTypes::notifications, json_array);
                    self.send_message(&response).await;
                }
            }
        }
        if cv.is_type(CommunicationType::read_notification) {
            if let (Some(JsonValue::Number(user_id)), Some(JsonValue::Number(other_id))) = (
                cv.get_data(DataTypes::receiver_id),
                cv.get_data(DataTypes::sender_id),
            ) {
                if let Ok(_) = sql::read_notification(
                    user_id.as_fixed_point_i64(0).unwrap(),
                    other_id.as_fixed_point_i64(0).unwrap(),
                )
                .await
                {
                    let response = CommunicationValue::new(CommunicationType::read_notification)
                        .with_id(cv.get_id());
                    self.send_message(&response).await;
                }
            }
        }
        if cv.is_type(CommunicationType::push_notification) {
            if let (Some(JsonValue::Number(user_id)), Some(JsonValue::Number(other_id))) = (
                cv.get_data(DataTypes::receiver_id),
                cv.get_data(DataTypes::sender_id),
            ) {
                if let Ok(_) = sql::add_notification(
                    user_id.as_fixed_point_i64(0).unwrap(),
                    other_id.as_fixed_point_i64(0).unwrap(),
                )
                .await
                {
                    let response = CommunicationValue::new(CommunicationType::push_notification)
                        .with_id(cv.get_id());
                    self.send_message(&response).await;
                }
            }
        }
    }

    async fn send_error_response(&self, message_id: &Uuid, error_type: CommunicationType) {
        let error = CommunicationValue::new(error_type).with_id(*message_id);
        self.send_message(&error).await;
    }
    pub async fn close(&self) {
        if self.is_identified().await {
            let omikron_id = self.get_omikron_id().await;
            if omikron_id != 0 {
                log_in!(PrintType::Omega, "Omikron Disconnected");
                omikron_manager::remove_omikron(omikron_id).await;
                user_online_tracker::untrack_omikron(omikron_id).await;
            }
        }
    }
    pub async fn handle_close(self: Arc<Self>) {
        if self.is_identified().await {
            let omikron_id = self.get_omikron_id().await;
            if omikron_id != 0 {
                log_in!(PrintType::Omega, "Omikron Disconnected");
                omikron_manager::remove_omikron(omikron_id).await;
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
