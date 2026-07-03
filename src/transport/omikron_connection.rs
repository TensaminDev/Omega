use crate::{
    get_keyring, get_public_key_bundle, log, log_cv_in, log_cv_out, log_err, log_in,
    server::short_link::add_short_link,
    sql::{
        connection_status::UserStatus,
        sql::{self, get_by_user_id, get_by_username, get_iota_by_id, get_omikron_by_id},
        user_online_tracker::{self},
    },
    transport::omikron_manager,
    util::{file_util::load_file_vec, logger::PrintType},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use dashmap::DashMap;
use mtp::{codec::{CommunicationType, CommunicationValue, DataType, DataValue}, host::{HostConfig, MTPHost}};
use mtp::transport::{Host, Policy, Receiver, SendMode, Sender, host};
use rand::{Rng, distributions::Alphanumeric};
use std::net::{IpAddr, Ipv4Addr};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::interval,
};
use x448::PublicKey;

// ============================================================================
// Configuration
// ============================================================================

const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
const MAX_WAITING_AGE: Duration = Duration::from_secs(60);

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum OmikronError {
    #[error("Not connected")]
    NotConnected,
    #[error("Not authenticated")]
    NotAuthenticated,
    #[error("Invalid response")]
    InvalidResponse,
    #[error("Authentication failed")]
    AuthenticationFailed,
    #[error("SQL error: {0}")]
    Sql(String),
    #[error("Send error: {0}")]
    Send(String),
}

pub type OmikronResult<T> = Result<T, OmikronError>;

// ============================================================================
// Waiting Task System (Preserved from original)
// ============================================================================

pub struct WaitingTask {
    pub task: Box<dyn Fn(Arc<OmikronConnection>, CommunicationValue) -> bool + Send + Sync>,
    pub inserted_at: Instant,
}

// ============================================================================
// Connection State
// ============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthState {
    Unauthenticated,
    Identified { omikron_id: i64 },
    Authenticated { omikron_id: i64 },
}

impl AuthState {
    fn is_authenticated(&self) -> bool {
        match self {
            AuthState::Authenticated { omikron_id } => true,
            _ => false,
        }
    }

    fn omikron_id(&self) -> Option<i64> {
        match self {
            AuthState::Identified { omikron_id } | AuthState::Authenticated { omikron_id } => {
                Some(*omikron_id)
            }
            _ => None,
        }
    }
}

// ============================================================================
// Omikron Connection (mtp/QUIC-based)
// ============================================================================

pub struct OmikronConnection {
    id: u64,
    sender: Mutex<Option<Sender>>,
    state: RwLock<AuthState>,
    challenge: RwLock<String>,
    pub_key: RwLock<Option<Vec<u8>>>,
    pub ping: RwLock<i64>,
    waiting_tasks: DashMap<u32, WaitingTask>,
    cleanup_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for OmikronConnection {
    fn drop(&mut self) {
        if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
    }
}

impl OmikronConnection {
    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    pub fn new(sender: Sender) -> Arc<Self> {
        let conn = Arc::new(Self {
            id: rand::random(),
            sender: Mutex::new(Some(sender)),
            state: RwLock::new(AuthState::Unauthenticated),
            challenge: RwLock::new(String::new()),
            pub_key: RwLock::new(None),
            ping: RwLock::new(-1),
            waiting_tasks: DashMap::new(),
            cleanup_handle: std::sync::Mutex::new(None),
        });

        conn
    }

    // -------------------------------------------------------------------------
    // Main Handler Loop
    // -------------------------------------------------------------------------

    pub async fn handle(self: Arc<Self>, receiver: &mut Receiver) {
        log_in!(
            self.id as i64,
            PrintType::Omega,
            "Omikron connection started"
        );

        // Start cleanup task
        let cleanup_conn = self.clone();
        let cleanup_handle = tokio::spawn(async move {
            let mut ticker = interval(CLEANUP_INTERVAL);
            loop {
                ticker.tick().await;
                cleanup_conn
                    .waiting_tasks
                    .retain(|_, v| v.inserted_at.elapsed() < MAX_WAITING_AGE);
            }
        });
        *self.cleanup_handle.lock().unwrap() = Some(cleanup_handle);

        while let Ok(cv) = receiver.receive().await {
            if let Err(e) = self.clone().process_message(cv).await {
                log_err!(0, PrintType::Omega, "Error processing message: {}", e);
                if matches!(e, OmikronError::NotConnected) {
                    break;
                }
            }
        }

        self.clone().cleanup().await;
        log_in!(
            self.id as i64,
            PrintType::Omega,
            "Omikron connection closed"
        );
    }

    // -------------------------------------------------------------------------
    // Message Processing
    // -------------------------------------------------------------------------

    async fn process_message(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        if !cv.is_type(CommunicationType::Pong) && !cv.is_type(CommunicationType::Ping) {
            log_cv_in!(PrintType::Omikron, &cv);
        }

        let msg_id = cv.get_id();

        // Check waiting tasks first (response to previous request)
        if let Some((_, task)) = self.waiting_tasks.remove(&msg_id) {
            let _ = (task.task)(self.clone(), cv);
            return Ok(());
        }

        // Handle ping regardless of auth state
        if cv.is_type(CommunicationType::Ping) {
            return self.handle_ping(cv).await;
        }

        let current_state = *self.state.read().await;
        // Route based on authentication state
        match current_state {
            AuthState::Unauthenticated => self.clone().handle_unauthenticated(cv).await,
            AuthState::Identified { .. } => self.clone().handle_identified(cv).await,
            AuthState::Authenticated { omikron_id } => {
                self.clone().handle_authenticated(cv, omikron_id).await
            }
        }
    }

    // -------------------------------------------------------------------------
    // Authentication Handlers
    // -------------------------------------------------------------------------

    async fn handle_unauthenticated(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        if !cv.is_type(CommunicationType::Identification) {
            let _ = self
                .send_error_response(cv.get_id(), CommunicationType::ErrorNotAuthenticated)
                .await;
            return Err(OmikronError::NotAuthenticated);
        }

        // Extract omikron ID
        let omikron_id = cv
            .get_data(DataType::OmikronId)
            .as_number()
            .ok_or(OmikronError::InvalidResponse)?;
        log!("Omikron {:?} connected", omikron_id);

        // Lookup omikron in database
        let (public_key, _) = get_omikron_by_id(omikron_id as i64)
            .await
            .map_err(|e| OmikronError::Sql(e.to_string()))?;

        log!("Got public Key");

        let pub_key_bytes = STANDARD
            .decode(&public_key)
            .map_err(|_| OmikronError::AuthenticationFailed)?;

        let pub_key_bytes_clone = pub_key_bytes.clone();
        let omikron_pub_key = PublicKey::from_bytes(&pub_key_bytes_clone)
            .ok_or(OmikronError::AuthenticationFailed)?;

        log!("Decoded public Key");

        // Generate challenge
        let challenge: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();

        log!("Generated Challenge");

        *self.challenge.write().await = challenge.clone();

        log!("Stored Challenge");

        *self.pub_key.write().await = Some(pub_key_bytes);

        log!("Stored Pubkey");

        *self.state.write().await = AuthState::Identified {
            omikron_id: omikron_id as i64,
        };

        log!("Stored State");

        let challenge_clone: String = challenge.clone();
        let private_key = get_keyring();
        let public_key_for_encrypt = omikron_pub_key;

        let encrypted = tokio::task::spawn_blocking(move || {
            challenge_clone
            encrypt(private_key, public_key_for_encrypt, &challenge_clone)
                .map_err(|_| OmikronError::AuthenticationFailed)
        })
        .await
        .map_err(|_| OmikronError::AuthenticationFailed)??;

        log!("Encrypted Challenge");

        // Send challenge response
        let response = CommunicationValue::new(CommunicationType::Challenge)
            .with_id(cv.get_id())
            .add_typed_default(
                DataType::PublicKey,
                DataValue::Str(STANDARD.encode(get_public_key_bundle().as_bytes())),
            )
            .add_typed_default(DataType::Content, DataValue::Str(encrypted));

        log!("Sending Challenge");
        self.send(&response).await
    }

    async fn handle_identified(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        if !cv.is_type(CommunicationType::ChallengeResponse) {
            let _ = self
                .send_error_response(cv.get_id(), CommunicationType::ErrorNotAuthenticated)
                .await;
            return Err(OmikronError::NotAuthenticated);
        }

        let client_response = cv
            .get_data(DataType::Content)
            .as_str()
            .ok_or(OmikronError::InvalidResponse)?;

        let expected_challenge = self.challenge.read().await.clone();

        if client_response == expected_challenge {
            let omikron_id = self.state.read().await.omikron_id().unwrap_or(0);
            *self.state.write().await = AuthState::Authenticated { omikron_id };

            omikron_manager::add_omikron(self.clone()).await;

            let response = CommunicationValue::new(CommunicationType::IdentificationResponse)
                .with_id(cv.get_id())
                .add_typed_default(DataType::Accepted, DataValue::Bool(true));

            self.clone().send(&response).await?;
            log_in!(omikron_id, PrintType::Omega, "Omikron authenticated");
            Ok(())
        } else {
            let _ = self
                .send_error_response(cv.get_id(), CommunicationType::ErrorInvalidChallenge)
                .await;
            Err(OmikronError::AuthenticationFailed)
        }
    }

    // -------------------------------------------------------------------------
    // Authenticated Message Handlers
    // -------------------------------------------------------------------------

    async fn handle_authenticated(
        self: Arc<Self>,
        cv: CommunicationValue,
        omikron_id: i64,
    ) -> OmikronResult<()> {
        let comm_type = cv.get_comm_type_enum();
        match comm_type {
            // Link shortening
            Some(CommunicationType::ShortenLink) => self.handle_shorten_link(cv).await,

            // Online status tracking
            Some(CommunicationType::UserConnected) => {
                self.handle_user_connected(cv, omikron_id).await;
                Ok(())
            }
            Some(CommunicationType::UserDisconnected) => {
                self.handle_user_disconnected(cv, omikron_id).await;
                Ok(())
            }
            Some(CommunicationType::IotaConnected) => {
                self.handle_iota_connected(cv, omikron_id).await;
                Ok(())
            }
            Some(CommunicationType::IotaDisconnected) => {
                self.handle_iota_disconnected(cv, omikron_id).await;
                Ok(())
            }
            Some(CommunicationType::SyncClientIotaStatus) => {
                self.handle_sync_status(cv, omikron_id).await;
                Ok(())
            }

            Some(CommunicationType::GetUserData) => self.handle_get_user_data(cv).await,
            Some(CommunicationType::GetIotaData) => self.handle_get_iota_data(cv).await,

            Some(CommunicationType::GetRegister) => self.handle_get_register(cv).await,
            Some(CommunicationType::CompleteRegisterIota) => {
                self.handle_complete_register_iota(cv).await
            }
            Some(CommunicationType::CompleteRegisterUser) => {
                self.handle_complete_register_user(cv).await
            }

            Some(CommunicationType::ChangeUserData) => self.handle_change_user_data(cv).await,
            Some(CommunicationType::ChangeIotaData) => self.handle_change_iota_data(cv).await,
            Some(CommunicationType::DeleteUser) => self.handle_delete_user(cv).await,
            Some(CommunicationType::DeleteIota) => self.handle_delete_iota(cv).await,

            Some(CommunicationType::GetNotifications) => self.handle_get_notifications(cv).await,
            Some(CommunicationType::ReadNotification) => self.handle_read_notification(cv).await,
            Some(CommunicationType::PushNotification) => self.handle_push_notification(cv).await,
            Some(CommunicationType::GetStates) => self.handle_get_states(cv).await,

            _ => {
                log_err!(
                    0,
                    PrintType::Omega,
                    "Unknown message type: {:?}",
                    cv.get_type()
                );
                Ok(())
            }
        }
    }

    // -------------------------------------------------------------------------
    // Specific Handlers (ported from original WebSocket implementation)
    // -------------------------------------------------------------------------

    async fn handle_shorten_link(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        let link = cv
            .get_data(DataType::Link)
            .as_str()
            .ok_or(OmikronError::InvalidResponse)?;

        let short = add_short_link(link)
            .await
            .map_err(|_| OmikronError::Sql("Shortend link Error".to_string()))?;

        let response = CommunicationValue::new(CommunicationType::ShortenLink)
            .with_id(cv.get_id())
            .add_typed_default(DataType::Link, DataValue::Str(short));

        self.send(&response).await
    }

    async fn handle_user_connected(self: Arc<Self>, cv: CommunicationValue, omikron_id: i64) {
        log_in!(PrintType::Omega, "User connected");
        if let Some(user_id) = cv.get_data(DataType::UserId).as_number() {
            let status = cv
                .get_data(DataType::UserState)
                .as_str()
                .and_then(|s| UserStatus::from_str(s))
                .unwrap_or(UserStatus::user_online);
            user_online_tracker::track_user_status(user_id.try_into().unwrap(), status, omikron_id);
        }
    }

    async fn handle_user_disconnected(self: Arc<Self>, cv: CommunicationValue, _omikron_id: i64) {
        log_in!(PrintType::Omega, "User disconnected");
        if let Some(user_id) = cv.get_data(DataType::UserId).as_number() {
            if let Some(status) = user_online_tracker::get_user_status(user_id as i64) {
                user_online_tracker::track_user_status(
                    user_id as i64,
                    UserStatus::user_offline,
                    status.omikron_id,
                );
            }
        }
    }

    async fn handle_iota_connected(self: Arc<Self>, cv: CommunicationValue, omikron_id: i64) {
        log_in!(PrintType::Omega, "IOTA connected");
        let iota_id = match cv.get_data(DataType::IotaId).as_number() {
            Some(id) => id as i64,
            None => return,
        };
        user_online_tracker::track_iota_connection(iota_id, omikron_id, true);

        let mut user_ids = Vec::new();
        if let Ok(users) = sql::get_users_by_iota_id(iota_id.try_into().unwrap()).await {
            for (user_id, _, _, _, _, _, _, _, _, _, _, _) in users {
                user_ids.push(DataValue::SignedNumber(user_id.try_into().unwrap()));
                user_online_tracker::track_user_status(
                    user_id.try_into().unwrap(),
                    UserStatus::user_offline,
                    omikron_id,
                );
            }
        } else {
            log_in!(PrintType::General, "SQL error loading users for IOTA");
        }

        let response = CommunicationValue::new(CommunicationType::IotaUserData)
            .with_id(cv.get_id())
            .add_typed_default(DataType::UserIds, DataValue::Array(user_ids));

        let _ = self.send(&response).await;
    }

    async fn handle_iota_disconnected(self: Arc<Self>, cv: CommunicationValue, omikron_id: i64) {
        log_in!(PrintType::Omega, "IOTA disconnected");
        let iota_id = match cv.get_data(DataType::IotaId).as_number() {
            Some(id) => id as i64,
            None => return,
        };
        let iota_offline = user_online_tracker::untrack_iota_connection(iota_id, omikron_id);
        if iota_offline {
            if let Ok(users) = sql::get_users_by_iota_id(iota_id.try_into().unwrap()).await {
                let user_ids: Vec<i64> = users.iter().map(|u| u.0.try_into().unwrap()).collect();
                user_online_tracker::untrack_many_users(&user_ids);
            }
        }
    }

    async fn handle_sync_status(self: Arc<Self>, cv: CommunicationValue, omikron_id: i64) {
        if let DataValue::Array(user_ids) = cv.get_data(DataType::UserIds) {
            for user_id_val in user_ids {
                if let DataValue::SignedNumber(user_id) = user_id_val {
                    user_online_tracker::track_user_status(
                        *user_id as i64,
                        UserStatus::user_offline,
                        omikron_id,
                    );
                }
            }
        }

        if let DataValue::Array(iota_ids) = cv.get_data(DataType::IotaIds) {
            for iota_id_val in iota_ids {
                if let DataValue::SignedNumber(iota_id) = iota_id_val {
                    user_online_tracker::track_iota_connection(*iota_id as i64, omikron_id, true);
                }
            }
        }
    }

    async fn handle_get_user_data(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        // Try by user_id first
        if let Some(user_id) = cv.get_data(DataType::UserId).as_number() {
            if let Ok(user_data) = get_by_user_id(user_id as i64).await {
                let response = self
                    .clone()
                    .build_user_data_response(cv.get_id(), user_data)
                    .await;
                return self.send(&response).await;
            }
        }

        // Try by username
        if let Some(username) = cv.get_data(DataType::Username).as_str() {
            if let Ok(user_data) = get_by_username(username).await {
                let response = self
                    .clone()
                    .build_user_data_response(cv.get_id(), user_data)
                    .await;
                return self.send(&response).await;
            }
        }

        // Not found
        let response =
            CommunicationValue::new(CommunicationType::ErrorNotFound).with_id(cv.get_id());
        self.send(&response).await
    }

    async fn build_user_data_response(
        self: Arc<Self>,
        msg_id: u32,
        user: (
            i64,
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<Vec<u8>>,
            i32,
            i64,
            String,
            String,
            String,
        ),
    ) -> CommunicationValue {
        let (
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
        ) = user;

        let mut response = CommunicationValue::new(CommunicationType::GetUserData)
            .with_id(msg_id)
            .add_typed_default(DataType::Username, DataValue::Str(username.clone()))
            .add_typed_default(DataType::PublicKey, DataValue::Str(public_key))
            .add_typed_default(DataType::UserId, DataValue::SignedNumber(id.into()))
            .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()))
            .add_typed_default(
                DataType::SubLevel,
                DataValue::SignedNumber(sub_level as i128),
            )
            .add_typed_default(DataType::SubEnd, DataValue::SignedNumber(sub_end.into()));

        // Display name (fallback to username)
        let display_name = display.filter(|d| !d.is_empty()).unwrap_or(username);
        response = response.add_typed_default(DataType::Display, DataValue::Str(display_name));

        // Optional fields
        if let Some(s) = status.filter(|s| !s.is_empty()) {
            response = response.add_typed_default(DataType::Status, DataValue::Str(s));
        }
        if let Some(a) = about.filter(|a| !a.is_empty()) {
            response = response.add_typed_default(DataType::About, DataValue::Str(a));
        }
        if let Some(av) = avatar {
            response =
                response.add_typed_default(DataType::Avatar, DataValue::Str(STANDARD.encode(av)));
        }

        // Online status
        let user_status = user_online_tracker::get_user_status(id);
        let iota_connections =
            user_online_tracker::get_iota_omikron_connections(iota_id).unwrap_or_default();

        if let Some(us) = user_status {
            let display_status = if us.connection_type == UserStatus::user_invisible {
                UserStatus::user_offline
            } else {
                us.connection_type.clone()
            };
            response = response.add_typed_default(
                DataType::OnlineStatus,
                DataValue::Str(display_status.to_string()),
            );
            response = response.add_typed_default(
                DataType::OmikronId,
                DataValue::SignedNumber(us.omikron_id.into()),
            );
        } else {
            response = response.add_typed_default(
                DataType::OnlineStatus,
                DataValue::Str(UserStatus::iota_offline.to_string()),
            );
        }

        response = response.add_typed_default(
            DataType::OmikronConnections,
            DataValue::Array(
                iota_connections
                    .into_iter()
                    .map(|id| DataValue::SignedNumber(id as i128))
                    .collect(),
            ),
        );

        response
    }

    async fn handle_get_iota_data(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        // Try by iota_id
        if let Some(iota_id) = cv.get_data(DataType::IotaId).as_number() {
            if let Ok((iota_id, public_key)) = get_iota_by_id(iota_id as i64).await {
                let response = self
                    .clone()
                    .build_iota_data_response(cv.get_id(), iota_id, public_key, None, None)
                    .await;
                return self.send(&response).await;
            }
        }

        // Try by user_id
        if let Some(user_id) = cv.get_data(DataType::UserId).as_number() {
            if let Ok((_, iota_id, _, _, _, _, _, _, _, _, _, _)) =
                get_by_user_id(user_id as i64).await
            {
                if let Ok((iota_id, public_key)) = get_iota_by_id(iota_id).await {
                    let response = self
                        .clone()
                        .build_iota_data_response(
                            cv.get_id(),
                            iota_id,
                            public_key,
                            Some(user_id as i64),
                            None,
                        )
                        .await;
                    return self.send(&response).await;
                }
            }
        }

        // Try by username
        if let Some(username) = cv.get_data(DataType::Username).as_str() {
            if let Ok((user_id, iota_id, _, _, _, _, _, _, _, _, _, _)) =
                get_by_username(username).await
            {
                if let Ok((iota_id, public_key)) = get_iota_by_id(iota_id).await {
                    let response = self
                        .clone()
                        .build_iota_data_response(
                            cv.get_id(),
                            iota_id,
                            public_key,
                            Some(user_id),
                            Some(username.to_string()),
                        )
                        .await;
                    return self.send(&response).await;
                }
            }
        }

        let response =
            CommunicationValue::new(CommunicationType::ErrorNotFound).with_id(cv.get_id());
        self.send(&response).await
    }

    async fn build_iota_data_response(
        self: Arc<Self>,
        msg_id: u32,
        iota_id: i64,
        public_key: String,
        user_id: Option<i64>,
        username: Option<String>,
    ) -> CommunicationValue {
        let mut response = CommunicationValue::new(CommunicationType::GetIotaData)
            .with_id(msg_id)
            .add_typed_default(DataType::PublicKey, DataValue::Str(public_key))
            .add_typed_default(DataType::IotaId, DataValue::SignedNumber(iota_id.into()));

        if let Some(uid) = user_id {
            response =
                response.add_typed_default(DataType::UserId, DataValue::SignedNumber(uid.into()));
        }
        if let Some(uname) = username {
            response = response.add_typed_default(DataType::Username, DataValue::Str(uname));
        }

        let iota_connections =
            user_online_tracker::get_iota_omikron_connections(iota_id).unwrap_or_default();

        response.add_typed_default(
            DataType::OmikronConnections,
            DataValue::Array(
                iota_connections
                    .into_iter()
                    .map(|id| DataValue::SignedNumber(id as i128))
                    .collect(),
            ),
        )
    }

    async fn handle_get_register(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        let register_id = sql::get_register_id().await;
        let response = CommunicationValue::new(CommunicationType::GetRegister)
            .with_id(cv.get_id())
            .add_typed_default(
                DataType::UserId,
                DataValue::SignedNumber(register_id as i128),
            );
        self.send(&response).await
    }

    async fn handle_complete_register_iota(
        self: Arc<Self>,
        cv: CommunicationValue,
    ) -> OmikronResult<()> {
        let iota_id_opt = cv.get_data(DataType::IotaId).as_number().map(|n| n as i64);

        if let Some(public_key) = cv.get_data(DataType::PublicKey).as_str() {
            if let Some(iota_id) = iota_id_opt {
                // Register existing IOTA
                match sql::register_complete_iota(iota_id, public_key.to_string()).await {
                    Ok(_) => {
                        let response = CommunicationValue::new(CommunicationType::Success)
                            .with_id(cv.get_id());
                        self.send(&response).await
                    }
                    Err(e) => {
                        let response = CommunicationValue::new(CommunicationType::ErrorInternal)
                            .with_id(cv.get_id())
                            .add_typed_default(DataType::ErrorType, DataValue::Str(e.to_string()));
                        self.send(&response).await
                    }
                }
            } else {
                // Create new IOTA
                match sql::create_new_iota(public_key.to_string()).await {
                    Ok(new_iota_id) => {
                        let response =
                            CommunicationValue::new(CommunicationType::CompleteRegisterIota)
                                .with_id(cv.get_id())
                                .add_typed_default(
                                    DataType::IotaId,
                                    DataValue::SignedNumber(new_iota_id.into()),
                                );
                        self.send(&response).await
                    }
                    Err(e) => {
                        let response = CommunicationValue::new(CommunicationType::ErrorInternal)
                            .with_id(cv.get_id())
                            .add_typed_default(DataType::ErrorType, DataValue::Str(e.to_string()));
                        self.send(&response).await
                    }
                }
            }
        } else {
            self.send_error_response(cv.get_id(), CommunicationType::ErrorInvalidData)
                .await
        }
    }

    async fn handle_complete_register_user(
        self: Arc<Self>,
        cv: CommunicationValue,
    ) -> OmikronResult<()> {
        let user_id = cv.get_data(DataType::UserId).as_number().map(|n| n as i64);
        let username = cv
            .get_data(DataType::Username)
            .as_str()
            .map(|s| s.to_string());
        let public_key = cv
            .get_data(DataType::PublicKey)
            .as_str()
            .map(|s| s.to_string());
        let iota_id = cv.get_sender();
        let reset_token = cv
            .get_data(DataType::ResetToken)
            .as_str()
            .map(|s| s.to_string());

        if let (Some(uid), Some(uname), Some(pk), Some(rt)) =
            (user_id, username, public_key, reset_token)
        {
            match sql::register_complete_user(uid, uname, pk, iota_id as i64, rt).await {
                Ok(_) => {
                    let response =
                        CommunicationValue::new(CommunicationType::Success).with_id(cv.get_id());
                    self.send(&response).await
                }
                Err(e) => {
                    let response = CommunicationValue::new(CommunicationType::ErrorInternal)
                        .with_id(cv.get_id())
                        .add_typed_default(DataType::ErrorType, DataValue::Str(e.to_string()));
                    self.send(&response).await
                }
            }
        } else {
            self.send_error_response(cv.get_id(), CommunicationType::ErrorInvalidData)
                .await
        }
    }

    async fn handle_change_user_data(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        let user_id = cv.get_sender() as i64;
        let mut success = true;
        let mut error_message = String::new();

        // Process each field
        if let Some(username) = cv.get_data(DataType::Username).as_str() {
            if let Err(e) = sql::change_username(user_id, username.to_string()).await {
                success = false;
                error_message = e.to_string();
            }
        }
        if let Some(display) = cv.get_data(DataType::Display).as_str() {
            if let Err(e) = sql::change_display_name(user_id, display.to_string()).await {
                success = false;
                error_message = e.to_string();
            }
        }
        if let Some(avatar) = cv.get_data(DataType::Avatar).as_str() {
            if let Err(e) = sql::change_avatar(user_id, avatar.to_string()).await {
                success = false;
                error_message = e.to_string();
            }
        }
        if let Some(about) = cv.get_data(DataType::About).as_str() {
            if let Err(e) = sql::change_about(user_id, about.to_string()).await {
                success = false;
                error_message = e.to_string();
            }
        }
        if let Some(status) = cv.get_data(DataType::Status).as_str() {
            if let Err(e) = sql::change_status(user_id, status.to_string()).await {
                success = false;
                error_message = e.to_string();
            }
        }
        if let (Some(public_key), Some(private_key_hash)) = (
            cv.get_data(DataType::PublicKey).as_str(),
            cv.get_data(DataType::PrivateKeyHash).as_str(),
        ) {
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
        }

        if success {
            let response = CommunicationValue::new(CommunicationType::Success).with_id(cv.get_id());
            self.send(&response).await
        } else {
            let response = CommunicationValue::new(CommunicationType::ErrorInternal)
                .with_id(cv.get_id())
                .add_typed_default(DataType::ErrorType, DataValue::Str(error_message));
            self.send(&response).await
        }
    }

    async fn handle_change_iota_data(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        let user_id = cv.get_sender() as i64;

        if let (iota_id, Some(reset_token), Some(new_token)) = (
            cv.get_sender(),
            cv.get_data(DataType::ResetToken).as_str(),
            cv.get_data(DataType::NewToken).as_str(),
        ) {
            match sql::get_by_user_id(user_id).await {
                Ok(user) => {
                    let current_token = user.11; // reset_token field
                    if current_token == reset_token {
                        let mut success = true;
                        let mut error_message = String::new();

                        if let Err(e) = sql::change_iota_id(user_id, iota_id as i64).await {
                            success = false;
                            error_message = e.to_string();
                        }
                        if success {
                            if let Err(e) = sql::change_token(user_id, new_token.to_string()).await
                            {
                                success = false;
                                error_message = e.to_string();
                            }
                        }

                        if success {
                            let response = CommunicationValue::new(CommunicationType::Success)
                                .with_id(cv.get_id());
                            self.send(&response).await
                        } else {
                            let response =
                                CommunicationValue::new(CommunicationType::ErrorInternal)
                                    .with_id(cv.get_id())
                                    .add_typed_default(
                                        DataType::ErrorType,
                                        DataValue::Str(error_message),
                                    );
                            self.send(&response).await
                        }
                    } else {
                        self.send_error_response(
                            cv.get_id(),
                            CommunicationType::ErrorInvalidChallenge,
                        )
                        .await
                    }
                }
                Err(_) => {
                    self.send_error_response(cv.get_id(), CommunicationType::ErrorNotFound)
                        .await
                }
            }
        } else {
            self.send_error_response(cv.get_id(), CommunicationType::ErrorInvalidData)
                .await
        }
    }

    async fn handle_delete_user(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        let user_id = cv.get_sender() as i64;
        match sql::delete_user(user_id).await {
            Ok(_) => {
                let response =
                    CommunicationValue::new(CommunicationType::Success).with_id(cv.get_id());
                self.send(&response).await
            }
            Err(e) => {
                let response = CommunicationValue::new(CommunicationType::ErrorInternal)
                    .with_id(cv.get_id())
                    .add_typed_default(DataType::ErrorType, DataValue::Str(e.to_string()));
                self.send(&response).await
            }
        }
    }

    async fn handle_delete_iota(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        let iota_id = cv.get_sender();
        match sql::delete_iota(iota_id as i64).await {
            Ok(_) => {
                let response =
                    CommunicationValue::new(CommunicationType::Success).with_id(cv.get_id());
                self.send(&response).await
            }
            Err(e) => {
                let response = CommunicationValue::new(CommunicationType::ErrorInternal)
                    .with_id(cv.get_id())
                    .add_typed_default(DataType::ErrorType, DataValue::Str(e.to_string()));
                self.send(&response).await
            }
        }
    }

    async fn handle_get_notifications(
        self: Arc<Self>,
        cv: CommunicationValue,
    ) -> OmikronResult<()> {
        let user_id = cv.get_sender() as i64;
        let response_array = match sql::get_notifications(user_id).await {
            Ok(notifications) => notifications
                .into_iter()
                .map(|(sender, amount)| {
                    let tm = mtp::type_map::TypeMap::latest();
                    DataValue::Container(vec![
                        (
                            DataType::SenderId.to_id(&tm),
                            DataValue::SignedNumber(sender.into()),
                        ),
                        (
                            DataType::Amount.to_id(&tm),
                            DataValue::SignedNumber(amount.into()),
                        ),
                    ])
                })
                .collect(),
            Err(e) => {
                log!(PrintType::General, "SQL get_notifications error: {}", e);
                vec![]
            }
        };

        let response = CommunicationValue::new(CommunicationType::GetNotifications)
            .with_id(cv.get_id())
            .add_typed_default(DataType::Notifications, DataValue::Array(response_array));
        self.send(&response).await
    }

    async fn handle_read_notification(
        self: Arc<Self>,
        cv: CommunicationValue,
    ) -> OmikronResult<()> {
        let receiver_id = match cv.get_sender() {
            s if s > 0 => s as i64,
            _ => match cv.get_data(DataType::ReceiverId).as_number() {
                Some(id) => id as i64,
                None => return Ok(()),
            },
        };

        if let Some(other_id) = cv
            .get_data(DataType::SenderId)
            .as_number()
            .map(|n| n as i64)
        {
            if let Err(e) = sql::read_notification(receiver_id, other_id).await {
                log!(PrintType::General, "SQL read_notification error: {}", e);
            } else {
                let response = CommunicationValue::new(CommunicationType::ReadNotification)
                    .with_id(cv.get_id());
                let _ = self.send(&response).await;

                // Sync with Tauri
                crate::notifications::tauri::remove_notification(receiver_id, other_id).await;

                // Sync with other Omikron clients
                let sync_cv = CommunicationValue::new(CommunicationType::ReadNotification)
                    .with_receiver(receiver_id as u64)
                    .add_typed_default(
                        DataType::SenderId,
                        DataValue::SignedNumber(other_id.into()),
                    );
                crate::transport::omikron_manager::send_to_user(receiver_id, &sync_cv).await;
            }
        }
        Ok(())
    }

    async fn handle_push_notification(
        self: Arc<Self>,
        cv: CommunicationValue,
    ) -> OmikronResult<()> {
        let receiver_id = match cv.get_receiver() {
            r if r > 0 => r as i64,
            _ => match cv.get_data(DataType::ReceiverId).as_number() {
                Some(id) => id as i64,
                None => return Ok(()),
            },
        };

        let sender_id = match cv.get_data(DataType::SenderId).as_number() {
            Some(id) => id as i64,
            None => cv.get_sender() as i64,
        };

        if let Err(e) = sql::add_notification(receiver_id, sender_id).await {
            log!(PrintType::General, "SQL add_notification error: {}", e);
        } else {
            let response =
                CommunicationValue::new(CommunicationType::PushNotification).with_id(cv.get_id());
            let _ = self.send(&response).await;

            // Sync with Tauri
            crate::notifications::tauri::send_notification(receiver_id, sender_id).await;

            // Sync with other Omikron clients
            let push_cv = CommunicationValue::new(CommunicationType::PushNotification)
                .with_receiver(receiver_id as u64)
                .add_typed_default(
                    DataType::SenderId,
                    DataValue::SignedNumber(sender_id.into()),
                );
            crate::transport::omikron_manager::send_to_user(receiver_id, &push_cv).await;
        }
        Ok(())
    }

    async fn handle_get_states(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        let user_ids = match cv.get_data(DataType::UserIds) {
            DataValue::Array(ids) => ids,
            _ => return Ok(()),
        };

        let mut states = Vec::new();
        for id_val in user_ids {
            if let DataValue::SignedNumber(user_id) = id_val {
                let user_id = *user_id as i64;
                let status = user_online_tracker::get_user_status(user_id);
                let status_str = match status {
                    Some(ref us) => {
                        if us.connection_type == UserStatus::user_invisible {
                            "user_offline".to_string()
                        } else {
                            us.connection_type.to_string()
                        }
                    }
                    None => UserStatus::iota_offline.to_string(),
                };
                let tm = mtp::type_map::TypeMap::latest();
                let mut map = Vec::new();
                map.push((
                    DataType::UserId.to_id(&tm),
                    DataValue::SignedNumber(user_id.into()),
                ));
                map.push((DataType::UserState.to_id(&tm), DataValue::Str(status_str)));
                states.push(DataValue::Container(map));
            }
        }

        let response = CommunicationValue::new(CommunicationType::GetStates)
            .with_id(cv.get_id())
            .add_typed_default(DataType::UserStates, DataValue::Array(states));
        self.send(&response).await
    }

    async fn handle_ping(self: Arc<Self>, cv: CommunicationValue) -> OmikronResult<()> {
        if let DataValue::SignedNumber(last_ping) = cv.get_data(DataType::LastPing) {
            *self.ping.write().await = *last_ping as i64;
        }

        let response = CommunicationValue::new(CommunicationType::Pong).with_id(cv.get_id());
        self.send(&response).await
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    async fn send(self: Arc<Self>, cv: &CommunicationValue) -> OmikronResult<()> {
        if !cv.is_type(CommunicationType::Pong) && !cv.is_type(CommunicationType::Ping) {
            log_cv_out!(PrintType::Omikron, cv);
        }

        let guard = self.sender.lock().await;
        let sender = guard.as_ref().ok_or(OmikronError::NotConnected)?;

        sender
            .send(cv)
            .await
            .map_err(|e| OmikronError::Send(e.to_string()))
    }

    async fn send_error_response(
        self: Arc<Self>,
        message_id: u32,
        error_type: CommunicationType,
    ) -> OmikronResult<()> {
        let error = CommunicationValue::new(error_type).with_id(message_id);
        self.send(&error).await
    }

    pub async fn close(self: Arc<Self>) {
        log_in!(
            self.get_omikron_id().await.unwrap_or(0),
            PrintType::Omega,
            "Omikron connection Closed"
        );
    }

    async fn cleanup(self: Arc<Self>) {
        if let Some(omikron_id) = self.state.read().await.omikron_id() {
            if omikron_id != 0 {
                log_in!(omikron_id, PrintType::Omega, "Omikron disconnected");
                omikron_manager::remove_omikron(omikron_id).await;
                user_online_tracker::untrack_omikron(omikron_id).await;
            }
        }

        if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
    }

    // Public API for external use
    pub async fn is_authenticated(self: Arc<Self>) -> bool {
        self.state.read().await.is_authenticated()
    }

    pub async fn get_omikron_id(self: Arc<Self>) -> Option<i64> {
        self.state.read().await.omikron_id()
    }

    pub async fn send_message(self: Arc<Self>, cv: &CommunicationValue) -> OmikronResult<()> {
        self.send(cv).await
    }
}

// ============================================================================
// Server Startup
// ============================================================================

pub async fn start(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let cert_pem = load_file_vec("certs", "transport_cert.pem").expect("Error loading Pemfile");

    let key_pem = load_file_vec("certs", "transport_key.pem").expect("Error loading Keyfile");

    let mut host: MTPHost = MTPHost::new(
        HostConfig::new(
            IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)),
        port,
        cert_pem,
        key_pem,
            )
    )
    .await?;

    log!("OmikronServer listening on port {}", port);

    while let Some((sender, mut receiver)) = host.next().await {
        tokio::spawn(async move {
            let conn = OmikronConnection::new(sender);
            conn.handle(&mut receiver).await;
        });
    }

    Ok(())
}
