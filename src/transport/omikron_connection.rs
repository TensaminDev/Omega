use super::capabilities::{OmegaCapabilities, PeerCapabilities};
use crate::models::OmikronId;
use crate::{
    load_keyring, log, log_cv_in, log_cv_out, log_err, log_in, server,
    state::OmegaState,
    transport::omikron_manager,
    util::{file_util::load_file_vec, logger::PrintType},
};
use dashmap::DashMap;
use mtp::{
    codec::{CommunicationType, CommunicationValue},
    crypto::PublicKeyBundle,
    host::{AuthState, AuthenticationPolicy, HostConfig, Policy, SendMode},
    webserver::{MTPWebServer, WebMtpReceiver, WebMtpSender},
};
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, mpsc},
    time::interval,
};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
const MAX_WAITING_AGE: Duration = Duration::from_secs(60);
static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_CONNECTIONS_BY_IP: once_cell::sync::Lazy<DashMap<IpAddr, usize>> =
    once_cell::sync::Lazy::new(DashMap::new);

struct ConnectionLimitGuard(Option<IpAddr>);
impl Drop for ConnectionLimitGuard {
    fn drop(&mut self) {
        ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::AcqRel);
        if let Some(ip) = self.0 {
            if let Some(mut count) = ACTIVE_CONNECTIONS_BY_IP.get_mut(&ip) {
                *count = count.saturating_sub(1);
            }
            ACTIVE_CONNECTIONS_BY_IP.remove_if(&ip, |_, count| *count == 0);
        }
    }
}

pub type OmikronResult<T> = crate::error::Result<T>;
pub struct WaitingTask {
    pub task: Box<dyn Fn(Arc<OmikronConnection>, CommunicationValue) -> bool + Send + Sync>,
    pub inserted_at: Instant,
}

pub struct OmikronConnection {
    id: u64,
    state: Arc<OmegaState>,
    sender: Mutex<Option<WebMtpSender>>,
    waiting_tasks: DashMap<u32, WaitingTask>,
    cleanup_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    peer_capabilities: PeerCapabilities,
}
impl Drop for OmikronConnection {
    fn drop(&mut self) {
        if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
    }
}

impl OmikronConnection {
    pub fn new(
        sender: WebMtpSender,
        id: u64,
        description: Option<&str>,
        state: Arc<OmegaState>,
        authenticated: bool,
    ) -> Option<Arc<Self>> {
        if !authenticated {
            return None;
        }
        let peer_capabilities =
            PeerCapabilities::from_identification_description(description).ok()?;
        Some(Arc::new(Self {
            id,
            state,
            sender: Mutex::new(Some(sender)),
            waiting_tasks: DashMap::new(),
            cleanup_handle: std::sync::Mutex::new(None),
            peer_capabilities,
        }))
    }

    pub fn peer_capabilities(&self) -> &PeerCapabilities {
        &self.peer_capabilities
    }

    pub async fn handle(self: Arc<Self>, receiver: &mut WebMtpReceiver) {
        log_in!(
            self.id as i64,
            PrintType::Omega,
            "Omikron connection started"
        );
        let capabilities = CommunicationValue::new(CommunicationType::IdentificationResponse)
            .add_typed_default(
                mtp::codec::DataType::Description,
                mtp::codec::DataValue::Str(
                    OmegaCapabilities::current().identification_description(),
                ),
            );
        if let Err(error) = self.clone().send(&capabilities).await {
            log_err!(
                self.id as i64,
                PrintType::Omega,
                "Failed to send Omega capabilities: {}",
                error
            );
            self.clone().cleanup().await;
            return;
        }
        let cleanup_conn = self.clone();
        *self.cleanup_handle.lock().unwrap() = Some(tokio::spawn(async move {
            let mut ticker = interval(CLEANUP_INTERVAL);
            loop {
                ticker.tick().await;
                cleanup_conn
                    .waiting_tasks
                    .retain(|_, task| task.inserted_at.elapsed() < MAX_WAITING_AGE);
            }
        }));
        loop {
            match receiver.receive().await {
                Ok(value) => {
                    if let Err(error) = self.clone().process_message(value).await {
                        log_err!(
                            self.id as i64,
                            PrintType::Omega,
                            "Error processing Omikron message: {}",
                            error
                        );
                        if matches!(error, crate::error::OmegaError::NotConnected) {
                            break;
                        }
                    }
                }
                Err(error) => {
                    log_err!(
                        self.id as i64,
                        PrintType::Omega,
                        "Omikron receive loop ended: {}; transport close reason: {:?}",
                        error,
                        receiver.close_reason()
                    );
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

    async fn process_message(self: Arc<Self>, value: CommunicationValue) -> OmikronResult<()> {
        log_cv_in!(PrintType::Omikron, &value);
        if value.is_type(CommunicationType::Relay) {
            return self.dispatch(value).await;
        }
        if let Some(message_id) = value.id()
            && let Some((_, task)) = self.waiting_tasks.remove(&message_id)
        {
            let _ = (task.task)(self.clone(), value);
            return Ok(());
        }
        self.dispatch(value).await
    }

    async fn dispatch(self: Arc<Self>, value: CommunicationValue) -> OmikronResult<()> {
        let id = self.id as i64;
        let state = self.state.clone();
        if value.is_type(CommunicationType::Relay) {
            let value = crate::transport::relay_router::ensure_relay_frame_id(value);
            let request_id = value.id();
            let result = crate::transport::relay_router::route_from_omikron(id, value).await;
            let response = match &result {
                Ok(()) => request_id.map(|request_id| {
                    CommunicationValue::new(CommunicationType::Success).with_id(request_id)
                }),
                Err(error) => {
                    log_err!(id, PrintType::Omega, "Relay routing failed: {}", error);
                    request_id.map(|request_id| {
                        CommunicationValue::new(
                            crate::transport::relay_router::error_response_type(error),
                        )
                        .with_id(request_id)
                    })
                }
            };
            if let Some(response) = response
                && let Err(send_error) = self.clone().send(&response).await
            {
                log_err!(
                    id,
                    PrintType::Omega,
                    "Relay routing response failed: {}",
                    send_error
                );
            }
            return result.map_err(|error| crate::error::OmegaError::Transport(error.to_string()));
        }
        match value.get_comm_type_enum() {
            Some(CommunicationType::ShortenLink) => {
                crate::transport::handlers::links::shorten(self, value).await
            }
            Some(CommunicationType::UserConnected) => {
                crate::transport::handlers::presence::user_connected(state, self, value, id).await
            }
            Some(CommunicationType::UserDisconnected) => {
                crate::transport::handlers::presence::user_disconnected(state, self, value, id)
                    .await
            }
            Some(CommunicationType::IotaConnected) => {
                crate::transport::handlers::presence::iota_connected(state, self, value, id).await
            }
            Some(CommunicationType::IotaDisconnected) => {
                crate::transport::handlers::presence::iota_disconnected(state, self, value, id)
                    .await
            }
            Some(CommunicationType::SyncClientIotaStatus) => {
                crate::transport::handlers::presence::sync_status(state, self, value, id).await
            }
            Some(CommunicationType::GetUserData) => {
                crate::transport::handlers::user_data::get_user(self, value).await
            }
            Some(CommunicationType::GetIotaData) => {
                crate::transport::handlers::user_data::get_iota(self, value).await
            }
            Some(CommunicationType::ChangeUserData) => {
                crate::transport::handlers::user_data::change_user(self, value).await
            }
            Some(CommunicationType::ChangeIotaData) => {
                crate::transport::handlers::user_data::change_iota(self, value).await
            }
            Some(CommunicationType::GetRegister) => {
                crate::transport::handlers::register::get_register(self, value).await
            }
            Some(CommunicationType::CompleteRegisterIota) => {
                crate::transport::handlers::register::complete_iota(self, value).await
            }
            Some(CommunicationType::CompleteRegisterUser) => {
                crate::transport::handlers::register::complete_user(self, value).await
            }
            Some(CommunicationType::DeleteUser) => {
                crate::transport::handlers::account::user(self, value).await
            }
            Some(CommunicationType::AttachUserBegin) => {
                crate::transport::handlers::account::attach_begin(self, value).await
            }
            Some(CommunicationType::AttachUserComplete) => {
                crate::transport::handlers::account::attach_complete(self, value).await
            }
            Some(CommunicationType::DeleteUserCredentialBegin) => {
                crate::transport::handlers::account::delete_credential_begin(self, value).await
            }
            Some(CommunicationType::DeleteUserCredentialComplete) => {
                crate::transport::handlers::account::delete_credential_complete(self, value).await
            }
            Some(CommunicationType::EraseHostedUserDataAck) => {
                crate::transport::handlers::account::erase_hosted_user_data_ack(self, value).await
            }
            Some(CommunicationType::ReleaseUserFromIota) => {
                crate::transport::handlers::account::release_from_iota(self, value).await
            }
            Some(CommunicationType::DeleteIota) => {
                crate::transport::handlers::account::iota(self, value).await
            }
            Some(CommunicationType::GetNotifications) => {
                crate::transport::handlers::notifications::get(self, value).await
            }
            Some(CommunicationType::ReadNotification) => {
                crate::transport::handlers::notifications::read(self, value).await
            }
            Some(CommunicationType::PushNotification) => {
                crate::transport::handlers::notifications::push(self, value).await
            }
            Some(CommunicationType::GetStates) => {
                crate::transport::handlers::states::get(self, value).await
            }
            Some(CommunicationType::StateSubscribe) => {
                crate::transport::handlers::presence::state_subscribe(state, self, value, id).await
            }
            Some(CommunicationType::ClientChanged) => {
                crate::transport::handlers::presence::client_changed_legacy(state, self, value, id)
                    .await
            }
            _ => {
                log_err!(
                    0,
                    PrintType::Omega,
                    "Unknown message type: {:?}",
                    value.get_type()
                );
                Ok(())
            }
        }
    }

    pub(crate) async fn send(self: Arc<Self>, value: &CommunicationValue) -> OmikronResult<()> {
        log_cv_out!(PrintType::Omikron, value);
        let guard = self.sender.lock().await;
        let sender = guard
            .as_ref()
            .ok_or(crate::error::OmegaError::NotConnected)?;
        sender
            .send(value)
            .await
            .map_err(|error| crate::error::OmegaError::SendError(error.to_string()))
    }

    pub(crate) async fn send_messages(
        self: Arc<Self>,
        values: &[CommunicationValue],
    ) -> OmikronResult<()> {
        let guard = self.sender.lock().await;
        let sender = guard
            .as_ref()
            .ok_or(crate::error::OmegaError::NotConnected)?;
        for value in values {
            log_cv_out!(PrintType::Omikron, value);
            sender
                .send(value)
                .await
                .map_err(|error| crate::error::OmegaError::SendError(error.to_string()))?;
        }
        Ok(())
    }

    pub(crate) async fn await_response(
        self: Arc<Self>,
        value: &CommunicationValue,
        timeout: Duration,
    ) -> OmikronResult<CommunicationValue> {
        let (tx, mut rx) = mpsc::channel(1);
        let message_id = value.id().unwrap_or_default();
        let task_tx = tx.clone();
        self.waiting_tasks.insert(
            message_id,
            WaitingTask {
                task: Box::new(move |_, response| {
                    let task_tx = task_tx.clone();
                    tokio::spawn(async move {
                        let _ = task_tx.send(response).await;
                    });
                    true
                }),
                inserted_at: Instant::now(),
            },
        );

        if let Err(error) = self.clone().send(value).await {
            self.waiting_tasks.remove(&message_id);
            return Err(error);
        }

        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(crate::error::OmegaError::Transport(
                "Relay response channel closed".into(),
            )),
            Err(_) => {
                self.waiting_tasks.remove(&message_id);
                Err(crate::error::OmegaError::Transport(
                    "Relay response timed out".into(),
                ))
            }
        }
    }
    pub(crate) async fn send_error_response(
        self: Arc<Self>,
        message_id: u32,
        error_type: CommunicationType,
    ) -> OmikronResult<()> {
        self.send(&CommunicationValue::new(error_type).with_id(message_id))
            .await
    }
    pub(crate) async fn send_error_response_with_detail(
        self: Arc<Self>,
        message_id: u32,
        error_type: CommunicationType,
        detail: &'static str,
    ) -> OmikronResult<()> {
        self.send(
            &CommunicationValue::new(error_type)
                .with_id(message_id)
                .add_typed_default(
                    mtp::codec::DataType::ErrorType,
                    mtp::codec::DataValue::Str(detail.to_string()),
                ),
        )
        .await
    }
    pub async fn close(self: Arc<Self>) {
        log_in!(
            self.id as i64,
            PrintType::Omega,
            "Omikron connection Closed"
        );
        if let Some(sender) = self.sender.lock().await.take() {
            sender.close();
        }
    }
    async fn cleanup(self: Arc<Self>) {
        if self.id != 0 {
            log_in!(self.id as i64, PrintType::Omega, "Omikron disconnected");
            if omikron_manager::remove_omikron(self.id as i64, &self).await {
                crate::transport::handlers::presence::omikron_disconnected(
                    self.state.clone(),
                    self.id as i64,
                )
                .await;
            }
        }
        if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
    }
    pub async fn get_omikron_id(self: Arc<Self>) -> Option<i64> {
        Some(self.id as i64)
    }
    pub fn state(&self) -> Arc<OmegaState> {
        self.state.clone()
    }
    pub async fn send_message(self: Arc<Self>, value: &CommunicationValue) -> OmikronResult<()> {
        self.send(value).await
    }
}

pub async fn get_by_omikron_id(
    omikron_id: u64,
    description: Option<String>,
) -> Option<PublicKeyBundle> {
    PeerCapabilities::from_identification_description(description.as_deref()).ok()?;
    crate::db::omikron_repo::get_omikron_by_id(OmikronId::from(omikron_id as i64))
        .await
        .ok()
        .map(|omikron| omikron.public_key)
}
pub async fn complete_register(_: PublicKeyBundle, _: Option<String>) -> u64 {
    0
}

pub async fn start(port: u16, state: Arc<OmegaState>) -> Result<(), Box<dyn std::error::Error>> {
    let cert_pem = load_file_vec("certs", "cert.pem")?;
    let key_pem = load_file_vec("certs", "key.pem")?;
    let web_config = server::server::build_web_config()?
        .serve_tcp_https(true)
        .max_tcp_connections(256);
    let ip = IpAddr::from(Ipv4Addr::new(0, 0, 0, 0));
    let host_config = HostConfig::new(ip, port, cert_pem, key_pem)
        .with_policy(Policy {
            send_mode: SendMode::SingleStreamPerMessage,
            max_message_size: 1_000_000_000,
            handshake_max_message_size: 1_000_000,
            close_frame_len: u32::MAX,
            application_close_code: 0,
            open_stream_timeout: Duration::from_millis(5_000),
            write_timeout: Duration::from_millis(5_000),
            accept_stream_timeout: Duration::from_millis(10_000),
            read_timeout: Duration::from_millis(30_000),
            keep_alive_interval: Some(Duration::from_secs(6)),
            max_idle_timeout: Some(Duration::from_secs(30)),
            force_close_delay: Duration::from_millis(300),
            receiver_queue_capacity: 1000,
            max_concurrent_stream_tasks: 64,
            persistent_stream_max_retries: 5,
            persistent_stream_retry_backoff: Duration::from_secs(5),
            max_frames_per_stream: None,
        })
        .with_authentication(
            load_keyring(),
            Box::new(|id, description| Box::pin(get_by_omikron_id(id, description))),
            Box::new(|key, description| Box::pin(complete_register(key, description))),
        )
        .with_authentication_policy(AuthenticationPolicy::ForceAuthentication);
    let mut server = MTPWebServer::new(host_config, web_config).await?;
    log!("OmegaServer listening on {}:{}", ip.to_string(), port);
    loop {
        let mut conn = match server.accept().await {
            Ok(Some(conn)) => conn,
            Ok(None) => break,
            Err(error) => {
                log_err!(
                    0,
                    PrintType::Omega,
                    "Rejected omikron connection: {}",
                    error
                );
                continue;
            }
        };
        let config = crate::config::RateLimitConfig::from_env();
        let peer_ip = conn.remote_addr.map(|address| address.ip());
        let active = ACTIVE_CONNECTIONS.fetch_add(1, Ordering::AcqRel) + 1;
        let peer_active = peer_ip.map(|ip| {
            let mut count = ACTIVE_CONNECTIONS_BY_IP.entry(ip).or_insert(0);
            *count += 1;
            *count
        });
        if active > config.transport_connections
            || peer_active.is_some_and(|count| count > config.transport_connections_per_ip)
        {
            drop(ConnectionLimitGuard(peer_ip));
            log_err!(
                0,
                PrintType::Omega,
                "Rejected connection: active connection limit reached"
            );
            continue;
        }
        let authenticated = matches!(&conn.auth_state, AuthState::Authenticated);
        let Some(connection) = OmikronConnection::new(
            conn.sender,
            conn.client_id,
            conn.description.as_deref(),
            state.clone(),
            authenticated,
        ) else {
            log_err!(
                0,
                PrintType::Omega,
                "Rejected Omikron connection without authenticated transport state or valid capabilities"
            );
            continue;
        };
        tokio::spawn(async move {
            let _guard = ConnectionLimitGuard(peer_ip);
            omikron_manager::add_omikron(connection.clone()).await;
            connection.handle(&mut conn.receiver).await;
        });
    }
    Ok(())
}
