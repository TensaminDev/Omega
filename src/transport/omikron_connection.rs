use crate::models::OmikronId;
use crate::{
    load_keyring, log, log_cv_in, log_cv_out, log_err, log_in, server,
    transport::omikron_manager,
    util::{file_util::load_file_vec, logger::PrintType},
};
use dashmap::DashMap;
use mtp::{
    codec::{CommunicationType, CommunicationValue, DataType, DataValue},
    crypto::PublicKeyBundle,
    host::{AuthenticationPolicy, HostConfig, Policy, SendMode},
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
    sync::{Mutex, RwLock},
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
    sender: Mutex<Option<WebMtpSender>>,
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
    pub fn new(sender: WebMtpSender, id: u64) -> Arc<Self> {
        Arc::new(Self {
            id,
            sender: Mutex::new(Some(sender)),
            ping: RwLock::new(-1),
            waiting_tasks: DashMap::new(),
            cleanup_handle: std::sync::Mutex::new(None),
        })
    }

    pub async fn handle(self: Arc<Self>, receiver: &mut WebMtpReceiver) {
        log_in!(
            self.id as i64,
            PrintType::Omega,
            "Omikron connection started"
        );
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
        while let Ok(value) = receiver.receive().await {
            if let Err(error) = self.clone().process_message(value).await {
                log_err!(0, PrintType::Omega, "Error processing message: {}", error);
                if matches!(error, crate::error::OmegaError::NotConnected) {
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
        if !value.is_type(CommunicationType::Pong) && !value.is_type(CommunicationType::Ping) {
            log_cv_in!(PrintType::Omikron, &value);
        }
        if let Some((_, task)) = self.waiting_tasks.remove(&value.get_id()) {
            let _ = (task.task)(self.clone(), value);
            return Ok(());
        }
        if value.is_type(CommunicationType::Ping) {
            return self.ping(value).await;
        }
        self.dispatch(value).await
    }

    async fn dispatch(self: Arc<Self>, value: CommunicationValue) -> OmikronResult<()> {
        let id = self.id as i64;
        match value.get_comm_type_enum() {
            Some(CommunicationType::ShortenLink) => {
                crate::transport::handlers::links::shorten(self, value).await
            }
            Some(CommunicationType::UserConnected) => {
                crate::transport::handlers::presence::user_connected(self, value, id).await
            }
            Some(CommunicationType::UserDisconnected) => {
                crate::transport::handlers::presence::user_disconnected(self, value, id).await
            }
            Some(CommunicationType::IotaConnected) => {
                crate::transport::handlers::presence::iota_connected(self, value, id).await
            }
            Some(CommunicationType::IotaDisconnected) => {
                crate::transport::handlers::presence::iota_disconnected(self, value, id).await
            }
            Some(CommunicationType::SyncClientIotaStatus) => {
                crate::transport::handlers::presence::sync_status(self, value, id).await
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

    async fn ping(self: Arc<Self>, value: CommunicationValue) -> OmikronResult<()> {
        if let DataValue::SignedNumber(last_ping) = value.get_data(DataType::LastPing) {
            *self.ping.write().await = *last_ping as i64;
        }
        let response = CommunicationValue::new(CommunicationType::Pong).with_id(value.get_id());
        self.send(&response).await
    }
    pub(crate) async fn send(self: Arc<Self>, value: &CommunicationValue) -> OmikronResult<()> {
        if !value.is_type(CommunicationType::Pong) && !value.is_type(CommunicationType::Ping) {
            log_cv_out!(PrintType::Omikron, value);
        }
        let guard = self.sender.lock().await;
        let sender = guard
            .as_ref()
            .ok_or(crate::error::OmegaError::NotConnected)?;
        sender
            .send(value)
            .await
            .map_err(|error| crate::error::OmegaError::SendError(error.to_string()))
    }
    pub(crate) async fn send_error_response(
        self: Arc<Self>,
        message_id: u32,
        error_type: CommunicationType,
    ) -> OmikronResult<()> {
        self.send(&CommunicationValue::new(error_type).with_id(message_id))
            .await
    }
    pub async fn close(self: Arc<Self>) {
        log_in!(
            self.get_omikron_id().await.unwrap_or(0),
            PrintType::Omega,
            "Omikron connection Closed"
        );
    }
    async fn cleanup(self: Arc<Self>) {
        if self.id != 0 {
            log_in!(self.id as i64, PrintType::Omega, "Omikron disconnected");
            omikron_manager::remove_omikron(self.id as i64).await;
            crate::sql::user_online_tracker::untrack_omikron(self.id as i64).await;
        }
        if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
    }
    pub async fn get_omikron_id(self: Arc<Self>) -> Option<i64> {
        Some(self.id as i64)
    }
    pub async fn send_message(self: Arc<Self>, value: &CommunicationValue) -> OmikronResult<()> {
        self.send(value).await
    }
}

pub async fn get_by_omikron_id(omikron_id: u64, _: Option<String>) -> Option<PublicKeyBundle> {
    crate::db::omikron_repo::get_omikron_by_id(OmikronId::from(omikron_id as i64))
        .await
        .ok()
        .map(|omikron| omikron.public_key)
}
pub async fn complete_register(_: PublicKeyBundle, _: Option<String>) -> u64 {
    0
}

pub async fn start(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let cert_pem = load_file_vec("certs", "transport_cert.pem")?;
    let key_pem = load_file_vec("certs", "transport_key.pem")?;
    let web_config = server::server::build_web_config()?;
    let host_config = HostConfig::new(
        IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)),
        port,
        cert_pem,
        key_pem,
    )
    .with_policy(Policy {
        send_mode: SendMode::SingleStreamPerMessage,
        max_message_size: 1_000_000_000,
        handshake_max_message_size: 1_000_000,
        close_frame_len: u32::MAX,
        application_close_code: 0,
        open_stream_timeout: Duration::from_millis(2_000),
        write_timeout: Duration::from_millis(2_000),
        accept_stream_timeout: Duration::from_millis(10_000),
        read_timeout: Duration::from_millis(30_000),
        keep_alive_interval: Some(Duration::from_secs(6)),
        max_idle_timeout: Some(Duration::from_secs(30)),
        force_close_delay: Duration::from_millis(300),
        receiver_queue_capacity: 1000,
        max_concurrent_stream_tasks: 10,
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
    log!("OmegaServer listening on port {}", port);
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
        let connection = OmikronConnection::new(conn.sender, conn.client_id);
        tokio::spawn(async move {
            let _guard = ConnectionLimitGuard(peer_ip);
            omikron_manager::add_omikron(connection.clone()).await;
            connection.handle(&mut conn.receiver).await;
        });
    }
    Ok(())
}
