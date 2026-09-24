use super::capabilities::{OmegaCapabilities, PeerCapabilities};
use crate::models::OmikronId;
use crate::{
    config::TransportConfig,
    log, log_cv_in, log_cv_out, log_err, log_in, server,
    state::OmegaState,
    transport::connection::{
        RequiredMtpFields, validate_dispatch_fields, validate_dispatch_sender,
    },
    transport::omikron_manager,
    util::{file_util::load_file_vec, logger::PrintType},
};
use dashmap::{DashMap, mapref::entry::Entry};
use mtp::{
    codec::{CommunicationType, CommunicationValue},
    crypto::PublicKeyBundle,
    host::{AuthState, AuthenticationPolicy, HostConfig, Policy, SendMode},
    webserver::{MTPWebServer, WebMtpReceiver, WebMtpSender},
};
use std::{
    net::IpAddr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Semaphore, mpsc, oneshot},
    time::interval,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
const MAX_WAITING_AGE: Duration = Duration::from_secs(60);
const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0";
static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static NEXT_CORRELATION_ID: AtomicU32 = AtomicU32::new(1);
static ACTIVE_CONNECTIONS_BY_IP: once_cell::sync::Lazy<DashMap<IpAddr, usize>> =
    once_cell::sync::Lazy::new(DashMap::new);

fn parse_bind_address(value: Option<&str>) -> Result<IpAddr, std::net::AddrParseError> {
    value
        .unwrap_or(DEFAULT_BIND_ADDRESS)
        .trim()
        .parse::<IpAddr>()
}

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
    pub sender: oneshot::Sender<CommunicationValue>,
    pub inserted_at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DispatchClass {
    Concurrent,
    Ordered,
}

pub struct OmikronConnection {
    id: u64,
    state: Arc<OmegaState>,
    sender: Mutex<Option<WebMtpSender>>,
    waiting_tasks: DashMap<u32, WaitingTask>,
    cleanup_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    handler_tasks: TaskTracker,
    handler_cancel: CancellationToken,
    handler_limit: Arc<Semaphore>,
    relay_limit: Arc<Semaphore>,
    relay_budget: StdMutex<(Instant, u64)>,
    ordered_sender: mpsc::Sender<CommunicationValue>,
    ordered_receiver: StdMutex<Option<mpsc::Receiver<CommunicationValue>>>,
    peer_capabilities: PeerCapabilities,
}
impl Drop for OmikronConnection {
    fn drop(&mut self) {
        if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
            handle.abort();
        }
        self.handler_cancel.cancel();
        self.handler_tasks.close();
        self.waiting_tasks.clear();
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
        if !authenticated || id == 0 || i64::try_from(id).is_err() {
            return None;
        }
        let peer_capabilities =
            PeerCapabilities::from_identification_description(description).ok()?;
        let handler_concurrency = state.omikron_handler_concurrency;
        let relay_concurrency = state.config.dispatch.relay_max_concurrent;
        let (ordered_sender, ordered_receiver) = mpsc::channel(handler_concurrency);
        Some(Arc::new(Self {
            id,
            state,
            sender: Mutex::new(Some(sender)),
            waiting_tasks: DashMap::new(),
            cleanup_handle: std::sync::Mutex::new(None),
            handler_tasks: TaskTracker::new(),
            handler_cancel: CancellationToken::new(),
            handler_limit: Arc::new(Semaphore::new(handler_concurrency)),
            relay_limit: Arc::new(Semaphore::new(relay_concurrency)),
            relay_budget: StdMutex::new((Instant::now(), 0)),
            ordered_sender,
            ordered_receiver: StdMutex::new(Some(ordered_receiver)),
            peer_capabilities,
        }))
    }

    pub fn peer_capabilities(&self) -> &PeerCapabilities {
        &self.peer_capabilities
    }

    fn track_handler<F>(&self, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = self.handler_tasks.spawn(task);
        let id = self.id as i64;
        tokio::spawn(async move {
            if let Err(error) = handle.await {
                log_err!(
                    id,
                    PrintType::Omega,
                    "Omikron handler task failed: {}",
                    error
                );
            }
        });
    }

    fn start_ordered_worker(self: &Arc<Self>) {
        let Some(mut receiver) = self.ordered_receiver.lock().unwrap().take() else {
            return;
        };
        let connection = self.clone();
        let cancellation = self.handler_cancel.clone();
        self.track_handler(async move {
            loop {
                let value = tokio::select! {
                    _ = cancellation.cancelled() => break,
                    value = receiver.recv() => match value {
                        Some(value) => value,
                        None => break,
                    },
                };
                let global_permit = tokio::select! {
                    _ = cancellation.cancelled() => break,
                    permit = connection.state.global_handler_limit.clone().acquire_owned() => {
                        match permit {
                            Ok(permit) => permit,
                            Err(_) => break,
                        }
                    }
                };
                let permit = tokio::select! {
                    _ = cancellation.cancelled() => break,
                    permit = connection.handler_limit.clone().acquire_owned() => {
                        match permit {
                            Ok(permit) => permit,
                            Err(_) => break,
                        }
                    }
                };
                let request_id = value.id();
                let result = tokio::select! {
                    _ = cancellation.cancelled() => break,
                    result = connection.clone().dispatch(value) => result,
                };
                drop(permit);
                drop(global_permit);
                if let Err(error) = result {
                    log_err!(
                        connection.id as i64,
                        PrintType::Omega,
                        "Error processing ordered Omikron message: {}",
                        error
                    );
                    if let Some(request_id) = request_id {
                        let response_type = match error {
                            crate::OmegaError::NotAuthenticated => {
                                CommunicationType::ErrorNotAuthenticated
                            }
                            _ => CommunicationType::ErrorInternal,
                        };
                        let _ = connection
                            .clone()
                            .send_error_response(request_id, response_type)
                            .await;
                    }
                }
            }
        });
    }

    async fn stop_handlers(&self) {
        self.handler_cancel.cancel();
        self.handler_tasks.close();
        let _ = tokio::time::timeout(Duration::from_secs(1), self.handler_tasks.wait()).await;
        self.waiting_tasks.clear();
    }

    fn correlation_response(value: &CommunicationValue) -> bool {
        matches!(
            value.get_comm_type_enum(),
            Some(CommunicationType::Success)
                | Some(CommunicationType::ErrorProtocol)
                | Some(CommunicationType::ErrorAnonymous)
                | Some(CommunicationType::ErrorInternal)
                | Some(CommunicationType::ErrorInvalidData)
                | Some(CommunicationType::ErrorInvalidUserId)
                | Some(CommunicationType::ErrorInvalidOmikronId)
                | Some(CommunicationType::ErrorNotFound)
                | Some(CommunicationType::ErrorNotAuthenticated)
                | Some(CommunicationType::ErrorNoIota)
                | Some(CommunicationType::ErrorInvalidChallenge)
                | Some(CommunicationType::ErrorInvalidSecret)
                | Some(CommunicationType::ErrorInvalidPrivateKey)
                | Some(CommunicationType::ErrorInvalidPublicKey)
                | Some(CommunicationType::ErrorNoUserId)
                | Some(CommunicationType::ErrorNoCallId)
                | Some(CommunicationType::ErrorInvalidCallId)
        )
    }

    fn next_correlation_id(&self) -> Option<u32> {
        for _ in 0..u32::MAX {
            let id = NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed);
            if id != 0 && !self.waiting_tasks.contains_key(&id) {
                return Some(id);
            }
        }
        None
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
        self.start_ordered_worker();
        let cleanup_conn = Arc::downgrade(&self);
        *self.cleanup_handle.lock().unwrap() = Some(tokio::spawn(async move {
            let mut ticker = interval(CLEANUP_INTERVAL);
            loop {
                ticker.tick().await;
                let Some(connection) = cleanup_conn.upgrade() else {
                    break;
                };
                connection
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
        if Self::correlation_response(&value)
            && let Some(message_id) = value.id()
            && let Some((_, task)) = self.waiting_tasks.remove(&message_id)
        {
            let _ = task.sender.send(value);
            return Ok(());
        }

        if !value.is_type(CommunicationType::Relay) {
            validate_dispatch_fields(&value)?;
            match validate_dispatch_sender(&self, &value) {
                Ok(()) => {}
                Err(crate::OmegaError::NotAuthenticated) => {
                    return self
                        .clone()
                        .send_error_response(
                            value.require_id()?,
                            CommunicationType::ErrorNotAuthenticated,
                        )
                        .await;
                }
                Err(error) => return Err(error),
            }
        }

        let dispatch_class = Self::dispatch_class(&value);
        if dispatch_class == DispatchClass::Ordered {
            tokio::select! {
                _ = self.handler_cancel.cancelled() => {
                    return Err(crate::error::OmegaError::NotConnected);
                }
                result = self.ordered_sender.send(value) => {
                    result.map_err(|_| crate::error::OmegaError::NotConnected)?;
                }
            }
            return Ok(());
        }

        let global_permit = tokio::select! {
            _ = self.handler_cancel.cancelled() => {
                return Err(crate::error::OmegaError::NotConnected);
            }
            permit = self.state.global_handler_limit.clone().acquire_owned() => {
                permit.map_err(|_| crate::error::OmegaError::NotConnected)?
            }
        };
        let permit = tokio::select! {
            _ = self.handler_cancel.cancelled() => {
                drop(global_permit);
                return Err(crate::error::OmegaError::NotConnected);
            }
            permit = self.handler_limit.clone().acquire_owned() => {
                permit.map_err(|_| crate::error::OmegaError::NotConnected)?
            }
        };
        let connection = self.clone();
        let cancellation = self.handler_cancel.clone();
        let request_id = value
            .id()
            .filter(|_| !value.is_type(CommunicationType::Relay));
        self.track_handler(async move {
            tokio::select! {
                _ = cancellation.cancelled() => {}
                result = connection.clone().dispatch(value) => {
                    if let Err(error) = result {
                        log_err!(
                            connection.id as i64,
                            PrintType::Omega,
                            "Error processing Omikron message: {}",
                            error
                        );
                        if let Some(request_id) = request_id {
                            let response_type = match error {
                                crate::OmegaError::NotAuthenticated => CommunicationType::ErrorNotAuthenticated,
                                _ => CommunicationType::ErrorInternal,
                            };
                            let _ = connection.clone().send_error_response(request_id, response_type).await;
                        }
                    }
                }
            }
            drop(permit);
            drop(global_permit);
        });
        Ok(())
    }

    fn dispatch_class(value: &CommunicationValue) -> DispatchClass {
        match value.get_comm_type_enum() {
            Some(CommunicationType::UserConnected)
            | Some(CommunicationType::UserDisconnected)
            | Some(CommunicationType::IotaConnected)
            | Some(CommunicationType::IotaDisconnected)
            | Some(CommunicationType::SyncClientIotaStatus)
            | Some(CommunicationType::StateSubscribe)
            | Some(CommunicationType::ClientChanged) => DispatchClass::Ordered,
            _ => DispatchClass::Concurrent,
        }
    }

    async fn dispatch(self: Arc<Self>, value: CommunicationValue) -> OmikronResult<()> {
        let id = self.id as i64;
        let state = self.state.clone();
        if value.is_type(CommunicationType::Relay) {
            let value = crate::transport::relay_router::ensure_relay_frame_id(value);
            let request_id = value.id();
            let admitted = self.relay_limit.clone().try_acquire_owned().ok();
            let bytes = value
                .payload()
                .as_encrypted()
                .map(|payload| payload.ciphertext.len() as u64);
            let allowed = {
                let mut budget = self.relay_budget.lock().unwrap();
                if budget.0.elapsed() >= self.state.config.dispatch.relay_window {
                    *budget = (Instant::now(), 0);
                }
                admitted
                    .as_ref()
                    .and(bytes)
                    .and_then(|bytes| budget.1.checked_add(bytes))
                    .filter(|total| *total <= self.state.config.dispatch.relay_bytes_per_window)
                    .map(|total| {
                        budget.1 = total;
                    })
                    .is_some()
            };
            if admitted.is_none() || !allowed {
                log_err!(id, PrintType::Omega, "Relay admission limit reached");
                if let Some(request_id) = request_id {
                    self.clone()
                        .send(
                            &CommunicationValue::new(CommunicationType::ErrorInternal)
                                .with_id(request_id),
                        )
                        .await?;
                }
                return Ok(());
            }
            let _relay_permit = admitted;
            let result =
                crate::transport::relay_router::route_from_omikron(&state, id, value).await;
            let response = match &result {
                Ok(response) => request_id.map(|request_id| response.clone().with_id(request_id)),
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
            return result
                .map(|_| ())
                .map_err(|error| crate::error::OmegaError::Transport(error.to_string()));
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
            Some(CommunicationType::GetRegister) => {
                crate::transport::handlers::register::get_register(self, value).await
            }
            Some(CommunicationType::CompleteRegisterIota) => {
                crate::transport::handlers::register::complete_iota(self, value).await
            }
            Some(CommunicationType::CompleteRegisterUser) => {
                crate::transport::handlers::register::complete_user(self, value).await
            }
            Some(CommunicationType::CreateUserInvitation) => {
                crate::transport::handlers::invitations::manage(self, value).await
            }
            Some(CommunicationType::ListUserInvitations) => {
                crate::transport::handlers::invitations::manage(self, value).await
            }
            Some(CommunicationType::RevokeUserInvitation) => {
                crate::transport::handlers::invitations::manage(self, value).await
            }
            Some(CommunicationType::RedeemUserInvitation) => {
                crate::transport::handlers::invitations::redeem(self, value).await
            }
            Some(CommunicationType::GetUserInvitationStatus) => {
                crate::transport::handlers::invitations::redeem(self, value).await
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
            Some(CommunicationType::AcknowledgeIotaUserProvision) => {
                crate::transport::handlers::account::acknowledge_iota_user_provisioning(self, value)
                    .await
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
        value.require_id()?;
        let (tx, rx) = oneshot::channel();
        let message_id = self.next_correlation_id().ok_or_else(|| {
            crate::OmegaError::Transport("no relay correlation id available".into())
        })?;
        match self.waiting_tasks.entry(message_id) {
            Entry::Vacant(entry) => {
                entry.insert(WaitingTask {
                    sender: tx,
                    inserted_at: Instant::now(),
                });
            }
            Entry::Occupied(_) => {
                return Err(crate::OmegaError::Transport(
                    "relay correlation id collision".into(),
                ));
            }
        }
        let outbound = value.clone().with_id(message_id);

        if let Err(error) = self.clone().send(&outbound).await {
            self.waiting_tasks.remove(&message_id);
            return Err(error);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                self.waiting_tasks.remove(&message_id);
                Err(crate::error::OmegaError::Transport(
                    "Relay response channel closed".into(),
                ))
            }
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
        self.stop_handlers().await;
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
        Some(self.id_i64())
    }
    pub fn id_i64(&self) -> i64 {
        self.id as i64
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
    let omikron_id = i64::try_from(omikron_id).ok().filter(|id| *id > 0)?;
    crate::db::omikron_repo::get_omikron_by_id(OmikronId::from(omikron_id))
        .await
        .ok()
        .map(|omikron| omikron.public_key)
}
pub async fn complete_register(_: PublicKeyBundle, _: Option<String>) -> u64 {
    0
}

fn omega_transport_policy(config: &TransportConfig) -> Policy {
    Policy {
        send_mode: SendMode::SingleStreamPerMessage,
        max_message_size: config.max_message_size,
        handshake_max_message_size: config.handshake_max_message_size,
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
        parallel_multi_stream_sends: true,
        pipe_decision_timeout: Duration::from_secs(20),
        pipe_stream_open_timeout: Duration::from_millis(500),
        request_tombstone_ttl: Duration::from_secs(30),
        request_tombstone_capacity: 200,
        pipe_tombstone_ttl: Duration::from_secs(30),
        pipe_tombstone_capacity: 200,
    }
}

pub async fn start(
    port: u16,
    state: Arc<OmegaState>,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let cert_pem = load_file_vec("certs", "cert.pem")?;
    let key_pem = load_file_vec("certs", "key.pem")?;
    let web_config = server::web::build_web_config(state.clone())?
        .serve_tcp_https(true)
        .max_tcp_connections(256);
    let ip = parse_bind_address(std::env::var("BIND_ADDRESS").ok().as_deref())?;
    let host_config = HostConfig::new(ip, port, cert_pem, key_pem)
        .with_policy(omega_transport_policy(&state.config.transport))
        .with_authentication(
            state.identity.clone_keyring()?,
            Box::new(|id, description| Box::pin(get_by_omikron_id(id, description))),
            Box::new(|key, description| Box::pin(complete_register(key, description))),
        )
        .with_authentication_policy(AuthenticationPolicy::ForceAuthentication);
    let mut server = MTPWebServer::new(host_config, web_config).await?;
    log!("OmegaServer listening on {}:{}", ip.to_string(), port);
    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = server.accept() => accepted,
        };
        let mut conn = match accepted {
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
        let config = &state.config.rate_limits;
        let peer_ip = conn.remote_addr.map(|address| address.ip());
        let active = ACTIVE_CONNECTIONS.fetch_add(1, Ordering::AcqRel) + 1;
        let peer_active = peer_ip.map(|ip| {
            let mut count = ACTIVE_CONNECTIONS_BY_IP.entry(ip).or_insert(0);
            *count += 1;
            *count
        });
        let connection_limit_guard = ConnectionLimitGuard(peer_ip);
        if active > config.transport_connections
            || peer_active.is_some_and(|count| count > config.transport_connections_per_ip)
        {
            drop(connection_limit_guard);
            log_err!(
                0,
                PrintType::Omega,
                "Rejected connection: active connection limit reached"
            );
            continue;
        }
        let authenticated = matches!(&conn.auth_state, AuthState::Authenticated);
        let Some(connection) = OmikronConnection::new(
            conn.sender.clone(),
            conn.client_id,
            conn.description.as_deref(),
            state.clone(),
            authenticated,
        ) else {
            drop(connection_limit_guard);
            log_err!(
                0,
                PrintType::Omega,
                "Rejected Omikron connection without authenticated transport state or valid capabilities"
            );
            continue;
        };
        tokio::spawn(async move {
            let _guard = connection_limit_guard;
            omikron_manager::add_omikron(connection.clone()).await;
            connection.handle(&mut conn.receiver).await;
        });
    }
    server.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DispatchClass, OmikronConnection, omega_transport_policy, parse_bind_address};
    use crate::config::TransportConfig;
    use mtp::codec::{CommunicationType, CommunicationValue};
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn relay_dispatch_is_not_on_the_ordered_state_lane() {
        let value = CommunicationValue::new(CommunicationType::Relay);
        assert_eq!(
            OmikronConnection::dispatch_class(&value),
            DispatchClass::Concurrent
        );
    }

    #[test]
    fn omega_transport_policy_uses_configured_message_limits() {
        let policy = omega_transport_policy(&TransportConfig {
            max_message_size: 123,
            handshake_max_message_size: 45,
        });

        assert_eq!(policy.max_message_size, 123);
        assert_eq!(policy.handshake_max_message_size, 45);
    }

    #[test]
    fn parses_configured_bind_address() {
        assert_eq!(
            parse_bind_address(Some("10.200.2.0")),
            Ok(IpAddr::V4(Ipv4Addr::new(10, 200, 2, 0)))
        );
    }

    #[test]
    fn presence_lifecycle_dispatch_is_ordered() {
        let value = CommunicationValue::new(CommunicationType::UserConnected);
        assert_eq!(
            OmikronConnection::dispatch_class(&value),
            DispatchClass::Ordered
        );
    }

    #[test]
    fn client_presence_changes_are_ordered() {
        let value = CommunicationValue::new(CommunicationType::ClientChanged);
        assert_eq!(
            OmikronConnection::dispatch_class(&value),
            DispatchClass::Ordered
        );
    }

    #[test]
    fn only_protocol_responses_match_waiting_tasks() {
        assert!(OmikronConnection::correlation_response(
            &CommunicationValue::new(CommunicationType::Success).with_id(1)
        ));
        assert!(OmikronConnection::correlation_response(
            &CommunicationValue::new(CommunicationType::ErrorInvalidData).with_id(1)
        ));
        assert!(!OmikronConnection::correlation_response(
            &CommunicationValue::new(CommunicationType::GetUserData).with_id(1)
        ));
    }

    #[test]
    fn defaults_bind_address_to_all_interfaces() {
        assert_eq!(
            parse_bind_address(None),
            Ok(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        );
    }
}
