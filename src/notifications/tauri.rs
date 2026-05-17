use crate::log;
use crate::util::file_util::load_file_vec;
use crate::util::logger::PrintType;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::futures;
use ttp_core::{CommunicationType, CommunicationValue, DataTypes, DataValue};
use ttp_native::{Host, Policy, Receiver, SendMode, Sender};

pub struct TauriConnection {
    pub user_id: i64,
    pub sender: Arc<Sender>,
}

static TAURI_CONNECTIONS: Lazy<DashMap<i64, Vec<Arc<TauriConnection>>>> = Lazy::new(DashMap::new);

pub async fn start(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let cert_pem = load_file_vec("certs", "transport_cert.pem").expect("Error loading Pemfile");
    let key_pem = load_file_vec("certs", "transport_key.pem").expect("Error loading Keyfile");

    let mut host: Host = ttp_native::host(
        port,
        cert_pem,
        key_pem,
        Policy {
            send_mode: SendMode::SingleStreamPerMessage,
            max_message_size: 1_000_000_000,
            close_frame_len: u32::MAX,
            application_close_code: 0,
            open_stream_timeout: Duration::from_millis(2_000),
            write_timeout: Duration::from_millis(2_000),
            accept_stream_timeout: Duration::from_millis(10_000),
            read_timeout: Duration::from_millis(30_000),
            keep_alive_interval: Some(Duration::from_secs(6)),
            max_idle_timeout: Some(Duration::from_secs(30)),
            force_close_delay: Duration::from_millis(300),
            max_transient_recv_errors: 20,
            transient_recv_backoff: Duration::from_millis(100),
            receiver_queue_capacity: 1000,
        },
    )
    .await?;

    log!("TauriServer listening on port {}", port);

    while let Some((sender, mut receiver)) = host.next().await {
        tokio::spawn(async move {
            handle_connection(sender, &mut receiver).await;
        });
    }

    Ok(())
}

async fn handle_connection(sender: Sender, receiver: &mut Receiver) {
    let mut current_user_id = 0;
    let sender = Arc::new(sender);

    while let Ok(cv) = receiver.receive().await {
        match cv.get_type() {
            CommunicationType::tauri_identification => {
                let user_id = cv.get_data(DataTypes::user_id).as_number().unwrap_or(0);
                if user_id != 0 {
                    current_user_id = user_id;
                    let conn = Arc::new(TauriConnection {
                        user_id,
                        sender: sender.clone(),
                    });

                    TAURI_CONNECTIONS
                        .entry(user_id)
                        .and_modify(|conns| {
                            conns.retain(|c| !Arc::ptr_eq(&c.sender.handle(), &sender.handle()));
                            conns.push(conn.clone());
                        })
                        .or_insert_with(|| vec![conn]);

                    let response =
                        CommunicationValue::new(CommunicationType::success).with_id(cv.get_id());
                    if let Err(e) = sender.send(&response).await {
                        log!(
                            PrintType::General,
                            "Failed to send tauri success response: {}",
                            e
                        );
                    } else {
                        log!(user_id, PrintType::Client, "Tauri device registered");
                    }
                }
            }
            CommunicationType::ping => {
                let response =
                    CommunicationValue::new(CommunicationType::pong).with_id(cv.get_id());
                if let Err(_) = sender.send(&response).await {
                    break;
                }
            }
            _ => {}
        }
    }

    // Cleanup
    if current_user_id != 0 {
        if let Some(mut conns) = TAURI_CONNECTIONS.get_mut(&current_user_id) {
            conns.retain(|c| !Arc::ptr_eq(c.sender.handle(), sender.handle()));
        }
    }
}

pub async fn send_notification(user_id: i64, sender_id: i64) {
    let conns_opt = {
        let entry = TAURI_CONNECTIONS.get(&user_id);
        entry.map(|e| e.clone())
    };

    if let Some(conns) = conns_opt {
        let cv = CommunicationValue::new(CommunicationType::push_notification)
            .add_data(DataTypes::sender_id, DataValue::Number(sender_id));

        let mut remove_needed = false;
        for conn in conns.iter() {
            if conn.sender.send(&cv).await.is_err() {
                remove_needed = true;
            }
        }

        if remove_needed {
            if let Some(mut conns_mut) = TAURI_CONNECTIONS.get_mut(&user_id) {
                conns_mut.retain(|conn| conn.sender.is_open());
            }
        }
    }
}

pub async fn remove_notification(user_id: i64, sender_id: i64) {
    let conns_opt = {
        let entry = TAURI_CONNECTIONS.get(&user_id);
        entry.map(|e| e.clone())
    };

    if let Some(conns) = conns_opt {
        let cv = CommunicationValue::new(CommunicationType::read_notification)
            .add_data(DataTypes::sender_id, DataValue::Number(sender_id));

        let mut remove_needed = false;
        for conn in conns.iter() {
            if conn.sender.send(&cv).await.is_err() {
                remove_needed = true;
            }
        }

        if remove_needed {
            if let Some(mut conns_mut) = TAURI_CONNECTIONS.get_mut(&user_id) {
                conns_mut.retain(|conn| conn.sender.is_open());
            }
        }
    }
}
