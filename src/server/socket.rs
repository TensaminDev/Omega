use std::sync::Arc;

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use futures::StreamExt;

use crate::data::communication::{CommunicationType, CommunicationValue};
use crate::log;
use crate::server::omikron_connection::OmikronConnection;

pub fn handle(path: String, upgrades: WebSocket) {
    tokio::spawn(async move {
        log!(
            "[ws] Spawning new task to handle WebSocket upgrade for path: {}",
            path
        );
        log!("[ws] WebSocket upgrade successful for path: {}", path);

        log!(
            "[ws] WebSocket handshake successful, handling connection for {}",
            path
        );

        let (writer, reader) = upgrades.split();
        if path == "/ws/omikron" {
            let connection = OmikronConnection::new(writer, reader);
            tokio::spawn(start_connecteable_handler(connection));
        }

        log!(
            "[ws] WebSocket handling task for path: {} is finished.",
            path
        );
    });
}
pub async fn start_connecteable_handler(connection: Arc<OmikronConnection>) {
    use futures::SinkExt;
    use tokio::time::Duration;

    const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

    log!("[ws_handler] Starting connection handler loop.");
    loop {
        let mut receiver_guard = connection.receiver.write().await;

        match tokio::time::timeout(IDLE_TIMEOUT, receiver_guard.next()).await {
            Ok(Some(Ok(msg))) => {
                drop(receiver_guard);

                match msg {
                    Message::Text(text) => {
                        let conn_clone = connection.clone();
                        tokio::spawn(async move {
                            conn_clone.handle_message(text.to_string()).await;
                        });
                    }
                    Message::Close(_) => {
                        log!("[ws_handler] Received 'Close' message. Breaking loop.");
                        break;
                    }
                    Message::Pong(_) => {
                        log!("[ws_handler] Received 'Pong'. Connection is alive.");
                    }
                    _ => {
                        log!("[ws_handler] Received unhandled message type.");
                    }
                }
            }
            Ok(Some(Err(e))) => {
                log!("[ERROR] WS Error: {}. Breaking loop.", e);
                break;
            }
            Ok(_) => {
                log!("[ws_handler] WebSocket stream closed by peer. Breaking loop.");
                break;
            }
            Err(_) => {
                drop(receiver_guard);
                log!("[ws_handler] Timeout: Dropped receiver lock. Sending a ping.");
                let mut sender = connection.sender.write().await;
                log!("[ws_handler] Acquired sender lock for ping.");
                if let Err(e) = sender
                    .send(Message::Text(Utf8Bytes::from(
                        CommunicationValue::new(CommunicationType::ping)
                            .to_json()
                            .to_string(),
                    )))
                    .await
                {
                    log!("[ERROR] Failed to send ping: {}. Closing connection.", e);
                    break;
                }
                log!("[ws_handler] Ping sent successfully.");
            }
        }
    }
    log!("[ws_handler] Connection handler loop finished.");
    connection.handle_close().await;
    log!("[ws_handler] Connection closed.");
}
