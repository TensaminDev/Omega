use std::sync::Arc;

use futures::StreamExt;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message;

use crate::log;
use crate::server::omikron_connection::OmikronConnection;

pub fn handle(path: String, upgrades: OnUpgrade) {
    tokio::spawn(async move {
        match upgrades.await {
            Ok(upgraded_stream) => {
                log!("Valid WebSocket upgrade");
                let raw_stream = TokioIo::new(upgraded_stream);

                let ws_stream = WebSocketStream::from_raw_socket(
                    raw_stream,
                    tungstenite::protocol::Role::Server,
                    None,
                )
                .await;
                log!(
                    "WebSocket handshake successful, handling connection for {}",
                    path
                );

                let (writer, reader) = ws_stream.split();
                if path == "/ws/omikron" {
                    let connection = OmikronConnection::new(writer, reader);
                    start_connecteable_handler(connection).await;
                }
            }
            Err(e) => {
                log!("WebSocket upgrade failed after response: {:?}", e);
            }
        }
    });
}
pub async fn start_connecteable_handler(connection: Arc<OmikronConnection>) {
    use futures::SinkExt;
    use tokio::time::Duration;

    const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

    loop {
        let mut receiver = connection.receiver.write().await;

        match tokio::time::timeout(IDLE_TIMEOUT, receiver.next()).await {
            Ok(Some(Ok(msg))) => {
                // Drop the lock so other tasks can use the receiver if needed,
                // and so we can handle the message without holding the lock.
                drop(receiver);

                match msg {
                    Message::Text(text) => {
                        let conn_clone = connection.clone();
                        tokio::spawn(async move {
                            conn_clone.handle_message(text.to_string()).await;
                        });
                    }
                    Message::Close(_) => {
                        break;
                    }
                    Message::Pong(_) => {
                        // Received a pong, connection is alive.
                    }
                    _ => {}
                }
            }
            Ok(Some(Err(e))) => {
                log!("WS Error: {}", e);
                break;
            }
            Ok(None) => {
                // Stream is closed
                break;
            }
            Err(_) => {
                // Timeout, we need to send a ping.
                // Drop receiver lock before acquiring sender lock to avoid deadlock.
                drop(receiver);
                log!("WebSocket connection is idle. Sending a ping.");
                let mut sender = connection.sender.write().await;
                if let Err(e) = sender.send(Message::Ping(vec![])).await {
                    log!("Failed to send ping: {}. Closing connection.", e);
                    break;
                }
            }
        }
    }
    connection.handle_close().await;
}
