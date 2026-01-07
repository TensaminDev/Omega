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
    loop {
        let msg = match {
            let mut receiver = connection.receiver.write().await;
            receiver.next().await
        } {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => {
                log!("WS Error: {}", e);
                break;
            }
            _ => break,
        };

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
            _ => {}
        }
    }
    connection.handle_close().await;
}
