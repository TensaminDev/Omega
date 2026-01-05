use std::sync::Arc;

use futures::StreamExt;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tungstenite::Message;

use crate::log;
use crate::server::omikron_connection::OmikronConnection;

pub fn handle(
    path: String,
    writer: SplitSink<tokio_tungstenite::WebSocketStream<TokioIo<Upgraded>>, Message>,
    reader: SplitStream<tokio_tungstenite::WebSocketStream<TokioIo<Upgraded>>>,
) {
    log!("handling");
    tokio::spawn(async move {
        if path.starts_with("/ws/phi/") {
        } else if path.starts_with("/ws/omikron/") {
            let community_conn: Arc<OmikronConnection> =
                Arc::from(OmikronConnection::new(writer, reader));
            loop {
                let msg_result: Option<Result<_, _>> = {
                    let mut session_lock = community_conn.receiver.write().await;
                    session_lock.next().await
                };

                match msg_result {
                    Some(Ok(msg)) => {
                        if msg.is_text() {
                            let text = msg.into_text().unwrap();
                            community_conn
                                .clone()
                                .handle_message(text.to_string())
                                .await;
                        } else if msg.is_ping() {
                            let pong_response = crate::data::communication::CommunicationValue::new(
                                crate::data::communication::CommunicationType::pong,
                            );
                            community_conn.send_message(&pong_response).await;
                        } else if msg.is_close() {
                            log!("Closing: {}", msg);
                            community_conn.handle_close().await;
                            return;
                        }
                    }
                    Some(Err(e)) => {
                        log!("Closing ERR: {}", e);
                        community_conn.handle_close().await;
                        return;
                    }
                    None => {
                        log!("Closed Session me!");
                        community_conn.handle_close().await;
                        return;
                    }
                }
            }
        }
    });
}
