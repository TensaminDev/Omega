use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::{Actor, ActorContext, AsyncContext, StreamHandler};
use actix_web_actors::ws;

use crate::data::communication::{CommunicationType, CommunicationValue};
use crate::log;
use crate::server::omikron_connection::OmikronConnection;

const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

use actix::Message;

#[derive(Message)]
#[rtype(result = "()")]
pub struct WsSendMessage(pub String);

impl actix::Handler<WsSendMessage> for WsSession {
    type Result = ();

    fn handle(&mut self, msg: WsSendMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

pub struct WsSession {
    path: String,
    last_heartbeat: Instant,
    omikron: Option<Arc<OmikronConnection>>,
}

impl WsSession {
    pub fn new(path: String) -> Self {
        Self {
            path,
            last_heartbeat: Instant::now(),
            omikron: None,
        }
    }

    fn start_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(Duration::from_secs(5), |act, ctx| {
            if Instant::now().duration_since(act.last_heartbeat) > IDLE_TIMEOUT {
                log!("[ws_handler] Heartbeat failed. Disconnecting.");
                ctx.close(None);
                ctx.stop();
                return;
            }

            let ping = CommunicationValue::new(CommunicationType::ping)
                .to_json()
                .to_string();

            ctx.text(ping);
        });
    }
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_heartbeat(ctx);

        if self.path == "omikron" {
            let addr = ctx.address();
            let connection = OmikronConnection::new(addr);
            self.omikron = Some(connection);
        }
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        log!(
            "[ws] WebSocket handling task for path: {} is finished.",
            self.path
        );

        if let Some(conn) = &self.omikron {
            let conn = conn.clone();
            actix_rt::spawn(async move {
                conn.handle_close().await;
            });
        }
    }
}
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Text(text)) => {
                self.last_heartbeat = Instant::now();

                if let Some(conn) = &self.omikron {
                    let conn_clone = conn.clone();
                    actix_rt::spawn(async move {
                        conn_clone.handle_message(text.to_string()).await;
                    });
                }
            }

            Ok(ws::Message::Ping(msg)) => {
                self.last_heartbeat = Instant::now();
                ctx.pong(&msg);
            }

            Ok(ws::Message::Pong(_)) => {
                self.last_heartbeat = Instant::now();
                log!("[ws_handler] Received Pong. Connection alive.");
            }

            Ok(ws::Message::Close(reason)) => {
                log!("[ws_handler] Received Close. Disconnecting.");
                ctx.close(reason);
                ctx.stop();
            }

            Ok(ws::Message::Binary(_)) => {
                log!("[ws_handler] Binary message ignored.");
            }

            Err(e) => {
                log!("[ERROR] WS Error: {}. Closing.", e);
                ctx.stop();
            }

            _ => {}
        }
    }
}
