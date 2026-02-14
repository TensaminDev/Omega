use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use futures_util::TryFutureExt;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::server::conn::{http1, http2};
use hyper::{Method, StatusCode};
use hyper::{Request as HttpRequest, Response as HttpResponse, body::Incoming, upgrade};
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::tokio::TokioIo;
use hyper_util::service::TowerToHyperService;
use sha1::{Digest, Sha1};
use std::io;
use std::net::SocketAddr;
use std::result::Result::Ok;
use std::{future::Future, pin::Pin, time::Duration};
use tokio::net::TcpListener;
use tower::Service;

use crate::log;
use crate::server::api;
use crate::server::short_link::get_short_link;
use crate::server::socket;

// --- ApiService for HTTP/2 ---

#[derive(Clone)]
struct ApiService {
    _peer_addr: SocketAddr,
}

impl Service<HttpRequest<Incoming>> for ApiService {
    type Response = HttpResponse<Full<Bytes>>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(std::io::Result::Ok(()))
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        let (parts, body) = req.into_parts();
        let path = parts.uri.path().to_string();
        let headers = parts.headers.clone();

        let fut = async move {
            if path.starts_with("/api") {
                let whole_body =
                    tokio::time::timeout(Duration::from_secs(10), body.collect()).await??;
                let bytes = whole_body.to_bytes();
                let body_string: Option<String> = String::from_utf8(bytes.to_vec()).ok();
                Ok(api::handle(&path, headers, body_string).await)
            } else if path.starts_with("/direct") {
                let short = path.replace("/direct/", "");
                if let Ok(long) = get_short_link(&short).await {
                    let response = HttpResponse::builder()
                        .status(StatusCode::FOUND)
                        .header("Location", long)
                        .body(Full::new(Bytes::from("")))
                        .unwrap();
                    Ok(response)
                } else {
                    let response = HttpResponse::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Full::new(Bytes::from("Short link not found")))
                        .unwrap();
                    Ok(response)
                }
            } else {
                // Not a WebSocket, /api, or /direct, so it's a 404
                let response = HttpResponse::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from("Not Found")))
                    .unwrap();
                Ok(response)
            }
        };

        Box::pin(fut.map_err(|err: color_eyre::eyre::ErrReport| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Error in API request: {}", err),
            )
        }))
    }
}

// --- WebSocketService for HTTP/1.1 ---

#[derive(Clone)]
struct WebSocketService {
    _peer_addr: SocketAddr,
}

impl Service<HttpRequest<Incoming>> for WebSocketService {
    type Response = HttpResponse<Full<Bytes>>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(std::io::Result::Ok(()))
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        let (parts, body) = req.into_parts();
        let method = parts.method.clone();
        let path = parts.uri.path().to_string();
        let headers = parts.headers.clone();

        let fut = async move {
            let is_websocket_upgrade = path.starts_with("/ws")
                && method == Method::GET
                && headers
                    .get("connection")
                    .map(|v| v.to_str().unwrap_or("").contains("Upgrade"))
                    .unwrap_or(false)
                && headers.get("upgrade").map(|v| v.to_str().unwrap_or("")) == Some("websocket");

            if is_websocket_upgrade {
                log!("Attempting WebSocket upgrade on /ws");
                if let Some(sec_websocket_key) = headers.get("sec-websocket-key") {
                    let sec_websocket_key_str =
                        sec_websocket_key.to_str().unwrap_or("").to_string();
                    let sec_websocket_accept = calculate_accept_key(&sec_websocket_key_str);

                    let response = HttpResponse::builder()
                        .status(StatusCode::SWITCHING_PROTOCOLS)
                        .header("Upgrade", "websocket")
                        .header("Connection", "Upgrade")
                        .header("Sec-WebSocket-Accept", sec_websocket_accept)
                        .body(Full::new(Bytes::from("")))
                        .unwrap();

                    let req_for_upgrade = HttpRequest::from_parts(parts, body);
                    let upgrades = upgrade::on(req_for_upgrade);
                    socket::handle(path, upgrades);
                    Ok(response)
                } else {
                    log!("No Sec-WebSocket-Key found in request headers");
                    let response = HttpResponse::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(Bytes::from("Missing Sec-WebSocket-Key")))
                        .unwrap();
                    Ok(response)
                }
            } else {
                let response = HttpResponse::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from(
                        "Not Found: Use the API server for non-WebSocket requests.",
                    )))
                    .unwrap();
                Ok(response)
            }
        };

        Box::pin(fut.map_err(|_err: color_eyre::eyre::ErrReport| {
            io::Error::new(io::ErrorKind::Other, "Error in WebSocket request")
        }))
    }
}

async fn run_api_server(port: u16) -> bool {
    let mut ip = "0.0.0.0".to_string();
    for iface in pnet::datalink::interfaces() {
        if let Some(ip_net) = iface.ips.iter().find(|ip_net| {
            ip_net.is_ipv4()
                && (ip_net.ip().to_string().starts_with("10.")
                    || ip_net.ip().to_string().starts_with("192."))
        }) {
            ip = ip_net.ip().to_string();
            break;
        }
    }

    let addr = SocketAddr::new(ip.parse().unwrap(), port);
    let listener_res = TcpListener::bind(format!("0.0.0.0:{}", port)).await;
    if let Err(e) = listener_res {
        log!(
            "[FATAL] Failed to bind API server to port {}: {:?}",
            port,
            e
        );
        return false;
    }
    let listener = listener_res.unwrap();
    log!("API Server (HTTP/2) listening on {}:{}", ip, port);

    tokio::spawn(async move {
        loop {
            if let Ok((stream, addr)) = listener.accept().await {
                let service = ApiService { _peer_addr: addr };
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let conn = http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, TowerToHyperService::new(service));
                    if let Err(err) = conn.await {
                        if !err
                            .to_string()
                            .starts_with("error shutting down connection")
                        {
                            log!("API server connection error: {:?}", err);
                        }
                    }
                });
            }
        }
    });

    true
}

async fn run_websocket_server(port: u16) -> bool {
    let mut ip = "0.0.0.0".to_string();
    for iface in pnet::datalink::interfaces() {
        if let Some(ip_net) = iface.ips.iter().find(|ip_net| {
            ip_net.is_ipv4()
                && (ip_net.ip().to_string().starts_with("10.")
                    || ip_net.ip().to_string().starts_with("192."))
        }) {
            ip = ip_net.ip().to_string();
            break;
        }
    }

    let addr = SocketAddr::new(ip.parse().unwrap(), port);
    let listener_res = TcpListener::bind(format!("0.0.0.0:{}", port)).await;
    if let Err(e) = listener_res {
        log!(
            "[FATAL] Failed to bind WebSocket server to port {}: {:?}",
            port,
            e
        );
        return false;
    }
    let listener = listener_res.unwrap();
    log!("WebSocket Server (HTTP/1.1) listening on {}:{}", ip, port);

    tokio::spawn(async move {
        loop {
            if let Ok((stream, addr)) = listener.accept().await {
                let service = WebSocketService { _peer_addr: addr };
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let conn = http1::Builder::new()
                        .preserve_header_case(true)
                        .title_case_headers(true)
                        .serve_connection(io, TowerToHyperService::new(service))
                        .with_upgrades();
                    if let Err(err) = conn.await {
                        if !err
                            .to_string()
                            .starts_with("error shutting down connection")
                        {
                            log!("WebSocket server connection error: {:?}", err);
                        }
                    }
                });
            }
        }
    });

    true
}

// --- Main Start function ---

pub async fn start(api_port: u16, ws_port: u16) {
    // We are not using TLS for this setup as per the request's focus on splitting protocols.
    // The previous TLS loading logic is removed for simplicity.
    tokio::spawn(run_api_server(api_port));
    tokio::spawn(run_websocket_server(ws_port));
}

fn calculate_accept_key(key: &str) -> String {
    let websocket_guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(websocket_guid.as_bytes());
    let result = sha1.finalize();
    STANDARD.encode(result)
}
