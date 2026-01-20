use crate::log;
use crate::server::api;
use crate::server::short_link::get_short_link;
use crate::server::socket;
use crate::util::file_util::load_file_buf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use futures_util::TryFutureExt;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::{Method, StatusCode};
use hyper::{
    Request as HttpRequest, Response as HttpResponse, body::Incoming, server::conn::http1, upgrade,
};
use hyper_util::rt::tokio::TokioIo;
use hyper_util::service::TowerToHyperService;
use pnet::datalink::NetworkInterface;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sha1::{Digest, Sha1};
use std::error::Error;
use std::io::ErrorKind;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::result::Result::Ok;
use std::sync::Arc;
use std::{future::Future, pin::Pin, time::Duration};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_rustls::TlsAcceptor;
use tower::Service;

#[derive(Clone)]
struct HttpService {
    peer_addr: SocketAddr,
}

impl Service<HttpRequest<Incoming>> for HttpService {
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
        let _peer_ip = self.peer_addr.ip();

        let (parts, body) = req.into_parts();

        let method = parts.method.clone();
        let path = parts.uri.path().to_string();
        let headers = parts.headers.clone();

        let fut = async move {
            let is_websocket_upgrade = path.starts_with("/ws")
                && method == Method::GET
                && headers
                    .get("connection")
                    .map(|v| {
                        v.to_str()
                            .unwrap_or("")
                            .split(',')
                            .any(|s| s.trim().eq_ignore_ascii_case("upgrade"))
                    })
                    .unwrap_or(false)
                && headers
                    .get("upgrade")
                    .map(|v| v.to_str().unwrap_or("").eq_ignore_ascii_case("websocket"))
                    .unwrap_or(false);

            if is_websocket_upgrade {
                log!("Attempting WebSocket upgrade on {}", path);

                if let Some(sec_websocket_key) = headers.get("sec-websocket-key") {
                    let sec_websocket_key = sec_websocket_key.to_str().unwrap_or("").to_string();
                    let sec_websocket_accept = calculate_accept_key(&sec_websocket_key);

                    let response = HttpResponse::builder()
                        .status(StatusCode::SWITCHING_PROTOCOLS)
                        .header("Upgrade", "websocket")
                        .header("Connection", "Upgrade")
                        .header("Sec-WebSocket-Accept", sec_websocket_accept)
                        .body(Full::new(Bytes::from("")))
                        .unwrap();
                    let req_for_upgrade = HttpRequest::from_parts(parts, body);
                    let upgrades = upgrade::on(req_for_upgrade);
                    log!("Handling WebSocket upgrade");

                    socket::handle(path, upgrades);

                    log!("Handled WebSocket connection initiation");
                    Ok(response)
                } else {
                    log!("No Sec-WebSocket-Key found in request headers");
                    let response = HttpResponse::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(Bytes::from("Missing Sec-WebSocket-Key")))
                        .unwrap();
                    Ok(response)
                }
            } else if path.starts_with("/api") {
                let whole_body = match body.collect().await {
                    Ok(collected) => collected,
                    Err(e) => {
                        log!("Error collecting body: {}", e);
                        return Ok(HttpResponse::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Full::new(Bytes::from(format!(
                                "Failed to read body: {}",
                                e
                            ))))
                            .unwrap());
                    }
                };
                let bytes = whole_body.to_bytes();

                let body_string: Option<String> = match String::from_utf8(bytes.to_vec()) {
                    Ok(s) => Some(s),
                    Err(_) => None,
                };

                Ok(api::handle(&path, headers.clone(), body_string).await)
            } else if path.starts_with("/direct") {
                let short = path.split('/').nth(2).unwrap_or_default();
                if let Ok(long) = get_short_link(short).await {
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
                let response = HttpResponse::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("No path provided")))
                    .unwrap();
                Ok(response)
            }
        };

        Box::pin(fut.map_err(|err: color_eyre::eyre::ErrReport| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Error in request handling: {}", err),
            )
        }))
    }
}

async fn run_http_server(port: u16) -> bool {
    let ip = "0.0.0.0".to_string();
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await;
    if let Err(e) = listener {
        log!("Failed to bind to port {}: {:?}", port, e);
        return false;
    }
    let listener = listener.unwrap();
    log!(
        "Standard Server listening for HTTP and WS on {}:{}",
        ip,
        port
    );

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    tokio::spawn(async move {
        loop {
            tokio::select! {

                accepted = listener.accept() => {
                    match accepted {
                        std::result::Result::Ok((stream, addr)) => {
                            let service = HttpService { peer_addr: addr };
                            let io = TokioIo::new(stream);

                            let mut rx = shutdown_tx.subscribe();

                            tokio::spawn(async move {
                                // Prepare the connection future
                                let conn = http1::Builder::new()
                                    .preserve_header_case(true)
                                    .title_case_headers(true)
                                    .serve_connection(io, TowerToHyperService::new(service))
                                    .with_upgrades();

                                tokio::select! {
                                    res = conn => {
                                        if let Err(err) = res {
                                            if let Some(io_err) = err.source().and_then(|e| e.downcast_ref::<io::Error>()) {
                                                if io_err.kind() != io::ErrorKind::ConnectionReset
                                                   && io_err.kind() != io::ErrorKind::BrokenPipe
                                                {
                                                    log!("Error serving connection: {:?}", err);
                                                }
                                            }
                                        }
                                    }
                                    _ = rx.recv() => {
                                        // Shutdown signal received.
                                        // Dropping the 'conn' future here closes the socket immediately.
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            log!("Error accepting connection: {:?}", e);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }
    });

    true
}

/// Runs the encrypted HTTPS/WSS server loop using the provided TLS config.
async fn run_tls_server(port: u16, tls_config: Arc<ServerConfig>) -> bool {
    let mut ip = "0.0.0.0".to_string();
    for iface in pnet::datalink::interfaces() {
        let iface: NetworkInterface = iface;
        let ipsv = format!("{}", iface.ips[0]);
        let ips: &str = ipsv.split('/').next().unwrap();
        log!("{}", ips.to_string());
        if format!("{}", ips).starts_with("10.") {
            ip = ips.to_string();
        }
    }

    let acceptor = TlsAcceptor::from(tls_config);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await;
    if let Err(e) = listener {
        log!("Failed to bind to port {}: {:?}", port, e);
        return false;
    }
    let listener = listener.unwrap();
    log!(
        "Encrypted Server listening for HTTPS and WSS on {}:{}",
        ip,
        port
    );

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = async {
                    loop {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                } => {
                    log!("Encrypted Server received shutdown signal.");
                    // Send kill signal to all active connection tasks
                    let _ = shutdown_tx.send(());
                    break;
                }

                accepted = listener.accept() => {
                    match accepted {
                        std::result::Result::Ok((stream, addr)) => {
                            let service = HttpService { peer_addr: addr };
                            let acceptor = acceptor.clone();

                            // Subscribe to the shutdown signal for this specific connection
                            let mut rx = shutdown_tx.subscribe();

                            tokio::spawn(async move {
                                // Perform TLS handshake
                                let tls_stream = match acceptor.accept(stream).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        if e.kind() != io::ErrorKind::Interrupted {
                                            log!("TLS Handshake error: {:?}", e);
                                        }
                                        return;
                                    }
                                };
                                let io = TokioIo::new(tls_stream);

                                let conn = http1::Builder::new()
                                    .preserve_header_case(true)
                                    .title_case_headers(true)
                                    .serve_connection(io, TowerToHyperService::new(service))
                                    .with_upgrades();

                                tokio::select! {
                                    res = conn => {
                                        if let Err(err) = res {
                                            if let Some(io_err) = err.source().and_then(|e| e.downcast_ref::<io::Error>()) {
                                                if io_err.kind() != io::ErrorKind::ConnectionReset
                                                   && io_err.kind() != io::ErrorKind::BrokenPipe
                                                {
                                                    log!("Error serving connection: {:?}", err);
                                                }
                                            }
                                        }
                                    }
                                    _ = rx.recv() => {
                                        // Shutdown signal received.
                                        // Dropping the 'conn' future here closes the socket immediately.
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            log!("Error accepting connection: {:?}", e);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }

        log!("Encrypted Server shutdown complete.");
    });
    true
}

pub async fn start(port: u16) -> bool {
    let tls_result = load_tls_config();

    match tls_result {
        Ok(Some(tls_config)) => run_tls_server(port, tls_config).await,
        Ok(_) => run_http_server(port).await,
        Err(e) => {
            log!("Fatal error during TLS config load: {}", e);
            false
        }
    }
}
fn calculate_accept_key(key: &str) -> String {
    let websocket_guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(websocket_guid.as_bytes());
    let result = sha1.finalize();
    STANDARD.encode(result)
}

/// Loads TLS config. Returns Ok(None) if cert files are not found, and an error if parsing fails.
fn load_tls_config() -> Result<Option<Arc<ServerConfig>>, Box<dyn Error>> {
    let cert_file_res = load_file_buf("certs", "cert.pem");
    let key_file_res = load_file_buf("certs", "cert.key");

    // Check if certificate files are present. If not, return None.
    let cert_file_buf = match cert_file_res {
        Ok(b) => b,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            log!("TLS certificate 'certs/cert.pem' not found.");
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    };

    let key_file_buf = match key_file_res {
        Ok(b) => b,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            log!("TLS key 'certs/cert.key' not found.");
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    };

    // Continue with configuration if both files were found
    let mut cert_reader = BufReader::new(cert_file_buf);
    let cert_ders = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<CertificateDer>, io::Error>>()?;

    // PKCS8
    let mut key_reader = BufReader::new(key_file_buf);
    let mut key_ders = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .map(|r| r.map(Into::into))
        .collect::<Result<Vec<PrivateKeyDer>, io::Error>>()?;

    if key_ders.is_empty() {
        // RSA
        key_reader = BufReader::new(load_file_buf("certs", "cert.key")?);
        key_ders = rustls_pemfile::rsa_private_keys(&mut key_reader)
            .map(|r| r.map(Into::into))
            .collect::<Result<Vec<PrivateKeyDer>, io::Error>>()?;
    }

    if key_ders.is_empty() {
        // EC
        key_reader = BufReader::new(load_file_buf("certs", "cert.key")?);
        key_ders = rustls_pemfile::ec_private_keys(&mut key_reader)
            .map(|r| r.map(Into::into))
            .collect::<Result<Vec<PrivateKeyDer>, io::Error>>()?;
    }

    if key_ders.is_empty() {
        return Err("No valid private keys found in key file (Tried PKCS8, RSA and EC).".into());
    }

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_ders, key_ders.remove(0))
        .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;

    Ok(Some(Arc::new(config)))
}
