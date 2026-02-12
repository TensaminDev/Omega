use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, OriginalUri, Path, ws::WebSocketUpgrade},
    response::{IntoResponse, Redirect},
    routing::get,
};

use pnet::datalink::NetworkInterface;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;

use crate::log;
use crate::server::api;
use crate::server::short_link::get_short_link;
use crate::server::socket;

pub async fn start(port: u16) -> bool {
    let app = Router::new()
        .route("/ws/omikron", get(ws_handler))
        .route("/direct/{short}", get(direct_handler))
        .fallback(fallback_handler);
    run_http_server(port, app).await
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    OriginalUri(uri): OriginalUri,
    ConnectInfo(_): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    log!("Attempting WebSocket upgrade on {}", uri.path());
    let path = uri.path().to_string();

    ws.on_upgrade(async move |socket| socket::handle(path, socket))
}

async fn direct_handler(Path(short): Path<String>) -> impl IntoResponse {
    match get_short_link(&short).await {
        Ok(long) => Redirect::temporary(&long),
        Err(_) => Redirect::temporary("https://tensamin.net"),
    }
}

async fn fallback_handler(
    OriginalUri(uri): OriginalUri,
    headers: axum::http::HeaderMap,
    body: Body,
) -> impl IntoResponse {
    let path = uri.path().to_string();

    let whole_body = tokio::time::timeout(
        Duration::from_secs(10),
        axum::body::to_bytes(body, 1024 * 1024 * 10),
    )
    .await;

    let body_string = match whole_body {
        Ok(Ok(bytes)) => String::from_utf8(bytes.to_vec()).ok(),
        _ => None,
    };

    api::handle(&path, headers, body_string).await
}

async fn run_http_server(port: u16, app: Router) -> bool {
    let ip = find_local_ip();
    let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(l) => l,
        Err(e) => {
            log!("Failed to bind to port {}: {:?}", port, e);
            return false;
        }
    };

    log!(
        "Standard Server listening for HTTP and WS on {}:{}",
        ip,
        port
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map(|_| true)
    .unwrap_or_else(|e| {
        log!("Server error: {}", e);
        false
    })
}

fn find_local_ip() -> String {
    for iface in pnet::datalink::interfaces() {
        let iface: NetworkInterface = iface;
        if !iface.ips.is_empty() {
            let ipsv = format!("{}", iface.ips[0]);
            let ips: &str = ipsv.split('/').next().unwrap();
            if ips.starts_with("10.") || ips.starts_with("192.") {
                return ips.to_string();
            }
        }
    }
    "0.0.0.0".to_string()
}
