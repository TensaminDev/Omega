use crate::{
    log,
    server::{api, short_link::get_short_link},
    util::file_util::get_directory,
};

use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, http::header, web};

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, pkcs8_private_keys};

use std::fs::File;
use std::io::BufReader;

pub async fn start(port: u16) -> anyhow::Result<()> {
    let mut cert_reader =
        BufReader::new(File::open(format!("{}/certs/cert.pem", get_directory()))?);

    let mut key_reader = BufReader::new(File::open(format!("{}/certs/key.pem", get_directory()))?);

    let cert_chain: Vec<CertificateDer<'static>> =
        certs(&mut cert_reader).collect::<Result<_, _>>()?;

    let mut keys: Vec<PrivateKeyDer<'static>> = pkcs8_private_keys(&mut key_reader)
        .map(|res| res.map(Into::into))
        .collect::<Result<_, _>>()?;

    let key = keys.remove(0);

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)?;

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let addr = format!("0.0.0.0:{port}");
    log!("  Server on {}", addr);

    HttpServer::new(move || {
        App::new()
            .route("/api/{path:.*}", web::to(api_handler))
            .route("/direct/{path:.*}", web::to(direct_handler))
    })
    .bind_rustls_0_23(addr, config)?
    .run()
    .await?;

    Ok(())
}
async fn direct_handler(req: HttpRequest) -> impl Responder {
    let path = req.uri().path().to_string();
    let short = path.replace("/direct/", "");

    if let Ok(long) = get_short_link(&short).await {
        HttpResponse::TemporaryRedirect()
            .append_header((header::LOCATION, long))
            .finish()
    } else {
        HttpResponse::TemporaryRedirect()
            .append_header((header::LOCATION, "https://tensamin.net"))
            .finish()
    }
}

async fn api_handler(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    let path = req.uri().path().to_string();
    let body_string = String::from_utf8_lossy(&body).to_string();

    api::handle(&path, Some(body_string)).await
}
