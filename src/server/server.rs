use crate::server::{api, index::index_handler};
use mtp::webserver::WebServerConfig;

pub fn build_web_config() -> Result<WebServerConfig, mtp::webserver::RouterError> {
    WebServerConfig::new()
        .route("/api/download/iota_frontend", api::handle)?
        .route("/api/get/omikron", api::handle)?
        .route("/api/get/connections", api::handle)?
        .route("/api/get/public_key", api::handle)?
        .route_pattern("/api/get/omikron/{id}", api::handle_pattern)?
        .route_pattern("/api/get/iota/{id}", api::handle_pattern)?
        .route_pattern("/api/get/id/{username}", api::handle_pattern)?
        .route_pattern("/api/get/user/{id}", api::handle_pattern)?
        .route_pattern("/direct/{short}", api::handle_pattern)?
        .fallback(|_request, response| async move { index_handler(response) })
}
