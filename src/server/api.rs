use crate::api::response::{
    ConnectionsResponse, IotaResponse, OmikronResponse, PublicKeyResponse, StatusResponse,
    UserResponse, UsernameResponse, json,
};
use crate::db::{
    iota_repo::get_iota_by_id,
    omikron_repo::get_omikron_by_id,
    user_repo::{get_by_user_id, get_by_username},
};
use crate::error::{OmegaError, Result};
use crate::load_keyring;
use crate::models::UserId;
use crate::server::{
    middleware,
    validation::{parse_positive_id, validate_non_empty},
};
use crate::sql::user_online_tracker::{get_all_connections, get_iota_primary_omikron_connection};
use crate::transport::omikron_manager::get_random_omikron;
use crate::util::file_util::get_directory;
use base64::Engine as _;
use bytes::Bytes;
use http::{Method, StatusCode};
use mtp::webserver::{Http3Request, Http3Response, RouteParams};
use std::collections::BTreeMap;

fn error_body(error: &OmegaError) -> String {
    json(&StatusResponse {
        status: match error {
            OmegaError::Validation(_) => "error_bad_request",
            OmegaError::NotFound => "error_not_found",
            _ => "error",
        },
    })
}

fn user_response(user: crate::models::User) -> UserResponse {
    UserResponse {
        status: "success",
        username: user.username,
        public_key: user.public_key.to_base64(),
        user_id: user.id.0,
        iota_id: user.iota_id.0,
        sub_level: user.sub_level,
        sub_end: user.sub_end,
        display: user.display,
        status_message: user.status,
        about: user.about,
        avatar: user
            .avatar
            .map(|value| base64::engine::general_purpose::STANDARD.encode(value)),
    }
}

async fn route(path_parts: &[&str]) -> Result<(StatusCode, String)> {
    match path_parts {
        ["api", "get", "omikron"] => {
            let connection = get_random_omikron()
                .await
                .map_err(|_| OmegaError::NotFound)?;
            let id = connection
                .get_omikron_id()
                .await
                .ok_or(OmegaError::NotFound)?;
            let omikron = get_omikron_by_id(id.into()).await?;
            Ok((
                StatusCode::OK,
                json(&OmikronResponse {
                    status: "success",
                    id,
                    public_key: omikron.public_key.to_base64(),
                    ip_address: omikron.ip_address,
                    port: omikron.port,
                }),
            ))
        }
        ["api", "get", "omikron", id] => {
            let id = parse_positive_id(id)?;
            let omikron = match get_omikron_by_id(id.into()).await {
                Ok(value) => value,
                Err(_) => {
                    let fallback_id =
                        if let Some(fallback_id) = get_iota_primary_omikron_connection(id) {
                            fallback_id
                        } else {
                            let user = get_by_user_id(UserId::from(id)).await?;
                            get_iota_primary_omikron_connection(user.iota_id.0)
                                .ok_or(OmegaError::NotFound)?
                        };
                    get_omikron_by_id(fallback_id.into()).await?
                }
            };
            Ok((
                StatusCode::OK,
                json(&OmikronResponse {
                    status: "success",
                    id: omikron.id.0,
                    public_key: omikron.public_key.to_base64(),
                    ip_address: omikron.ip_address,
                    port: omikron.port,
                }),
            ))
        }
        ["api", "get", "connections"] => {
            let connections = get_all_connections()
                .await
                .map_err(|_| OmegaError::Transport("failed to load connections".to_string()))?;
            let connections = connections
                .into_iter()
                .map(|(omikron_id, iotas)| {
                    let iotas = iotas
                        .into_iter()
                        .map(|(iota_id, users)| {
                            (
                                iota_id.to_string(),
                                users.into_iter().map(i64::from).collect(),
                            )
                        })
                        .collect();
                    (omikron_id.to_string(), iotas)
                })
                .collect::<BTreeMap<_, _>>();
            Ok((
                StatusCode::OK,
                json(&ConnectionsResponse {
                    status: "success",
                    connections,
                }),
            ))
        }
        ["api", "get", "iota", id] => {
            let id = parse_positive_id(id)?;
            let iota = get_iota_by_id(id.into()).await?;
            Ok((
                StatusCode::OK,
                json(&IotaResponse {
                    status: "success",
                    iota_id: iota.id.0,
                    public_key: iota.public_key.to_base64(),
                }),
            ))
        }
        ["api", "get", "id", username] => {
            validate_non_empty(username, "username", 15)?;
            let user = get_by_username(username).await?;
            Ok((
                StatusCode::OK,
                json(&UsernameResponse {
                    status: "success",
                    username: user.username,
                    public_key: user.public_key.to_base64(),
                    user_id: user.id.0,
                    iota_id: user.iota_id.0,
                    sub_level: user.sub_level,
                    sub_end: user.sub_end,
                }),
            ))
        }
        ["api", "get", "public_key"] => {
            let public_key = base64::engine::general_purpose::STANDARD
                .encode(load_keyring().public_key_bundle().as_bytes());
            Ok((
                StatusCode::OK,
                json(&PublicKeyResponse {
                    status: "success",
                    public_key,
                }),
            ))
        }
        ["api", "get", "user", id] => {
            let id = parse_positive_id(id)?;
            let user = get_by_user_id(UserId::from(id)).await?;
            Ok((StatusCode::OK, json(&user_response(user))))
        }
        _ => Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            json(&StatusResponse { status: "error" }),
        )),
    }
}

pub async fn handle(request: Http3Request, response: Http3Response) -> Http3Response {
    let method = request.method;
    let path = request.uri.path().to_string();
    if method != Method::OPTIONS && !middleware::allow(request.remote_addr.ip(), &path) {
        return response
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header("access-control-allow-origin", &crate::config::cors_origin())
            .body(json(&StatusResponse {
                status: "error_rate_limited",
            }));
    }
    if method == Method::OPTIONS {
        return response
            .status(StatusCode::OK)
            .header("access-control-allow-origin", &crate::config::cors_origin())
            .header("access-control-allow-methods", "GET, POST, OPTIONS")
            .header("access-control-allow-headers", "*");
    }
    let path_parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if let ["api", "download", "iota_frontend"] = path_parts.as_slice() {
        let file_path = format!("{}/downloads/iota_frontend.zip", get_directory());
        return match std::fs::read(file_path) {
            Ok(bytes) => response
                .status(StatusCode::OK)
                .header("access-control-allow-origin", &crate::config::cors_origin())
                .header("content-type", "application/zip")
                .header(
                    "content-disposition",
                    "attachment; filename=\"iota_frontend.zip\"",
                )
                .body(Bytes::from(bytes)),
            Err(_) => response
                .status(StatusCode::NOT_FOUND)
                .header("access-control-allow-origin", &crate::config::cors_origin())
                .body(json(&StatusResponse {
                    status: "error_not_found",
                })),
        };
    }
    if let ["direct", short @ ..] = path_parts.as_slice() {
        let short = short.join("");
        let location = crate::server::short_link::get_short_link(&short)
            .await
            .unwrap_or_else(|_| "https://tensamin.net".to_string());
        return response
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header("location", &location);
    }
    let (status, body) = route(&path_parts)
        .await
        .unwrap_or_else(|error| (error.status_code(), error_body(&error)));
    response
        .status(status)
        .header("access-control-allow-origin", &crate::config::cors_origin())
        .header("access-control-allow-headers", "*")
        .header("access-control-allow-methods", "GET, POST, OPTIONS")
        .body(body)
}

pub async fn handle_pattern(
    request: Http3Request,
    response: Http3Response,
    _params: RouteParams,
) -> Http3Response {
    handle(request, response).await
}
