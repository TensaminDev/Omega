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
use crate::identity::OmegaIdentity;
use crate::models::{IotaId, UserId};
use crate::server::{
    middleware,
    validation::{parse_positive_id, validate_non_empty},
    with_cors,
};
use crate::transport::omikron_manager::{
    get_all_connections, get_connected_omikron, get_iota_primary_omikron_connection,
    get_random_omikron,
};
use crate::util::file_util::get_directory;
use base64::Engine as _;
use bytes::Bytes;
use http::{Method, StatusCode};
use mtp::webserver::{HttpRequest, HttpResponse, RouteParams};
use std::collections::BTreeMap;
use std::sync::Arc;

fn error_body(error: &OmegaError) -> String {
    json(&StatusResponse {
        status: match error {
            OmegaError::Validation(_) => "error_bad_request",
            OmegaError::NotFound => "error_not_found",
            _ => "error",
        },
    })
}

fn user_response(user: crate::models::User) -> Result<UserResponse> {
    Ok(UserResponse {
        status: "success",
        username: user.username,
        public_key: user.public_key.try_to_base64()?,
        user_id: user.id.0,
        iota_id: user.iota_id.map(|id| id.0),
        sub_level: user.sub_level,
        sub_end: user.sub_end,
        display: user.display,
        status_message: user.status,
        about: user.about,
        avatar: user
            .avatar
            .map(|value| base64::engine::general_purpose::STANDARD.encode(value)),
    })
}

async fn route(path_parts: &[&str], identity: &OmegaIdentity) -> Result<(StatusCode, String)> {
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
                    public_key: omikron.public_key.try_to_base64()?,
                    ip_address: omikron.ip_address,
                    port: omikron.port,
                }),
            ))
        }
        ["api", "get", "omikron", id] => {
            let id = parse_positive_id(id)?;
            let omikron_id = if get_connected_omikron(id).is_some() {
                id
            } else if let Some(omikron_id) = get_iota_primary_omikron_connection(id) {
                omikron_id
            } else {
                let user = get_by_user_id(UserId::from(id)).await?;
                match user.iota_id {
                    Some(iota_id) => get_iota_primary_omikron_connection(iota_id.0),
                    None => {
                        get_random_omikron()
                            .await
                            .map_err(|_| OmegaError::NotFound)?
                            .get_omikron_id()
                            .await
                    }
                }
                .ok_or(OmegaError::NotFound)?
            };

            // Database rows describe registered Omikrons. The public discovery
            // API must expose only routes backed by a currently live transport.
            get_connected_omikron(omikron_id).ok_or(OmegaError::NotFound)?;
            let omikron = get_omikron_by_id(omikron_id.into()).await?;
            Ok((
                StatusCode::OK,
                json(&OmikronResponse {
                    status: "success",
                    id: omikron.id.0,
                    public_key: omikron.public_key.try_to_base64()?,
                    ip_address: omikron.ip_address,
                    port: omikron.port,
                }),
            ))
        }
        ["api", "get", "registration-omikron", id] => {
            let iota_id = parse_positive_id(id)?;
            get_iota_by_id(IotaId::from(iota_id)).await?;
            let omikron_id = if let Some(primary) = get_iota_primary_omikron_connection(iota_id) {
                primary
            } else {
                get_random_omikron()
                    .await
                    .map_err(|_| OmegaError::NotFound)?
                    .get_omikron_id()
                    .await
                    .ok_or(OmegaError::NotFound)?
            };
            get_connected_omikron(omikron_id).ok_or(OmegaError::NotFound)?;
            let omikron = get_omikron_by_id(omikron_id.into()).await?;
            Ok((
                StatusCode::OK,
                json(&OmikronResponse {
                    status: "success",
                    id: omikron.id.0,
                    public_key: omikron.public_key.try_to_base64()?,
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
                        .map(|(iota_id, users)| (iota_id.to_string(), users.into_iter().collect()))
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
                    public_key: iota.public_key.try_to_base64()?,
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
                    public_key: user.public_key.try_to_base64()?,
                    user_id: user.id.0,
                    iota_id: user.iota_id.map(|id| id.0),
                    sub_level: user.sub_level,
                    sub_end: user.sub_end,
                }),
            ))
        }
        ["api", "get", "public_key"] => {
            let public_key = base64::engine::general_purpose::STANDARD
                .encode(identity.public_key_bundle().try_as_bytes()?);
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
            Ok((StatusCode::OK, json(&user_response(user)?)))
        }
        _ => Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            json(&StatusResponse { status: "error" }),
        )),
    }
}

pub async fn handle(
    request: HttpRequest,
    response: HttpResponse,
    identity: Arc<OmegaIdentity>,
) -> HttpResponse {
    let method = request.method;
    let path = request.uri.path().to_string();
    if method != Method::OPTIONS && !middleware::allow(request.remote_addr.ip(), &path) {
        return with_cors(response.status(StatusCode::TOO_MANY_REQUESTS).body(json(
            &StatusResponse {
                status: "error_rate_limited",
            },
        )));
    }
    if method == Method::OPTIONS {
        return with_cors(response.status(StatusCode::NO_CONTENT));
    }
    let path_parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if let ["api", "download", "iota_frontend"] = path_parts.as_slice() {
        let file_path = format!("{}/downloads/iota_frontend.zip", get_directory());
        return match std::fs::read(file_path) {
            Ok(bytes) => with_cors(
                response
                    .status(StatusCode::OK)
                    .header("content-type", "application/zip")
                    .header(
                        "content-disposition",
                        "attachment; filename=\"iota_frontend.zip\"",
                    )
                    .body(Bytes::from(bytes)),
            ),
            Err(_) => with_cors(response.status(StatusCode::NOT_FOUND).body(json(
                &StatusResponse {
                    status: "error_not_found",
                },
            ))),
        };
    }
    if let ["direct", short @ ..] = path_parts.as_slice() {
        let short = short.join("");
        return match crate::server::short_link::get_short_link(&short).await {
            Ok(location) => with_cors(
                response
                    .status(StatusCode::TEMPORARY_REDIRECT)
                    .header("location", &location)
                    .header("cache-control", "no-store")
                    .header("referrer-policy", "no-referrer"),
            ),
            Err(_) => with_cors(
                response
                    .status(StatusCode::NOT_FOUND)
                    .header("cache-control", "no-store")
                    .header("referrer-policy", "no-referrer")
                    .body(json(&StatusResponse {
                        status: "error_not_found",
                    })),
            ),
        };
    }
    let (status, body) = route(&path_parts, &identity)
        .await
        .unwrap_or_else(|error| (error.status_code(), error_body(&error)));
    with_cors(response.status(status).body(body))
}

pub async fn handle_pattern(
    request: HttpRequest,
    response: HttpResponse,
    _params: RouteParams,
    identity: Arc<OmegaIdentity>,
) -> HttpResponse {
    handle(request, response, identity).await
}
