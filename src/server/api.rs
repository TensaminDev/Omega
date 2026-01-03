use crate::{get_public_key, log};
use crate::{
    sql::{
        iota_omikron_tracker::{get_omikron_for_iota, track_iota_omikron, untrack_iota},
        sql::{
            change_about, change_avatar, change_display_name, change_iota_id, change_iota_key,
            change_keys, change_status, change_username, get_by_id, get_by_username,
            get_iota_by_id, get_omikron_by_id, get_random_omikron, get_register_id,
            register_complete_iota, register_complete_user,
        },
    },
    util::crypto_helper::public_key_to_base64,
};
use axum::http::HeaderValue;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{HeaderMap, Response as HttpResponse, StatusCode};
use json::JsonValue;

pub async fn handle(
    path: &str,
    headers: HeaderMap<HeaderValue>,
    body_string: Option<String>,
) -> HttpResponse<Full<Bytes>> {
    let path_parts: Vec<&str> = path.split("/").filter(|s| !s.is_empty()).collect();

    let body: Option<JsonValue> = if body_string.is_some() {
        if let Ok(body_json) = json::parse(&body_string.unwrap()) {
            Some(body_json)
        } else {
            None
        }
    } else {
        None
    };
    // api/
    //   get/
    //     omikron/
    //     id/
    //   register/
    //     innit/
    //     complete
    log!("{}", path);
    log!("{:?} .len = {}", path_parts, path_parts.len());
    let (status, content, body_text) = if path_parts.len() >= 2 {
        match path_parts[1] {
            "get" => match path_parts[2] {
                // api/get/omikron -> any omikron
                // api/get/omikron/<id> -> omikron for id (user / iota / omikron)
                "omikron" => {
                    if path_parts.len() == 3 {
                        if let Ok((id, public_key, ip_address)) = get_random_omikron().await {
                            (
                                StatusCode::OK,
                                "application/json",
                                format!(
                                    "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                                    id, public_key, ip_address
                                ),
                            )
                        } else {
                            not_found()
                        }
                    } else if path_parts.len() == 4 {
                        let id = path_parts[3].parse::<i64>().unwrap_or(0);
                        if id == 0 {
                            not_found()
                        } else if let Ok((omikron_id, public_key, ip_address)) =
                            get_omikron_by_id(id).await
                        {
                            (
                                StatusCode::OK,
                                "application/json",
                                format!(
                                    "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                                    omikron_id, public_key, ip_address
                                ),
                            )
                        } else if let Some(omikron_id) = get_omikron_for_iota(id).await {
                            if let Ok((omikron_id, public_key, ip_address)) =
                                get_omikron_by_id(omikron_id).await
                            {
                                (
                                    StatusCode::OK,
                                    "application/json",
                                    format!(
                                        "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                                        omikron_id, public_key, ip_address
                                    ),
                                )
                            } else {
                                not_found()
                            }
                        } else if let Ok((_, iota_id, _, _, _, _, _, _, _, _, _, _)) =
                            get_by_id(id).await
                        {
                            if let Some(omikron_id) = get_omikron_for_iota(iota_id).await {
                                if let Ok((omikron_id, public_key, ip_address)) =
                                    get_omikron_by_id(omikron_id).await
                                {
                                    (
                                        StatusCode::OK,
                                        "application/json",
                                        format!(
                                            "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                                            omikron_id, public_key, ip_address
                                        ),
                                    )
                                } else {
                                    not_found()
                                }
                            } else {
                                not_found()
                            }
                        } else {
                            not_found()
                        }
                    } else {
                        bad_request()
                    }
                }
                // get/id/<username>
                "id" => {
                    let username = path_parts[3];
                    bad_request()
                }
                "public_key" => (
                    StatusCode::OK,
                    "application/json",
                    public_key_to_base64(&get_public_key()),
                ),
                _ => {
                    let id = path_parts[2];
                    let id: i64 = id.parse().unwrap_or(0);
                    if id == 0 {
                        bad_request()
                    } else {
                        bad_request()
                    }
                }
            },
            _ => not_found(),
        }
    } else {
        not_found()
    };
    let body = Full::new(Bytes::from(body_text.to_string()));
    HttpResponse::builder().status(status).body(body).unwrap()
}
pub fn bad_request() -> (StatusCode, &'static str, String) {
    (
        StatusCode::BAD_REQUEST,
        "text/text",
        "400 Bad Request".to_string(),
    )
}
pub fn unauthorized() -> (StatusCode, &'static str, String) {
    (
        StatusCode::UNAUTHORIZED,
        "text/text",
        "401 Unauthorized".to_string(),
    )
}
pub fn forbidden() -> (StatusCode, &'static str, String) {
    (
        StatusCode::FORBIDDEN,
        "text/text",
        "403 Forbidden".to_string(),
    )
}
pub fn not_found() -> (StatusCode, &'static str, String) {
    (
        StatusCode::NOT_FOUND,
        "text/text",
        "404 Not Found".to_string(),
    )
}
