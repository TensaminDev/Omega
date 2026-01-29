use crate::data::communication::{CommunicationType, CommunicationValue, DataTypes};
use crate::get_public_key;
use crate::server::omikron_manager::get_random_omikron;
use crate::sql::sql;
use crate::sql::user_online_tracker::{
    get_iota_omikron_connections, get_iota_primary_omikron_connection,
};
use crate::{
    sql::sql::{get_by_user_id, get_omikron_by_id},
    util::crypto_helper::public_key_to_base64,
};
use axum::http::HeaderValue;
use base64::Engine as _;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{HeaderMap, Response as HttpResponse, StatusCode};
use json::JsonValue;
use json::number::Number;

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
    let (status, content, body_text) = if path_parts.len() >= 2 {
        match path_parts[1] {
            "get" => match path_parts[2] {
                // api/get/omikron -> any omikron
                // api/get/omikron/<id> -> omikron for id (user / iota / omikron)
                "omikron" => {
                    if path_parts.len() == 3 {
                        if let Ok(omikron_conn) = get_random_omikron().await {
                            if let Ok((public_key, ip_address)) =
                                sql::get_omikron_by_id(omikron_conn.get_omikron_id().await).await
                            {
                                (
                                    StatusCode::OK,
                                    "application/json",
                                    format!(
                                        "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                                        omikron_conn.get_omikron_id().await,
                                        public_key,
                                        ip_address
                                    ),
                                )
                            } else {
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "text/plain",
                                    "selected an invalid omikron".to_string(),
                                )
                            }
                        } else {
                            (
                                StatusCode::NOT_FOUND,
                                "text/plain",
                                "couldn't find online omikron".to_string(),
                            )
                        }
                    } else if path_parts.len() == 4 {
                        let id = path_parts[3].parse::<i64>().unwrap_or(0);
                        if id == 0 {
                            not_found()
                        } else if let Ok((public_key, ip_address)) = get_omikron_by_id(id).await {
                            (
                                StatusCode::OK,
                                "application/json",
                                format!(
                                    "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                                    id, public_key, ip_address
                                ),
                            )
                        } else if let Some(omikron_id) = get_iota_primary_omikron_connection(id) {
                            if let Ok((public_key, ip_address)) =
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
                            get_by_user_id(id).await
                        {
                            if let Some(omikron_id) = get_iota_primary_omikron_connection(iota_id) {
                                if let Ok((public_key, ip_address)) =
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
                    if path_parts.len() != 4 {
                        bad_request()
                    } else {
                        let username = path_parts[3];
                        if username.is_empty() {
                            not_found()
                        } else {
                            if let Ok((
                                id,
                                iota_id,
                                username,
                                _,
                                _,
                                _,
                                _,
                                sub_level,
                                sub_end,
                                public_key,
                                _,
                                _,
                            )) = sql::get_by_username(username).await
                            {
                                let cv = CommunicationValue::new(CommunicationType::success)
                                    .add_data_str(DataTypes::username, username)
                                    .add_data_str(DataTypes::public_key, public_key)
                                    .add_data(
                                        DataTypes::user_id,
                                        JsonValue::Number(Number::from(id)),
                                    )
                                    .add_data(
                                        DataTypes::iota_id,
                                        JsonValue::Number(Number::from(iota_id)),
                                    )
                                    .add_data(
                                        DataTypes::sub_level,
                                        JsonValue::Number(Number::from(sub_level)),
                                    )
                                    .add_data(
                                        DataTypes::sub_end,
                                        JsonValue::Number(Number::from(sub_end)),
                                    );
                                (StatusCode::OK, "application/json", cv.to_json().to_string())
                            } else {
                                (
                                    StatusCode::OK,
                                    "application/json",
                                    CommunicationValue::new(CommunicationType::error_not_found)
                                        .to_json()
                                        .to_string(),
                                )
                            }
                        }
                    }
                }
                "public_key" => (
                    StatusCode::OK,
                    "application/json",
                    public_key_to_base64(&get_public_key()),
                ),
                "user" => {
                    if path_parts.len() != 4 {
                        bad_request()
                    } else {
                        let id = path_parts[3];
                        let id: i64 = id.parse().unwrap_or(0);
                        if id == 0 {
                            bad_request()
                        } else {
                            if let Ok((
                                id,
                                iota_id,
                                username,
                                display,
                                status,
                                about,
                                avatar,
                                sub_level,
                                sub_end,
                                public_key,
                                _,
                                _,
                            )) = sql::get_by_user_id(id).await
                            {
                                let mut cv = CommunicationValue::new(CommunicationType::success)
                                    .add_data_str(DataTypes::username, username)
                                    .add_data_str(DataTypes::public_key, public_key)
                                    .add_data(
                                        DataTypes::user_id,
                                        JsonValue::Number(Number::from(id)),
                                    )
                                    .add_data(
                                        DataTypes::iota_id,
                                        JsonValue::Number(Number::from(iota_id)),
                                    )
                                    .add_data(
                                        DataTypes::sub_level,
                                        JsonValue::Number(Number::from(sub_level)),
                                    )
                                    .add_data(
                                        DataTypes::sub_end,
                                        JsonValue::Number(Number::from(sub_end)),
                                    );
                                if let Some(display) = display {
                                    cv = cv.add_data_str(DataTypes::display, display);
                                }
                                if let Some(status) = status {
                                    cv = cv.add_data_str(DataTypes::status, status);
                                }
                                if let Some(about) = about {
                                    cv = cv.add_data_str(DataTypes::about, about);
                                }
                                if let Some(avatar) = avatar {
                                    cv = cv.add_data_str(
                                        DataTypes::avatar,
                                        base64::engine::general_purpose::STANDARD.encode(avatar),
                                    );
                                }
                                (StatusCode::OK, "application/json", cv.to_json().to_string())
                            } else {
                                (
                                    StatusCode::OK,
                                    "application/json",
                                    CommunicationValue::new(CommunicationType::error_not_found)
                                        .to_json()
                                        .to_string(),
                                )
                            }
                        }
                    }
                }
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
