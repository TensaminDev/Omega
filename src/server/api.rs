use crate::data::communication::{CommunicationType, CommunicationValue, DataTypes};
use crate::get_public_key;
use crate::server::omikron_manager::get_random_omikron;
use crate::sql::sql;
use crate::sql::user_online_tracker::get_iota_primary_omikron_connection;
use crate::{
    sql::sql::{get_by_user_id, get_omikron_by_id},
    util::crypto_helper::public_key_to_base64,
};
use actix_web::HttpResponse;
use actix_web::http::{StatusCode, header};
use base64::Engine as _;
use json::JsonValue;
use json::number::Number;

pub async fn handle(path: &str, body_string: Option<String>) -> HttpResponse {
    if path == "OPTIONS" {
        return HttpResponse::Ok()
            .insert_header(("Access-Control-Allow-Origin", "*"))
            .insert_header(("Access-Control-Allow-Methods", "GET, POST, OPTIONS"))
            .insert_header(("Access-Control-Allow-Headers", "*"))
            .finish();
    }

    let path_parts: Vec<&str> = path.split("/").filter(|s| !s.is_empty()).collect();

    let _body: Option<JsonValue> = if body_string.is_some() {
        if let Ok(body_json) = json::parse(&body_string.unwrap()) {
            Some(body_json)
        } else {
            None
        }
    } else {
        None
    };

    let (status, body_text) = match path_parts.as_slice() {
        ["api", "download", "iota_frontend"] => {
            let file_path = "downloads/[iota_frontend].zip";

            match std::fs::read(file_path) {
                Ok(file_bytes) => {
                    return HttpResponse::Ok()
                        .insert_header(("Access-Control-Allow-Origin", "*"))
                        .insert_header(("Content-Type", "application/zip"))
                        .insert_header((
                            "Content-Disposition",
                            "attachment; filename=\"iota_frontend.zip\"",
                        ))
                        .body(file_bytes);
                }
                Err(_) => {
                    return HttpResponse::NotFound()
                        .insert_header(("Access-Control-Allow-Origin", "*"))
                        .body("File not found");
                }
            }
        }
        ["api", "get", "omikron"] => {
            if let Ok(omikron_conn) = get_random_omikron().await {
                if let Ok((public_key, ip_address)) =
                    sql::get_omikron_by_id(omikron_conn.get_omikron_id().await).await
                {
                    (
                        StatusCode::OK,
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
                        "selected an invalid omikron".to_string(),
                    )
                }
            } else {
                (
                    StatusCode::NOT_FOUND,
                    "couldn't find online omikron".to_string(),
                )
            }
        }
        ["api", "get", "omikron", id] => {
            let id = id.parse::<i64>().unwrap_or(0);
            if id == 0 {
                not_found()
            } else if let Ok((public_key, ip_address)) = get_omikron_by_id(id).await {
                (
                    StatusCode::OK,
                    format!(
                        "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                        id, public_key, ip_address
                    ),
                )
            } else if let Some(omikron_id) = get_iota_primary_omikron_connection(id) {
                if let Ok((public_key, ip_address)) = get_omikron_by_id(omikron_id).await {
                    (
                        StatusCode::OK,
                        format!(
                            "{{\"id\": {}, \"public_key\": \"{}\", \"ip_address\": \"{}\"}}",
                            omikron_id, public_key, ip_address
                        ),
                    )
                } else {
                    not_found()
                }
            } else if let Ok((_, iota_id, _, _, _, _, _, _, _, _, _, _)) = get_by_user_id(id).await
            {
                if let Some(omikron_id) = get_iota_primary_omikron_connection(iota_id) {
                    if let Ok((public_key, ip_address)) = get_omikron_by_id(omikron_id).await {
                        (
                            StatusCode::OK,
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
        }
        ["api", "get", "id", username] => {
            if username.is_empty() {
                not_found()
            } else if let Ok((
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
                    .add_data(DataTypes::user_id, JsonValue::Number(Number::from(id)))
                    .add_data(DataTypes::iota_id, JsonValue::Number(Number::from(iota_id)))
                    .add_data(
                        DataTypes::sub_level,
                        JsonValue::Number(Number::from(sub_level)),
                    )
                    .add_data(DataTypes::sub_end, JsonValue::Number(Number::from(sub_end)));
                (StatusCode::OK, cv.to_json().to_string())
            } else {
                (
                    StatusCode::OK,
                    CommunicationValue::new(CommunicationType::error_not_found)
                        .to_json()
                        .to_string(),
                )
            }
        }
        ["api", "get", "public_key"] => (StatusCode::OK, public_key_to_base64(&get_public_key())),
        ["api", "get", "user", id] => {
            let id: i64 = id.parse().unwrap_or(0);
            if id == 0 {
                bad_request()
            } else if let Ok((
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
                    .add_data(DataTypes::user_id, JsonValue::Number(Number::from(id)))
                    .add_data(DataTypes::iota_id, JsonValue::Number(Number::from(iota_id)))
                    .add_data(
                        DataTypes::sub_level,
                        JsonValue::Number(Number::from(sub_level)),
                    )
                    .add_data(DataTypes::sub_end, JsonValue::Number(Number::from(sub_end)));
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
                (StatusCode::OK, cv.to_json().to_string())
            } else {
                (
                    StatusCode::OK,
                    CommunicationValue::new(CommunicationType::error_not_found)
                        .to_json()
                        .to_string(),
                )
            }
        }
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            CommunicationValue::new(CommunicationType::error)
                .to_json()
                .to_string(),
        ),
    };
    let body_bytes = body_text.into_bytes();

    HttpResponse::build(status)
        .insert_header((header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"))
        .insert_header((header::ACCESS_CONTROL_ALLOW_HEADERS, "*"))
        .insert_header((header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS"))
        .body(body_bytes)
}

pub fn bad_request() -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, "400 Bad Request".to_string())
}
pub fn not_found() -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, "404 Not Found".to_string())
}
