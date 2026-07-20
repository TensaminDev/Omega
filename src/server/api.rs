use crate::load_keyring;
use crate::sql::sql;
use crate::sql::sql::{get_by_user_id, get_iota_by_id, get_omikron_by_id};
use crate::sql::user_online_tracker::{get_all_connections, get_iota_primary_omikron_connection};
use crate::transport::omikron_manager::get_random_omikron;
use crate::util::file_util::get_directory;
use base64::Engine as _;
use bytes::Bytes;
use http::{Method, StatusCode};
use json::JsonValue;
use mtp::webserver::{Http3Request, Http3Response, RouteParams};

pub async fn handle(request: Http3Request, response: Http3Response) -> Http3Response {
    let method = request.method;
    let path = request.uri.path().to_string();
    let body_string = request
        .body
        .map(|body| String::from_utf8_lossy(&body).to_string());

    if method == Method::OPTIONS {
        return response
            .status(StatusCode::OK)
            .header("access-control-allow-origin", "*")
            .header("access-control-allow-methods", "GET, POST, OPTIONS")
            .header("access-control-allow-headers", "*");
    }

    let path_parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    let _body: Option<JsonValue> = if let Some(ref bs) = body_string {
        if let Ok(body_json) = json::parse(bs) {
            Some(body_json)
        } else {
            None
        }
    } else {
        None
    };

    let (status, body_text) = match path_parts.as_slice() {
        // ==================================================
        // DOWNLOAD IOTA FRONTEND
        // ==================================================
        ["api", "download", "iota_frontend"] => {
            let file_path = format!("{}/downloads/iota_frontend.zip", get_directory());

            match std::fs::read(file_path) {
                Ok(file_bytes) => {
                    return response
                        .status(StatusCode::OK)
                        .header("access-control-allow-origin", "*")
                        .header("content-type", "application/zip")
                        .header(
                            "content-disposition",
                            "attachment; filename=\"iota_frontend.zip\"",
                        )
                        .body(Bytes::from(file_bytes));
                }
                Err(_) => {
                    let mut res = JsonValue::new_object();
                    res["status"] = "error_not_found".into();
                    return response
                        .status(StatusCode::NOT_FOUND)
                        .header("access-control-allow-origin", "*")
                        .body(res.dump());
                }
            }
        }

        // ==================================================
        // GET RANDOM OMIKRON
        // ==================================================
        ["api", "get", "omikron"] => {
            if let Ok(omikron_conn) = get_random_omikron().await {
                if let Some(id) = omikron_conn.get_omikron_id().await {
                    if let Ok((public_key, ip_address, port)) = sql::get_omikron_by_id(id).await {
                        let mut res = JsonValue::new_object();
                        res["status"] = "success".into();
                        res["id"] = id.into();
                        res["public_key"] = public_key.to_base64().into();
                        res["ip_address"] = ip_address.into();
                        res["port"] = port.into();

                        (StatusCode::OK, res.dump())
                    } else {
                        let mut res = JsonValue::new_object();
                        res["status"] = "error".into();
                        (StatusCode::INTERNAL_SERVER_ERROR, res.dump())
                    }
                } else {
                    let mut res = JsonValue::new_object();
                    res["status"] = "error".into();
                    (StatusCode::INTERNAL_SERVER_ERROR, res.dump())
                }
            } else {
                let mut res = JsonValue::new_object();
                res["status"] = "error_not_found".into();
                (StatusCode::NOT_FOUND, res.dump())
            }
        }

        // ==================================================
        // GET OMIKRON BY ID
        // ==================================================
        ["api", "get", "omikron", id] => {
            let id = id.parse::<i64>().unwrap_or(0);

            if id == 0 {
                let mut res = JsonValue::new_object();
                res["status"] = "error_bad_request".into();
                (StatusCode::BAD_REQUEST, res.dump())
            } else if let Ok((public_key, ip_address, port)) = get_omikron_by_id(id).await {
                let mut res = JsonValue::new_object();
                res["status"] = "success".into();
                res["id"] = id.into();
                res["public_key"] = public_key.to_base64().into();
                res["ip_address"] = ip_address.into();
                res["port"] = port.into();
                (StatusCode::OK, res.dump())
            } else if let Some(omikron_id) = get_iota_primary_omikron_connection(id) {
                if let Ok((public_key, ip_address, port)) = get_omikron_by_id(omikron_id).await {
                    let mut res = JsonValue::new_object();
                    res["status"] = "success".into();
                    res["id"] = omikron_id.into();
                    res["public_key"] = public_key.to_base64().into();
                    res["ip_address"] = ip_address.into();
                    res["port"] = port.into();
                    (StatusCode::OK, res.dump())
                } else {
                    let mut res = JsonValue::new_object();
                    res["status"] = "error_not_found".into();
                    (StatusCode::NOT_FOUND, res.dump())
                }
            } else if let Ok((_, iota_id, _, _, _, _, _, _, _, _, _, _)) = get_by_user_id(id).await
            {
                if let Some(omikron_id) = get_iota_primary_omikron_connection(iota_id) {
                    if let Ok((public_key, ip_address, port)) = get_omikron_by_id(omikron_id).await
                    {
                        let mut res = JsonValue::new_object();
                        res["status"] = "success".into();
                        res["id"] = omikron_id.into();
                        res["public_key"] = public_key.to_base64().into();
                        res["ip_address"] = ip_address.into();
                        res["port"] = port.into();
                        (StatusCode::OK, res.dump())
                    } else {
                        let mut res = JsonValue::new_object();
                        res["status"] = "error_not_found".into();
                        (StatusCode::NOT_FOUND, res.dump())
                    }
                } else {
                    let mut res = JsonValue::new_object();
                    res["status"] = "error_not_found".into();
                    (StatusCode::NOT_FOUND, res.dump())
                }
            } else {
                let mut res = JsonValue::new_object();
                res["status"] = "error_not_found".into();
                (StatusCode::NOT_FOUND, res.dump())
            }
        }

        ["api", "get", "connections"] => {
            if let Ok(connections) = get_all_connections().await {
                let mut res = JsonValue::new_object();
                res["status"] = "success".into();

                for (omikron_id, iota_map) in connections {
                    let mut omikron_obj = JsonValue::new_object();
                    for (iota_id, user_ids) in iota_map {
                        let mut user_arr = JsonValue::new_array();
                        for user_id in user_ids {
                            let _ = user_arr.push(user_id);
                        }
                        omikron_obj[&iota_id.to_string()] = user_arr;
                    }
                    res[&omikron_id.to_string()] = omikron_obj;
                }

                (StatusCode::OK, res.dump())
            } else {
                let mut res = JsonValue::new_object();
                res["status"] = "error".into();
                (StatusCode::INTERNAL_SERVER_ERROR, res.dump())
            }
        }

        // ==================================================
        // GET IOTA BY ID
        // ==================================================
        ["api", "get", "iota", id] => {
            let id: i64 = id.parse().unwrap_or(0);

            if id == 0 {
                let mut res = JsonValue::new_object();
                res["status"] = "error_bad_request".into();
                (StatusCode::BAD_REQUEST, res.dump())
            } else if let Ok((id, public_key)) = get_iota_by_id(id).await {
                let mut res = JsonValue::new_object();
                res["status"] = "success".into();
                res["iota_id"] = id.into();
                res["public_key"] = public_key.to_base64().into();
                (StatusCode::OK, res.dump())
            } else {
                let mut res = JsonValue::new_object();
                res["status"] = "error_not_found".into();
                (StatusCode::NOT_FOUND, res.dump())
            }
        }

        // ==================================================
        // GET ID BY USERNAME
        // ==================================================
        ["api", "get", "id", username] => {
            if username.is_empty() {
                let mut res = JsonValue::new_object();
                res["status"] = "error_bad_request".into();
                (StatusCode::BAD_REQUEST, res.dump())
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
                let mut res = JsonValue::new_object();
                res["status"] = "success".into();
                res["username"] = username.into();
                res["public_key"] = public_key.to_base64().into();
                res["user_id"] = id.into();
                res["iota_id"] = iota_id.into();
                res["sub_level"] = sub_level.into();
                res["sub_end"] = sub_end.into();

                (StatusCode::OK, res.dump())
            } else {
                let mut res = JsonValue::new_object();
                res["status"] = "error_not_found".into();
                (StatusCode::OK, res.dump())
            }
        }

        // ==================================================
        // GET SERVER PUBLIC KEY
        // ==================================================
        ["api", "get", "public_key"] => {
            let mut res = JsonValue::new_object();
            res["status"] = "success".into();
            let bundle = load_keyring().public_key_bundle();
            res["public_key"] = base64::engine::general_purpose::STANDARD
                .encode(bundle.as_bytes())
                .into();
            (StatusCode::OK, res.dump())
        }

        // ==================================================
        // GET USER BY ID
        // ==================================================
        ["api", "get", "user", id] => {
            let id: i64 = id.parse().unwrap_or(0);

            if id == 0 {
                let mut res = JsonValue::new_object();
                res["status"] = "error_bad_request".into();
                (StatusCode::BAD_REQUEST, res.dump())
            } else if let Ok((
                id,
                iota_id,
                username,
                display,
                status_msg,
                about,
                avatar,
                sub_level,
                sub_end,
                public_key,
                _,
                _,
            )) = sql::get_by_user_id(id).await
            {
                let mut res = JsonValue::new_object();
                res["status"] = "success".into();
                res["username"] = username.into();
                res["public_key"] = public_key.to_base64().into();
                res["user_id"] = id.into();
                res["iota_id"] = iota_id.into();
                res["sub_level"] = sub_level.into();
                res["sub_end"] = sub_end.into();

                if let Some(display) = display {
                    res["display"] = display.into();
                }
                if let Some(status_msg) = status_msg {
                    res["status_message"] = status_msg.into();
                }
                if let Some(about) = about {
                    res["about"] = about.into();
                }
                if let Some(avatar) = avatar {
                    res["avatar"] = base64::engine::general_purpose::STANDARD
                        .encode(avatar)
                        .into();
                }

                (StatusCode::OK, res.dump())
            } else {
                let mut res = JsonValue::new_object();
                res["status"] = "error_not_found".into();
                (StatusCode::OK, res.dump())
            }
        }

        // ==================================================
        // DIRECT - SHORT LINK RESOLUTION
        // ==================================================
        ["direct", short @ ..] => {
            let short_str = short.join("/");
            let short = short_str.replace("/", "");
            if let Ok(long) = crate::server::short_link::get_short_link(&short).await {
                return response
                    .status(StatusCode::TEMPORARY_REDIRECT)
                    .header("location", &long);
            } else {
                return response
                    .status(StatusCode::TEMPORARY_REDIRECT)
                    .header("location", "https://tensamin.net");
            }
        }

        // ==================================================
        // DEFAULT
        // ==================================================
        _ => {
            let mut res = JsonValue::new_object();
            res["status"] = "error".into();
            (StatusCode::INTERNAL_SERVER_ERROR, res.dump())
        }
    };

    response
        .status(status)
        .header("access-control-allow-origin", "*")
        .header("access-control-allow-headers", "*")
        .header("access-control-allow-methods", "GET, POST, OPTIONS")
        .body(body_text)
}

pub async fn handle_pattern(
    request: Http3Request,
    response: Http3Response,
    _params: RouteParams,
) -> Http3Response {
    handle(request, response).await
}
