use serde::Serialize;

#[derive(Serialize)]
pub struct StatusResponse {
    pub status: &'static str,
}

#[derive(Serialize)]
pub struct OmikronResponse {
    pub status: &'static str,
    pub id: i64,
    pub public_key: String,
    pub ip_address: String,
    pub port: u16,
}

#[derive(Serialize)]
pub struct IotaResponse {
    pub status: &'static str,
    pub iota_id: i64,
    pub public_key: String,
}

#[derive(Serialize)]
pub struct UserResponse {
    pub status: &'static str,
    pub username: String,
    pub public_key: String,
    pub user_id: i64,
    pub iota_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    #[serde(rename = "status_message", skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub about: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
}

#[derive(Serialize)]
pub struct UsernameResponse {
    pub status: &'static str,
    pub username: String,
    pub public_key: String,
    pub user_id: i64,
    pub iota_id: Option<i64>,
    pub principal: String,
}

#[derive(Serialize)]
pub struct PublicKeyResponse {
    pub status: &'static str,
    pub public_key: String,
}

pub fn json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{\"status\":\"error\"}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_user_response_excludes_account_subscription() {
        let response = UserResponse {
            status: "success",
            username: "alice".into(),
            public_key: "public".into(),
            user_id: 42,
            iota_id: Some(7),
            display: None,
            status_message: None,
            about: None,
            avatar: None,
        };
        let value = serde_json::to_value(response).unwrap();
        assert!(value.get("sub_level").is_none());
        assert!(value.get("sub_end").is_none());
    }
}
