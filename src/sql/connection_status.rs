#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionType {
    Online,
    UserOffline,
    IotaOffline,
    Away,
    DoNotDisturb,
}
impl ConnectionType {
    pub fn to_str(&self) -> &str {
        match self {
            ConnectionType::Online => "online",
            ConnectionType::UserOffline => "user_offline",
            ConnectionType::IotaOffline => "iota_offline",
            ConnectionType::Away => "away",
            ConnectionType::DoNotDisturb => "do_not_disturb",
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            ConnectionType::Online => "online".to_string(),
            ConnectionType::UserOffline => "user_offline".to_string(),
            ConnectionType::IotaOffline => "iota_offline".to_string(),
            ConnectionType::Away => "away".to_string(),
            ConnectionType::DoNotDisturb => "do_not_disturb".to_string(),
        }
    }
    pub fn from_str(s: &str) -> Option<ConnectionType> {
        match s.to_lowercase().as_str() {
            "online" => Some(ConnectionType::Online),
            "user_offline" => Some(ConnectionType::UserOffline),
            "iota_offline" => Some(ConnectionType::IotaOffline),
            "away" => Some(ConnectionType::Away),
            "do_not_disturb" => Some(ConnectionType::DoNotDisturb),
            _ => None,
        }
    }
}
