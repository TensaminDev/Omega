#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(unused, non_camel_case_types)]
pub enum UserStatus {
    user_offline,
    user_online,
    user_dnd,
    user_idle,
    user_wc,
    user_borked,
    user_invisible,
    iota_offline,
    iota_online,
    iota_borked,
}
impl UserStatus {
    pub fn from_client_preference(s: &str) -> Option<Self> {
        match s {
            "user_online" => Some(Self::user_online),
            "user_idle" => Some(Self::user_idle),
            "user_dnd" => Some(Self::user_dnd),
            "user_wc" => Some(Self::user_wc),
            "user_invisible" => Some(Self::user_invisible),
            _ => None,
        }
    }

    pub fn public_value(&self) -> Self {
        match self {
            Self::user_invisible => Self::user_offline,
            value => value.clone(),
        }
    }

    /// Parse a value received from a client or persisted as an account
    /// preference. Derived connectivity and diagnostic states are never valid
    /// preferences.
    #[allow(dead_code)]
    pub fn from_str(s: &str) -> Option<Self> {
        Self::from_client_preference(s)
    }
}

impl std::fmt::Display for UserStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::UserStatus;

    #[test]
    fn accepts_only_client_preferences() {
        for value in [
            "user_online",
            "user_idle",
            "user_dnd",
            "user_wc",
            "user_invisible",
        ] {
            assert!(
                UserStatus::from_client_preference(value).is_some(),
                "{value}"
            );
        }

        for value in [
            "user_offline",
            "iota_offline",
            "iota_online",
            "user_borked",
            "iota_borked",
            "unknown",
        ] {
            assert_eq!(UserStatus::from_client_preference(value), None, "{value}");
            assert_eq!(UserStatus::from_str(value), None, "{value}");
        }
    }

    #[test]
    fn invisible_is_publicly_offline() {
        assert_eq!(
            UserStatus::user_invisible.public_value(),
            UserStatus::user_offline
        );
        assert_eq!(UserStatus::user_dnd.public_value(), UserStatus::user_dnd);
    }
}
