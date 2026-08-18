pub(crate) use super::omikron_connection::{OmikronConnection, OmikronResult};

use mtp::codec::{CommunicationValue, DataValue};

pub(crate) trait MtpValueCompat {
    fn get_id(&self) -> u32;
    fn get_sender(&self) -> u64;
    fn get_receiver(&self) -> u64;
}

impl MtpValueCompat for CommunicationValue {
    fn get_id(&self) -> u32 {
        self.id().unwrap_or_default()
    }

    fn get_sender(&self) -> u64 {
        self.sender().unwrap_or_default()
    }

    fn get_receiver(&self) -> u64 {
        self.receiver().unwrap_or_default()
    }
}

pub(crate) trait OptionalDataValueCompat {
    fn as_number(&self) -> Option<i128>;
    fn as_signed_number(&self) -> Option<i128>;
    fn as_str(&self) -> Option<&str>;
    fn as_bytes(&self) -> Option<Vec<u8>>;
}

impl OptionalDataValueCompat for Option<&DataValue> {
    fn as_number(&self) -> Option<i128> {
        self.and_then(|value| value.as_number())
    }

    fn as_signed_number(&self) -> Option<i128> {
        self.and_then(|value| value.as_signed_number())
    }

    fn as_str(&self) -> Option<&str> {
        self.and_then(|value| value.as_str())
    }

    fn as_bytes(&self) -> Option<Vec<u8>> {
        self.and_then(|value| value.as_bytes())
    }
}
