use super::super::omikron_connection::{OmikronConnection, OmikronResult};
use crate::sql::{connection_status::UserStatus, user_online_tracker};
use mtp::{
    codec::{CommunicationType, CommunicationValue, DataType, DataValue},
    type_map::TypeMap,
};
use std::sync::Arc;

pub async fn get(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let DataValue::Array(ids) = value.get_data(DataType::UserIds) else {
        return Ok(());
    };
    let tm = TypeMap::latest();
    let states = ids
        .iter()
        .filter_map(|id| {
            let DataValue::SignedNumber(id) = id else {
                return None;
            };
            let status = user_online_tracker::get_user_status(*id as i64)
                .map(|status| {
                    if status.connection_type == UserStatus::user_invisible {
                        UserStatus::user_offline.to_string()
                    } else {
                        status.connection_type.to_string()
                    }
                })
                .unwrap_or_else(|| UserStatus::iota_offline.to_string());
            let mut map = Vec::new();
            if let Some(kind) = DataType::UserId.try_to_id(&tm) {
                map.push((kind, DataValue::SignedNumber((*id as i64).into())));
            }
            if let Some(kind) = DataType::UserState.try_to_id(&tm) {
                map.push((kind, DataValue::Str(status)));
            }
            Some(DataValue::Container(map))
        })
        .collect();
    let response = CommunicationValue::new(CommunicationType::GetStates)
        .with_id(value.get_id())
        .add_typed_default(DataType::UserStates, DataValue::Array(states));
    connection.send(&response).await
}
