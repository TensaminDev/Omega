use super::super::omikron_connection::{OmikronConnection, OmikronResult};
use crate::{
    db::{iota_repo, user_repo},
    models::{IotaId, UserId},
};
use mtp::codec::{CommunicationType, CommunicationValue, DataType, DataValue};
use std::sync::Arc;

async fn delete(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
    result: impl std::future::Future<Output = crate::error::Result<()>>,
) -> OmikronResult<()> {
    let response = match result.await {
        Ok(()) => CommunicationValue::new(CommunicationType::Success),
        Err(error) => CommunicationValue::new(CommunicationType::ErrorInternal)
            .add_typed_default(DataType::ErrorType, DataValue::Str(error.to_string())),
    };
    connection.send(&response.with_id(value.get_id())).await
}

pub async fn user(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    delete(
        connection,
        value.clone(),
        user_repo::delete_user(UserId::from(value.get_sender() as i64)),
    )
    .await
}
pub async fn iota(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    delete(
        connection,
        value.clone(),
        iota_repo::delete_iota(IotaId::from(value.get_sender() as i64)),
    )
    .await
}
