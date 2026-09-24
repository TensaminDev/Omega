use super::super::connection::{
    OmikronConnection, OmikronResult, OptionalDataValueCompat, RequiredMtpFields,
};
use crate::server::short_link::add_short_link;
use mtp::codec::{CommunicationType, CommunicationValue, DataType, DataValue};
use std::sync::Arc;

pub async fn shorten(
    connection: Arc<OmikronConnection>,
    value: CommunicationValue,
) -> OmikronResult<()> {
    let link_data = value.get_data(DataType::Link);
    let link = link_data
        .as_str()
        .ok_or(crate::error::OmegaError::InvalidResponse)?;
    let short = add_short_link(connection.id_i64(), link, &connection.state().config).await?;
    let response = CommunicationValue::new(CommunicationType::ShortenLink)
        .with_id(value.require_id()?)
        .add_typed_default(DataType::Link, DataValue::Str(short));
    connection.send(&response).await
}
