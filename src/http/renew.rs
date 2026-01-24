use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct RenewSessionData {
    pub(crate) session_id: String,

    #[serde(rename = "validityInSecondsST")]
    pub(crate) session_validity: i64,
    pub(crate) session_token: String,

    #[serde(rename = "validityInSecondsMT")]
    pub(crate) master_validity: i64,
    pub(crate) master_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum RenewSessionResponse {
    Success(super::GenericResponse<RenewSessionData>),
    Failure(super::ErrorResponse),
}
