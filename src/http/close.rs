use serde::Deserialize;

// #[derive(Debug, Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub(crate) struct CloseSessionData {}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
pub(crate) enum CloseSessionResponse {
    Success(super::GenericResponse<()>),
    Failure(super::ErrorResponse),
}
