use serde::Deserialize;

// Structs taken from:
// https://github.com/snowflakedb/gosnowflake/blob/1922ccebdd8ea87aea0e03f53d9ad9246e7c1654/auth.go#L216
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct AuthResponseSessionInfo {
    pub database_name: String,
    pub schema_name: String,
    pub warehouse_name: String,
    pub role_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct AuthData {
    #[serde(rename = "validityInSeconds")]
    pub validity: Option<i64>,
    pub token: Option<String>,

    #[serde(rename = "masterValidityInSeconds")]
    pub master_validity: i64,
    pub master_token: Option<String>,

    #[serde(rename = "idTokenValidityInSeconds")]
    pub id_token_validity: i64,
    pub id_token: Option<String>,

    pub display_user_name: String,
    pub server_version: String,
    pub first_login: bool,

    #[serde(rename = "mfaTokenValidityInSeconds")]
    pub mfa_validity: i64,
    pub mfa_token: Option<String>,

    #[serde(rename = "remMeValidityInSeconds")]
    pub rem_me_validity: i64,
    pub rem_me_token: Option<String>,

    pub health_check_interval: i64,
    pub new_client_for_upgrade: Option<String>,

    #[serde(rename = "sessionId")]
    pub session_id: i64,

    pub parameters: Vec<super::NameValueParameter>,
    pub session_info: AuthResponseSessionInfo,

    #[serde(rename = "tokenUrl")]
    pub token_url: Option<String>,

    #[serde(rename = "ssoUrl")]
    pub sso_url: Option<String>,

    pub proof_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum LoginResponse {
    Success(super::GenericResponse<AuthData>),
    Failure(super::ErrorResponse),
}
