use serde::Serialize;

#[cfg(feature = "auth-cert")]
pub(crate) mod cert_auth;

// Snowflake Auth Strategies can be found here:
// https://github.com/snowflakedb/gosnowflake/blob/master/auth.go#L139
#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuthStrategy {
    Password(String),
    #[cfg(feature = "auth-cert")]
    Certificate(String),
    // TODO: Add more...
}
