use serde_json::json;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    SnowflakeError,
    connection::Connection,
    error,
    http::{
        self, client::SnowflakeHttpClient, close::CloseSessionResponse, login::LoginResponse,
        renew::RenewSessionResponse,
    },
    this_errors,
};

static LOGIN_REQUEST_PATH: &'static str = "/session/v1/login-request";
static TOKEN_REQUEST_PATH: &'static str = "/session/token-request";
static CLOSE_SESSION_PATH: &'static str = "/session";

pub(crate) struct Token {
    pub token: String,
    pub validity: Duration,
    pub issued_at: Instant,
}

impl Token {
    pub fn new(token_string: String, validity_in_seconds: i64) -> Self {
        let validity = Duration::from_secs(u64::try_from(validity_in_seconds).unwrap_or(u64::MAX));

        Self {
            token: token_string,
            validity,
            issued_at: Instant::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now().duration_since(self.issued_at) >= self.validity
    }
}

pub struct Session<C: SnowflakeHttpClient + Clone> {
    pub(crate) sequence_counter: u64,
    pub(crate) is_dirty: bool,
    token: Token,
    master_token: Token,
    conn: Connection<C>,
}

impl<C: SnowflakeHttpClient + Clone> Session<C> {
    pub(crate) async fn new(conn: Connection<C>) -> Result<Self, SnowflakeError> {
        let resp = Self::login(conn.clone()).await?;

        let token = resp
            .token
            .ok_or(error!("missing token in login response"))?;

        let master_token = resp
            .master_token
            .ok_or(error!("missing master_token in login response"))?;

        let validity = resp
            .validity
            .ok_or(error!("missing validity in login response"))?;

        Ok(Session {
            is_dirty: false,
            token: Token::new(token, validity),
            master_token: Token::new(master_token, resp.master_validity),
            sequence_counter: 0,
            conn,
        })
    }

    pub(crate) fn get_sequence_count(&mut self) -> u64 {
        let count = self.sequence_counter;
        self.sequence_counter += 1;
        count
    }

    pub(crate) async fn get_token(&mut self) -> Result<&str, SnowflakeError> {
        if self.token.is_expired() {
            if self.master_token.is_expired() {
                let resp = Self::login(self.conn.clone()).await?;

                let token = resp
                    .token
                    .ok_or(error!("missing token in login response"))?;

                let master_token = resp
                    .master_token
                    .ok_or(error!("missing master_token in login response"))?;

                let validity = resp
                    .validity
                    .ok_or(error!("missing validity in login response"))?;

                self.master_token = Token::new(master_token, resp.master_validity);
                self.token = Token::new(token, validity);
            } else {
                self.renew_token().await?;
            }
        }

        Ok(self.token.token.as_str())
    }

    pub(crate) fn get_conn(&self) -> Connection<C> {
        self.conn.clone()
    }

    async fn login(conn: Connection<C>) -> Result<http::login::AuthData, SnowflakeError> {
        let opts_clone = conn.get_opts();
        // let app_version = env!("CARGO_PKG_VERSION");
        let current_os = std::env::consts::OS;
        let current_os_arch = std::env::consts::ARCH;
        // TODO: Maybe actually retrieve release info
        let os_version = format!("{current_os}-{current_os_arch}");

        let mut login_body = json!({
            "data": {
                // This *needs* to be Go. Snowflake API changes response types depending on the library used
                "CLIENT_APP_ID": "Go",
                "CLIENT_APP_VERSION": "1.18.1",
                "SVN_REVISION": "",
                "ACCOUNT_NAME": opts_clone.account_id.as_str(),
                "LOGIN_NAME": opts_clone.username.as_str(),
                "SESSION_PARAMETERS": {
                    "CLIENT_VALIDATE_DEFAULT_PARAMETERS": true,
                },
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": "snowflakedb-rs",
                    "OS": current_os,
                    "OS_VERSION": os_version,
                    "OCSP_MODE": "FAIL_OPEN",
                    "ISA": current_os_arch,
                },
            }
        });

        match &opts_clone.strategy {
            super::AuthStrategy::Password(password) => {
                let obj_map = login_body["data"]
                    .as_object_mut()
                    .expect("unexpected none object in login_body");
                obj_map.insert("PASSWORD".to_string(), password.as_str().into());
            }
            #[cfg(feature = "auth-cert")]
            super::AuthStrategy::Certificate(cert) => {
                use crate::auth::strategies;

                let obj_map = login_body["data"]
                    .as_object_mut()
                    .expect("unexpected none object in login_body");
                obj_map.insert("AUTHENTICATOR".to_string(), "SNOWFLAKE_JWT".into());

                let token = strategies::cert_auth::create_cert_auth_jwt(
                    &opts_clone.account_id,
                    &opts_clone.username,
                    &cert,
                )?;

                obj_map.insert("TOKEN".to_string(), token.into());
            }
        };

        let params = http::params!(
            ("databaseName", opts_clone.database),
            ("warehouse", opts_clone.warehouse),
            ("schemaName", opts_clone.schema),
            ("roleName", opts_clone.role),
        );

        let mut headers = HashMap::new();
        headers.insert("ACCEPT".to_string(), "application/json".to_string());

        let request = this_errors!(
            "failed to build login request",
            http::RequestBuilder::default()
                .connection(conn.clone())
                .params(params)
                .path(LOGIN_REQUEST_PATH)
                .headers(headers)
                .build()
        );

        let resp = request
            .post::<http::login::LoginResponse>(login_body)
            .await?;

        match resp {
            LoginResponse::Success(data) => Ok(data.data),
            LoginResponse::Failure(err) => Err(error!(err.message)),
        }
    }

    async fn renew_token(&mut self) -> Result<(), SnowflakeError> {
        let body = json!({
            "oldSessionToken": self.token.token,
            "requestType": "RENEW"
        });

        let request = this_errors!(
            "failed to renew token",
            http::RequestBuilder::default()
                .connection(self.conn.clone())
                .headers([("ACCEPT".to_string(), "application/snowflake".to_string())])
                .path(TOKEN_REQUEST_PATH)
                .auth_token(&self.master_token.token)
                .build()
        );

        let resp = request.post::<RenewSessionResponse>(body).await?;

        match resp {
            RenewSessionResponse::Success(data) => {
                let session_token = Token::new(data.data.session_token, data.data.session_validity);
                self.token = session_token;

                let master_token = Token::new(data.data.master_token, data.data.master_validity);
                self.master_token = master_token;

                Ok(())
            }
            RenewSessionResponse::Failure(err) => Err(error!(err.message)),
        }
    }

    pub async fn close(self) -> Result<(), SnowflakeError> {
        let token = self.master_token.token;

        let request = this_errors!(
            "failed to build close session request",
            http::RequestBuilder::default()
                .connection(self.conn)
                .path(CLOSE_SESSION_PATH)
                .params(http::params!(("delete", "true")))
                .auth_token(token)
                .build()
        );

        let resp = this_errors!(
            "failed to post to close session route",
            request
                .post::<http::close::CloseSessionResponse>(json!({}))
                .await
        );

        match resp {
            CloseSessionResponse::Success(_) => Ok(()),
            CloseSessionResponse::Failure(err) => Err(error!(err.message)),
        }
    }
}
