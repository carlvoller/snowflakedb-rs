use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{error, http::client::SnowflakeHttpClient, this_errors};
use derive_builder::Builder;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{SnowflakeError, connection::Connection};

pub mod client;
pub mod close;
pub mod login;
mod macros;
pub mod renew;
mod url;

pub(crate) use macros::params;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(crate) struct NameValueParameter {
    pub name: String,
    pub value: Value,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(crate) struct GenericResponse<T> {
    pub(crate) data: T,
    pub(crate) code: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) success: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ErrorData {
    authn_method: Option<String>,
    error_code: Option<String>,
    next_action: Option<String>,
}

pub(crate) type ErrorResponse = GenericResponse<ErrorData>;

#[derive(Builder)]
pub(crate) struct Request<C: SnowflakeHttpClient + Clone> {
    connection: Connection<C>,

    #[builder(setter(into, strip_option), default = None)]
    params: Option<Vec<(String, String)>>,

    #[builder(setter(into), default = HashMap::new())]
    headers: HashMap<String, String>,
    #[builder(default = None, setter(into, strip_option))]
    path: Option<String>,

    #[builder(default = None, setter(into, strip_option))]
    full_url: Option<String>,

    #[builder(setter(into, strip_option), default = None)]
    auth_token: Option<String>,
}

impl<C: SnowflakeHttpClient + Clone> Request<C> {
    fn build_url(&self) -> Result<String, SnowflakeError> {
        if let Some(url) = self.full_url.as_deref() {
            Ok(url.to_string())
        } else {
            let conn_opts = self.connection.get_opts();
            let pathname = if let Some(path) = self.path.as_deref() {
                Ok(if let Some(host) = conn_opts.host.as_deref() {
                    format!("{}{}", host, path)
                } else {
                    format!(
                        "https://{}.snowflakecomputing.com{}",
                        conn_opts.account_id, path
                    )
                })
            } else {
                Err(error!("either path or full_url needs to be set on Request"))
            }?;

            let mut params = params!(
                ("requestId", uuid::Uuid::new_v4().to_string()),
                ("requestGuid", uuid::Uuid::new_v4().to_string()),
                (
                    "clientStartTime",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("time somehow went backwards")
                        .as_secs()
                        .to_string()
                )
            );

            if let Some(p) = self.params.as_deref() {
                params.extend_from_slice(p);
            }

            let url = this_errors!(
                "failed to construct url",
                url::construct_url(pathname.as_str(), &params)
            );

            Ok(url)
        }
    }

    pub async fn post<T: DeserializeOwned>(
        self,
        body: serde_json::Value,
    ) -> Result<T, SnowflakeError> {
        let url = self.build_url()?;
        let mut headers = self.headers;

        if let Some(token) = self.auth_token {
            headers.insert(
                "Authorization".into(),
                format!("Snowflake Token=\"{}\"", &token),
            );
        }

        headers.insert("content-type".into(), "application/json".into());

        let body_bytes = this_errors!(
            "failed to serialise body as json",
            serde_json::to_vec(&body)
        );

        let resp = this_errors!(
            "failed to make request",
            self.connection
                .get_client()
                .post(url.as_str(), body_bytes, headers)
                .await
        );

        let resp_as_json = this_errors!(
            "failed to parse response as json",
            serde_json::from_slice::<T>(&resp)
        );

        Ok(resp_as_json)
    }

    pub async fn get<T: DeserializeOwned>(self) -> Result<T, SnowflakeError> {
        let url = self.build_url()?;
        let mut headers = self.headers;

        if let Some(token) = self.auth_token {
            headers.insert(
                "Authorization".into(),
                format!("Snowflake Token=\"{}\"", &token),
            );
        }

        let resp = this_errors!(
            "failed to make request",
            self.connection
                .get_client()
                .get(url.as_str(), headers)
                .await
        );

        let resp_as_json = this_errors!(
            "failed to parse response as json",
            serde_json::from_slice::<T>(&resp)
        );

        Ok(resp_as_json)
    }

    pub async fn get_as_bytes(self) -> Result<Vec<u8>, SnowflakeError> {
        let url = self.build_url()?;
        let mut headers = self.headers;

        if let Some(token) = self.auth_token {
            headers.insert(
                "Authorization".into(),
                format!("Snowflake Token=\"{}\"", &token),
            );
        }

        let resp = this_errors!(
            "failed to make request",
            self.connection
                .get_client()
                .get(url.as_str(), headers)
                .await
        );

        Ok(resp)
    }
}
