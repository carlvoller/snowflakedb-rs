use std::collections::HashMap;

use crate::SnowflakeError;
use core::future::Future;

#[cfg(feature = "reqwest")]
use reqwest;

pub trait SnowflakeHttpClient: Clone + Send + 'static {
    fn new() -> Self;

    fn get(
        &self,
        url: &str,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<Vec<u8>, SnowflakeError>> + Send;

    fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<Vec<u8>, SnowflakeError>> + Send;
}

#[cfg(feature = "reqwest")]
impl SnowflakeHttpClient for reqwest::Client {
    fn new() -> Self {
        reqwest::Client::builder()
            .gzip(true)
            .referer(false)
            .build()
            .unwrap()
    }

    fn get(
        &self,
        url: &str,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<Vec<u8>, SnowflakeError>> {
        use crate::{error, this_errors};

        async move {
            use reqwest::header::HeaderMap;

            let url = this_errors!("failed to parse url", reqwest::Url::parse(url));

            let headers = this_errors!(
                "failed to convert headers",
                headers
                    .iter()
                    .map(|(k, v)| {
                        Ok((
                            reqwest::header::HeaderName::from_bytes(k.as_bytes())
                                .map_err(|e| error!(e))?,
                            reqwest::header::HeaderValue::from_str(v.as_str())
                                .map_err(|e| error!(e))?,
                        ))
                    })
                    .collect::<Result<
                        Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
                        SnowflakeError,
                    >>()
            );

            let resp = this_errors!(
                "failed to send get request",
                self.get(url)
                    .headers(HeaderMap::from_iter(headers))
                    .send()
                    .await
            );

            let bytes = this_errors!("failed to get response bytes", resp.bytes().await);

            Ok(bytes.to_vec())
        }
    }

    fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<Vec<u8>, SnowflakeError>> {
        use crate::{error, this_errors};

        async move {
            use reqwest::header::HeaderMap;

            println!("{:?}", url);

            let url = this_errors!("failed to parse url", reqwest::Url::parse(url));

            let headers = this_errors!(
                "failed to convert headers",
                headers
                    .iter()
                    .map(|(k, v)| {
                        Ok((
                            reqwest::header::HeaderName::from_bytes(k.as_bytes())
                                .map_err(|e| error!(e))?,
                            reqwest::header::HeaderValue::from_str(v.as_str())
                                .map_err(|e| error!(e))?,
                        ))
                    })
                    .collect::<Result<
                        Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
                        SnowflakeError,
                    >>()
            );

            let headers = HeaderMap::from_iter(headers);     

            println!("{:?}", String::from_utf8(body.clone()).unwrap()); 

            let resp = this_errors!(
                "failed to send post request",
                self.post(url)
                    .body(body)
                    .headers(headers)
                    .send()
                    .await
            );

            let bytes = this_errors!("failed to get response bytes", resp.bytes().await);

            println!("{:?}", String::from_utf8(bytes.to_vec()).unwrap());

            Ok(bytes.to_vec())
        }
    }
}
