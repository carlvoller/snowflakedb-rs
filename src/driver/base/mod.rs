use std::collections::HashMap;

use derive_builder::Builder;
use serde_json::json;

use crate::{
    SnowflakeError,
    auth::session::Session,
    error,
    http::{self, client::SnowflakeHttpClient},
    this_errors,
};

pub(crate) mod bindings;
pub(crate) mod response;

static QUERY_REQUEST_PATH: &'static str = "/queries/v1/query-request";

#[derive(Builder)]
pub(crate) struct BinaryQuery {
    pub sql_text: String,

    #[builder(setter(into), default = "\"application/json\".into()")]
    pub accept_header: String,

    #[builder(setter(into, strip_option), default = true)]
    pub is_describe_only: bool,

    #[builder(setter(strip_option), default = None)]
    bindings: Option<bindings::Bindings>,
}

impl BinaryQuery {
    pub async fn run<C: SnowflakeHttpClient>(
        mut self,
        session: &mut Session<C>,
    ) -> Result<response::RawQueryResponse, SnowflakeError> {
        let mut headers = HashMap::new();
        headers.insert("ACCEPT".to_string(), self.accept_header.clone());

        let bindings = self
            .bindings
            .take()
            .map(|x| x.get_final_bindings())
            .unwrap_or(HashMap::new());

        // let bindings = self.convert_bindings();

        let body = json!({
            "sqlText": self.sql_text,
            "sequenceId": session.get_sequence_count(),
            "describeOnly": self.is_describe_only,
            "bindings": bindings,
            "asyncExec": false,
            "isInternal": false,
        });

        let conn = session.get_conn();
        let token = session.get_token().await?;
        let request = this_errors!(
            "failed to send query request",
            http::RequestBuilder::default()
                .path(QUERY_REQUEST_PATH)
                .connection(conn)
                .headers(headers.clone())
                .auth_token(token)
                .build()
        );

        let response = request.post::<response::ExecResponse>(body).await?;
        let query_result = self.process_response(session, response).await?;

        Ok(query_result)
    }

    async fn process_response<C: SnowflakeHttpClient>(
        &self,
        session: &mut Session<C>,
        resp: response::ExecResponse,
    ) -> Result<response::RawQueryResponse, SnowflakeError> {
        match resp {
            response::ExecResponse::Success(data) => {
                match data.data {
                    response::ExecResponseData::Query(query_data) => Ok(query_data),
                    response::ExecResponseData::AsyncQuery(async_data) => {
                        // let async_data = async_data;

                        let token = session.get_token().await?;
                        let request = this_errors!(
                            "failed to build chunk request",
                            http::RequestBuilder::default()
                                .auth_token(token)
                                .path(async_data.get_result_url)
                                .connection(session.get_conn())
                                .build()
                        );

                        let resp = request.get::<response::ExecResponse>().await?;
                        Box::pin(self.process_response(session, resp)).await
                    }
                    response::ExecResponseData::PutGet(_) => Err(error!("Put/Get not implemented")),
                }
            }
            response::ExecResponse::Failure(err) => Err(error!(err.message)),
        }
    }
}

// fn transpose<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
//     assert!(!v.is_empty());
//     let len = v[0].len();
//     let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
//     (0..len)
//         .map(|_| {
//             iters
//                 .iter_mut()
//                 .map(|n| n.next().unwrap())
//                 .collect::<Vec<T>>()
//         })
//         .collect()
// }
