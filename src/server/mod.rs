pub mod dataset;
pub mod filters;
pub mod metrics;
pub mod search;
pub mod stats;

use std::convert::Infallible;

use anyhow::Error;
use askama::Template;
use axum::{
    async_trait,
    extract::{FromRequest, RequestParts},
    http::{header::ACCEPT, StatusCode},
    response::{Html, IntoResponse, Json, Response},
};
use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub enum Accept {
    Unspecified,
    Html,
    Json,
}

impl Accept {
    pub fn into_repsonse<P>(self, page: P) -> Response
    where
        P: Template + Serialize,
    {
        match self {
            Accept::Unspecified | Accept::Html => Html(page.render().unwrap()).into_response(),
            Accept::Json => Json(page).into_response(),
        }
    }
}

#[async_trait]
impl<B> FromRequest<B> for Accept
where
    B: Send,
{
    type Rejection = Infallible;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        if let Some(accept) = req
            .headers()
            .get(ACCEPT)
            .and_then(|header| header.to_str().ok())
        {
            if accept.contains("text/html") {
                return Ok(Self::Html);
            } else if accept.contains("application/json") {
                return Ok(Self::Json);
            }
        }

        Ok(Self::Unspecified)
    }
}

pub enum ServerError {
    BadRequest(&'static str),
    Internal(Error),
}

impl<E> From<E> for ServerError
where
    Error: From<E>,
{
    fn from(err: E) -> Self {
        Self::Internal(Error::from(err))
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        match self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            Self::Internal(err) => {
                (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
            }
        }
    }
}
