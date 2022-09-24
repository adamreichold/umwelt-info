pub mod dataset;
pub mod filters;
pub mod metrics;
pub mod prometheus;
pub mod search;
pub mod stats;

use std::convert::Infallible;

use anyhow::Error;
use askama::Template;
use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts},
    http::{header::ACCEPT, request::Parts, StatusCode},
    response::{Html, IntoResponse, Json, Response},
};
use cap_std::fs::Dir;
use parking_lot::Mutex;
use serde::Serialize;

use crate::{index::Searcher, server::stats::Stats};

pub struct State {
    pub searcher: Searcher,
    pub dir: Dir,
    pub stats: Mutex<Stats>,
}

impl FromRef<&'static State> for &'static Searcher {
    fn from_ref(state: &&'static State) -> Self {
        &state.searcher
    }
}

impl FromRef<&'static State> for &'static Dir {
    fn from_ref(state: &&'static State) -> Self {
        &state.dir
    }
}

impl FromRef<&'static State> for &'static Mutex<Stats> {
    fn from_ref(state: &&'static State) -> Self {
        &state.stats
    }
}

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
impl<S> FromRequestParts<S> for Accept
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(accept) = parts
            .headers
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
