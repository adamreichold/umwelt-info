use askama::Template;
use axum::{
    extract::{Extension, Query},
    response::Response,
};
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;

use crate::{
    dataset::Dataset,
    index::Searcher,
    server::{Accept, ServerError},
};

pub async fn search(
    Query(params): Query<SearchParams>,
    accept: Accept,
    Extension(searcher): Extension<&'static Searcher>,
    Extension(dir): Extension<&'static Dir>,
) -> Result<Response, ServerError> {
    fn inner(
        params: SearchParams,
        searcher: &Searcher,
        dir: &Dir,
    ) -> Result<SearchPage, ServerError> {
        if params.page == 0 || params.results_per_page == 0 {
            return Err(ServerError::BadRequest(
                "Page and results per page must not be zero",
            ));
        }

        if params.results_per_page > 100 {
            return Err(ServerError::BadRequest(
                "Results per page must not be larger than 100",
            ));
        }

        let (count, docs) = searcher.search(
            &params.query,
            params.results_per_page,
            (params.page - 1) * params.results_per_page,
        )?;

        tracing::debug!("Found {} documents", count);

        let pages = (count + params.results_per_page - 1) / params.results_per_page;

        let mut page = SearchPage {
            params,
            count,
            pages,
            results: Vec::new(),
        };

        let dir = dir.open_dir("datasets")?;

        for doc in docs {
            let (source, id) = doc?;

            let dataset = Dataset::read(dir.open_dir(&source)?.open(&id)?)?;

            page.results.push(SearchResult {
                source,
                id,
                dataset,
            });
        }

        Ok(page)
    }

    let page = spawn_blocking(|| inner(params, searcher, dir)).await??;

    Ok(accept.into_repsonse(page))
}

#[derive(Deserialize, Serialize)]
pub struct SearchParams {
    #[serde(default = "default_query")]
    query: String,
    #[serde(default = "default_page")]
    page: usize,
    #[serde(default = "default_results_per_page")]
    results_per_page: usize,
}

fn default_query() -> String {
    "*".to_owned()
}

fn default_page() -> usize {
    1
}

fn default_results_per_page() -> usize {
    10
}

#[derive(Template, Serialize)]
#[template(path = "search.html")]
struct SearchPage {
    params: SearchParams,
    count: usize,
    pages: usize,
    results: Vec<SearchResult>,
}

#[derive(Serialize)]
struct SearchResult {
    source: String,
    id: String,
    dataset: Dataset,
}
