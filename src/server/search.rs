use askama::Template;
use axum::{
    extract::{Extension, Query},
    response::Response,
};
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use tantivy::schema::Facet;
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
        accept: Accept,
        searcher: &Searcher,
        dir: &Dir,
    ) -> Result<Response, ServerError> {
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

        let results = searcher.search(
            &params.query,
            params.results_per_page,
            (params.page - 1) * params.results_per_page,
        )?;

        tracing::debug!("Found {} documents", results.count);

        let pages = (results.count + params.results_per_page - 1) / params.results_per_page;

        let licenses = results
            .licenses
            .get(results.licenses_root)
            .collect::<Vec<_>>();

        let provenances = results
            .provenances
            .get(results.provenances_root)
            .collect::<Vec<_>>();

        let mut page = SearchPage {
            params,
            count: results.count,
            pages,
            results: Vec::new(),
            licenses,
            provenances,
        };

        let dir = dir.open_dir("datasets")?;

        for doc in results.iter {
            let (source, id) = doc?;

            let dataset = Dataset::read(dir.open_dir(&source)?.open(&id)?)?;

            page.results.push(SearchResult {
                source,
                id,
                dataset,
            });
        }

        Ok(accept.into_repsonse(page))
    }

    spawn_blocking(move || inner(params, accept, searcher, dir)).await?
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
struct SearchPage<'a> {
    params: SearchParams,
    count: usize,
    pages: usize,
    results: Vec<SearchResult>,
    licenses: Vec<(&'a Facet, u64)>,
    provenances: Vec<(&'a Facet, u64)>,
}

impl SearchPage<'_> {
    fn pages(&self) -> Vec<usize> {
        let mut pages = Vec::new();

        pages.extend(1..=self.pages.min(5));

        let mut extend = |new_pages| {
            for new_page in new_pages {
                let last_page = *pages.last().unwrap();

                if last_page < new_page {
                    if last_page + 1 != new_page {
                        pages.push(0);
                    }

                    pages.push(new_page);
                }
            }
        };

        if self.params.page > 2 {
            extend(self.params.page - 2..=self.pages.min(self.params.page + 2))
        }

        if self.pages > 2 {
            extend(self.pages - 2..=self.pages);
        }

        pages
    }
}

#[derive(Serialize)]
struct SearchResult {
    source: String,
    id: String,
    dataset: Dataset,
}
