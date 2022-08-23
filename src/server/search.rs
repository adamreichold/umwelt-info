use std::borrow::Cow;

use askama::Template;
use axum::{
    extract::{Extension, Query},
    response::Response,
};
use cap_std::fs::Dir;
use hashbrown::HashSet;
use reqwest::Client;
use serde::{
    de::{Deserializer, Error},
    Deserialize, Serialize,
};
use tantivy::schema::Facet;
use tokio::task::spawn_blocking;

use crate::{
    dataset::Dataset,
    index::Searcher,
    server::{Accept, ServerError},
    umthes,
};

pub async fn search(
    Query(params): Query<SearchParams>,
    accept: Accept,
    Extension(searcher): Extension<&'static Searcher>,
    Extension(client): Extension<&'static Client>,
    Extension(similar_terms_cache): Extension<&'static umthes::SimilarTermsCache>,
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

        let results = searcher.search(
            &params.query,
            &params.provenances_root,
            &params.licenses_root,
            params.results_per_page,
            (params.page - 1) * params.results_per_page,
        )?;

        tracing::debug!("Found {} documents", results.count);

        let pages = (results.count + params.results_per_page - 1) / params.results_per_page;

        let provenances = results
            .provenances
            .get(params.provenances_root.clone())
            .map(|(facet, count)| (facet.to_string(), count))
            .collect::<Vec<_>>();

        let licenses = results
            .licenses
            .get(params.licenses_root.clone())
            .map(|(facet, count)| (facet.to_string(), count))
            .collect::<Vec<_>>();

        let mut page = SearchPage {
            params,
            count: results.count,
            pages,
            results: Vec::new(),
            provenances,
            licenses,
            terms: results.terms,
            related_terms: HashSet::new(),
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

        Ok(page)
    }

    let mut page = spawn_blocking(|| inner(params, searcher, dir)).await??;

    page.related_terms =
        umthes::fetch_similar_terms(client, similar_terms_cache, page.terms.iter()).await;

    Ok(accept.into_repsonse(page))
}

#[derive(Deserialize, Serialize)]
pub struct SearchParams {
    #[serde(default = "default_query")]
    query: String,
    #[serde(deserialize_with = "deserialize_facet", default = "default_root")]
    provenances_root: Facet,
    #[serde(deserialize_with = "deserialize_facet", default = "default_root")]
    licenses_root: Facet,
    #[serde(default = "default_page")]
    page: usize,
    #[serde(default = "default_results_per_page")]
    results_per_page: usize,
}

fn deserialize_facet<'de, D>(deserializer: D) -> Result<Facet, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Cow::<str>::deserialize(deserializer)?;

    Facet::from_text(&val).map_err(|err| D::Error::custom(err.to_string()))
}

fn default_query() -> String {
    "*".to_owned()
}

fn default_root() -> Facet {
    Facet::root()
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
    provenances: Vec<(String, u64)>,
    licenses: Vec<(String, u64)>,
    terms: Vec<String>,
    related_terms: HashSet<String>,
}

impl SearchPage {
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
