use std::cmp::Reverse;
use std::convert::Infallible;
use std::env::var;
use std::net::SocketAddr;

use anyhow::Error;
use askama::Template;
use axum::{
    async_trait,
    extract::{Extension, FromRequest, Path, Query, RequestParts},
    http::{header::ACCEPT, StatusCode},
    response::{Html, IntoResponse, Json, Redirect, Response},
    routing::get,
    Router, Server,
};
use cap_std::{ambient_authority, fs::Dir};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::{
    task::{spawn, spawn_blocking},
    time::{interval_at, Duration, Instant, MissedTickBehavior},
};
use tower::{
    limit::GlobalConcurrencyLimitLayer, load_shed::LoadShedLayer, make::Shared, ServiceBuilder,
};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use umwelt_info::{
    data_path_from_env,
    dataset::{Dataset, License},
    index::Searcher,
    metrics::{Harvest as HarvestMetrics, Metrics},
    server::Stats,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_path = data_path_from_env();

    let bind_addr = var("BIND_ADDR")
        .expect("Environment variable BIND_ADDR not set")
        .parse::<SocketAddr>()
        .expect("Environment variable BIND_ADDR invalid");

    let request_limit = var("REQUEST_LIMIT")
        .expect("Environment variable REQUEST_LIMIT not set")
        .parse::<usize>()
        .expect("Environment variable REQUEST_LIMIT invalid");

    let searcher = &*Box::leak(Box::new(Searcher::open(&data_path)?));

    let dir = &*Box::leak(Box::new(Dir::open_ambient_dir(
        data_path,
        ambient_authority(),
    )?));

    let stats = &*Box::leak(Box::new(Mutex::new(Stats::read(dir)?)));

    spawn(write_stats(dir, stats));

    let router = Router::new()
        .route("/", get(|| async { Redirect::permanent("/search") }))
        .route("/search", get(search))
        .route("/dataset/:source/:id", get(dataset))
        .route("/metrics", get(metrics))
        .layer(Extension(searcher))
        .layer(Extension(dir))
        .layer(Extension(stats));

    let make_service = Shared::new(
        ServiceBuilder::new()
            .layer(LoadShedLayer::new())
            .layer(GlobalConcurrencyLimitLayer::new(request_limit))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::default().include_headers(true)),
            )
            .service(router),
    );

    tracing::info!("Listening on {}", bind_addr);
    Server::bind(&bind_addr).serve(make_service).await?;

    Ok(())
}

async fn write_stats(dir: &'static Dir, stats: &'static Mutex<Stats>) {
    let mut interval = interval_at(
        Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        spawn_blocking(move || {
            if let Err(err) = Stats::write(stats, dir) {
                tracing::warn!("Failed to write stats: {:#}", err);
            }
        })
        .await
        .unwrap();
    }
}

#[derive(Deserialize, Serialize)]
struct SearchParams {
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

async fn search(
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

#[derive(Template, Serialize)]
#[template(path = "dataset.html")]
struct DatasetPage {
    source: String,
    id: String,
    dataset: Dataset,
    accesses: u64,
}

async fn dataset(
    Path((source, id)): Path<(String, String)>,
    accept: Accept,
    Extension(dir): Extension<&'static Dir>,
    Extension(stats): Extension<&'static Mutex<Stats>>,
) -> Result<Response, ServerError> {
    fn inner(
        source: String,
        id: String,
        dir: &Dir,
        stats: &Mutex<Stats>,
    ) -> Result<DatasetPage, ServerError> {
        let dir = dir.open_dir("datasets")?;

        let dataset = Dataset::read(dir.open_dir(&source)?.open(&id)?)?;

        let accesses = stats.lock().record_access(&source, &id);

        let page = DatasetPage {
            source,
            id,
            dataset,
            accesses,
        };

        Ok(page)
    }

    let page = inner(source, id, dir, stats)?;

    Ok(accept.into_repsonse(page))
}

#[derive(Template)]
#[template(path = "metrics.html")]
struct MetricsPage<'a> {
    accesses: Vec<(&'a String, u64)>,
    sum_accesses: u64,
    harvests: Vec<(&'a String, &'a HarvestMetrics)>,
    sum_count: usize,
    sum_transmitted: usize,
    sum_failed: usize,
    licenses: Vec<(String, usize)>,
    sum_other: usize,
}

async fn metrics(Extension(dir): Extension<&'static Dir>) -> Result<Html<String>, ServerError> {
    fn inner(dir: &Dir) -> Result<Html<String>, ServerError> {
        let stats = Stats::read(dir)?;

        let mut accesses = stats
            .accesses
            .iter()
            .map(|(source_name, accesses)| (source_name, accesses.values().sum()))
            .collect::<Vec<_>>();

        accesses.sort_unstable_by_key(|(_, accesses)| Reverse(*accesses));

        let sum_accesses = accesses.iter().map(|(_, accesses)| accesses).sum();

        let metrics = Metrics::read(dir)?;

        let mut harvests = metrics.harvests.iter().collect::<Vec<_>>();

        harvests.sort_unstable_by_key(|(_, harvest)| Reverse(harvest.start));

        let (sum_count, sum_transmitted, sum_failed) = metrics.harvests.values().fold(
            (0, 0, 0),
            |(sum_count, sum_transmitted, sum_failed), harvest| {
                (
                    sum_count + harvest.count,
                    sum_transmitted + harvest.transmitted,
                    sum_failed + harvest.failed,
                )
            },
        );

        let mut licenses = metrics
            .licenses
            .iter()
            .map(|(license, count)| (license.to_string(), *count))
            .collect::<Vec<_>>();

        licenses.sort_unstable_by_key(|(_, count)| Reverse(*count));

        let sum_other = metrics
            .licenses
            .iter()
            .filter(|(license, _)| matches!(license, License::Other(_)))
            .map(|(_, count)| *count)
            .sum();

        let page = MetricsPage {
            accesses,
            sum_accesses,
            harvests,
            sum_count,
            sum_transmitted,
            sum_failed,
            licenses,
            sum_other,
        };

        let page = Html(page.render().unwrap());

        Ok(page)
    }

    spawn_blocking(|| inner(dir)).await?
}

#[derive(Debug, Clone, Copy)]
enum Accept {
    Unspecified,
    Html,
    Json,
}

impl Accept {
    fn into_repsonse<P>(self, page: P) -> Response
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

enum ServerError {
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

mod filters {
    use std::time::{Duration, SystemTime};

    use askama::Result;
    use time::{macros::format_description, OffsetDateTime};

    pub fn system_time(val: &SystemTime) -> Result<String> {
        let val = OffsetDateTime::from(*val)
            .format(format_description!("[day].[month].[year] [hour]:[minute]"))
            .unwrap();

        Ok(val)
    }

    pub fn duration(val: &Duration) -> Result<String> {
        let secs = val.as_secs();

        let val = if secs > 3600 {
            format!("{}h", secs / 3600)
        } else if secs > 60 {
            format!("{}min", secs / 60)
        } else {
            format!("{}s", secs)
        };

        Ok(val)
    }
}
