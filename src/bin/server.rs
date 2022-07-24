use std::cmp::Reverse;
use std::env::var;
use std::net::SocketAddr;
use std::sync::Mutex;

use anyhow::Error;
use askama::Template;
use axum::{
    extract::{Extension, Path, Query},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
    Router, Server,
};
use cap_std::{ambient_authority, fs::Dir};
use serde::Deserialize;
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
    dataset::Dataset,
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
        });
    }
}

#[derive(Deserialize)]
struct SearchParams {
    #[serde(default = "default_query")]
    query: String,
}

fn default_query() -> String {
    "*".to_owned()
}

#[derive(Template)]
#[template(path = "search.html")]
struct SearchResults {
    query: String,
    count: usize,
    results: Vec<SearchResult>,
}

struct SearchResult {
    source: String,
    id: String,
    title: String,
    description: String,
}

async fn search(
    Query(params): Query<SearchParams>,
    Extension(searcher): Extension<&'static Searcher>,
    Extension(dir): Extension<&'static Dir>,
) -> Result<Html<String>, ServerError> {
    fn inner(
        params: SearchParams,
        searcher: &Searcher,
        dir: &Dir,
    ) -> Result<Html<String>, ServerError> {
        let (count, docs) = searcher.search(&params.query)?;

        tracing::debug!("Found {} documents", count);

        let mut results = SearchResults {
            query: params.query,
            count,
            results: Vec::new(),
        };

        let dir = dir.open_dir("datasets")?;

        for doc in docs {
            let (source, id) = doc?;

            let dataset = Dataset::read(dir.open_dir(&source)?.open(&id)?)?;

            results.results.push(SearchResult {
                source,
                id,
                title: dataset.title,
                description: dataset.description,
            });
        }

        let results = Html(results.render().unwrap());

        Ok(results)
    }

    spawn_blocking(|| inner(params, searcher, dir))
        .await
        .unwrap()
}

#[derive(Template)]
#[template(path = "dataset.html")]
struct DatasetPage {
    source: String,
    id: String,
    dataset: Dataset,
    accesses: u64,
}

async fn dataset(
    Path((source, id)): Path<(String, String)>,
    Extension(dir): Extension<&'static Dir>,
    Extension(stats): Extension<&'static Mutex<Stats>>,
) -> Result<Html<String>, ServerError> {
    let dir = dir.open_dir("datasets")?;

    let dataset = Dataset::read(dir.open_dir(&source)?.open(&id)?)?;

    let accesses = stats.lock().unwrap().record_access(&source, &id);

    let page = DatasetPage {
        source,
        id,
        dataset,
        accesses,
    };

    let page = Html(page.render().unwrap());

    Ok(page)
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

        let page = MetricsPage {
            accesses,
            sum_accesses,
            harvests,
            sum_count,
            sum_transmitted,
            sum_failed,
        };

        let page = Html(page.render().unwrap());

        Ok(page)
    }

    spawn_blocking(|| inner(dir)).await.unwrap()
}

struct ServerError(Error);

impl<E> From<E> for ServerError
where
    Error: From<E>,
{
    fn from(err: E) -> Self {
        Self(Error::from(err))
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
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
