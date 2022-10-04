use std::env::var;
use std::net::SocketAddr;

use anyhow::Error;
use axum::{extract::Extension, response::Redirect, routing::get, Router, Server};
use cap_std::{ambient_authority, fs::Dir};
use parking_lot::Mutex;
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
    index::Searcher,
    server::{
        dataset::dataset,
        metrics::metrics,
        search::{completions, search},
        stats::Stats,
    },
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
        .route("/completions", get(completions))
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
