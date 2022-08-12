use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use cap_std::{ambient_authority, fs::Dir};
use parking_lot::Mutex;
use reqwest::Client;
use tokio::spawn;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use umwelt_info::{
    data_path_from_env,
    harvester::{ckan, csw, doris_bfs, geo_network_q, wasser_de, Config, Source, Type},
    metrics::Metrics,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_path = data_path_from_env();

    let dir = Dir::open_ambient_dir(&data_path, ambient_authority())?;

    let config = Config::read(&dir)?;

    let count = config.sources.len();
    tracing::info!("Harvesting {} sources", count);

    let metrics = Arc::new(Mutex::new(Metrics::default()));

    let _ = dir.remove_dir_all("datasets.new");
    dir.create_dir("datasets.new")?;

    {
        let dir = Arc::new(dir.open_dir("datasets.new")?);

        let client = Client::builder()
            .user_agent("umwelt.info harvester")
            .timeout(Duration::from_secs(300))
            .build()?;

        let tasks = config
            .sources
            .into_iter()
            .map(|source| {
                let dir = dir.clone();
                let client = client.clone();
                let metrics = metrics.clone();

                spawn(async move { harvest(&dir, &client, &metrics, source).await })
            })
            .collect::<Vec<_>>();

        let mut errors = 0;

        for task in tasks {
            if let Err(err) = task.await? {
                tracing::error!("{:#}", err);

                errors += 1;
            }
        }

        if errors != 0 {
            tracing::error!("Failed to harvest {} out of {} sources", errors, count);
        }
    }

    if dir.exists("datasets") {
        let _ = dir.remove_dir_all("datasets.old");
        dir.rename("datasets", &dir, "datasets.old")?;
        dir.rename("datasets.new", &dir, "datasets")?;
    } else {
        dir.rename("datasets.new", &dir, "datasets")?;
    }

    Arc::try_unwrap(metrics).unwrap().into_inner().write(&dir)?;

    Ok(())
}

#[tracing::instrument(skip(dir, client, metrics))]
async fn harvest(
    dir: &Dir,
    client: &Client,
    metrics: &Mutex<Metrics>,
    source: Source,
) -> Result<()> {
    tracing::debug!("Harvesting source {}", source.name);

    dir.create_dir(&source.name)?;
    let dir = dir.open_dir(&source.name)?;

    let start = SystemTime::now();

    let res = match source.r#type {
        Type::Ckan => ckan::harvest(&dir, client, &source).await,
        Type::Csw => csw::harvest(&dir, client, &source).await,
        Type::WasserDe => wasser_de::harvest(&dir, client, &source).await,
        Type::DorisBfs => doris_bfs::harvest(&dir, client, &source).await,
        Type::GeoNetworkQ => geo_network_q::harvest(&dir, client, &source).await,
    };

    let (count, transmitted, failed) =
        res.with_context(|| format!("Failed to harvest source {}", source.name))?;

    if failed != 0 {
        tracing::error!(
            "Failed to harvest {failed} out of {count} datasets ({transmitted} were transmitted)"
        );
    }

    let duration = start.elapsed()?;
    metrics
        .lock()
        .record_harvest(source.name, start, duration, count, transmitted, failed);

    Ok(())
}
