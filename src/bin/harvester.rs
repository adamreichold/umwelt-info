use std::env::var;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result};
use cap_std::{ambient_authority, fs::Dir};
use parking_lot::Mutex;
use tokio::task::{spawn, spawn_blocking};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use umwelt_info::{
    data_path_from_env,
    harvester::{
        ckan, client::Client, csw, doris_bfs, geo_network_q, smart_finder, wasser_de, Config,
        Group, Source, Type,
    },
    metrics::Metrics,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_path = data_path_from_env();

    let source_group = var("SOURCE_GROUP")
        .ok()
        .map(|val| val.parse::<Group>())
        .transpose()
        .context("Environment variable SOURCE_GROUP invalid")?;

    let dir = Dir::open_ambient_dir(&data_path, ambient_authority())?;

    let config = Config::read(&dir)?;

    let (active_sources, inactive_sources) =
        config
            .sources
            .into_iter()
            .partition::<Vec<_>, _>(|source| match source_group {
                Some(source_group) => source_group == source.group,
                None => true,
            });

    let count = active_sources.len();
    tracing::info!("Harvesting {} sources", count);

    let metrics = Arc::new(Mutex::new(Metrics::default()));

    let client = Client::start(&dir)?;

    let _ = dir.remove_dir_all("datasets.new");
    dir.create_dir("datasets.new")?;

    {
        let dir_new = Arc::new(dir.open_dir("datasets.new")?);
        let dir = Arc::new(dir.open_dir("datasets")?);

        let active_tasks = active_sources
            .into_iter()
            .map(|source| {
                let dir_new = dir_new.clone();
                let client = client.clone();
                let metrics = metrics.clone();

                spawn(async move { harvest(&dir_new, &client, &metrics, source).await })
            })
            .collect::<Vec<_>>();

        let inactive_tasks = inactive_sources
            .into_iter()
            .map(|source| {
                let dir = dir.clone();
                let dir_new = dir_new.clone();

                spawn_blocking(move || keep(&dir, &dir_new, source))
            })
            .collect::<Vec<_>>();

        let mut errors = 0;

        for task in active_tasks {
            if let Err(err) = task.await? {
                tracing::error!("{:#}", err);

                errors += 1;
            }
        }

        for task in inactive_tasks {
            task.await??;
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
        Type::GeoNetworkQ => geo_network_q::harvest(&dir, client, &source).await,
        Type::DorisBfs => doris_bfs::harvest(&dir, client, &source).await,
        Type::SmartFinder => smart_finder::harvest(&dir, client, &source).await,
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

#[tracing::instrument(skip(dir, dir_new))]
fn keep(dir: &Dir, dir_new: &Dir, source: Source) -> Result<()> {
    tracing::debug!("Keeping source {}", source.name);

    let dir = dir.open_dir(&source.name)?;

    dir_new.create_dir(&source.name)?;
    let dir_new = dir_new.open_dir(&source.name)?;

    for entry in dir.entries()? {
        let file_name = entry?.file_name();

        dir.hard_link(&file_name, &dir_new, &file_name)?;
    }

    Ok(())
}
