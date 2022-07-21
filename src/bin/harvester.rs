use std::sync::Arc;

use anyhow::{Context, Result};
use cap_std::{ambient_authority, fs::Dir};
use reqwest::Client;
use tokio::{
    fs::{create_dir, remove_dir_all, rename},
    spawn,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use umwelt_info::{
    data_path_from_env,
    harvester::{ckan, csw, Config, Source, Type},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_path = data_path_from_env();

    let config = Config::read(data_path.join("harvester.toml"))?;

    let count = config.sources.len();
    tracing::info!("Harvesting {} sources", count);

    let datasets_path = data_path.join("datasets");
    let datasets_path_new = data_path.join("datasets.new");
    let datasets_path_old = data_path.join("datasets.old");

    let _ = remove_dir_all(&datasets_path_new).await;
    let _ = remove_dir_all(&datasets_path_old).await;
    create_dir(&datasets_path_new).await?;

    let dir = Arc::new(Dir::open_ambient_dir(
        &datasets_path_new,
        ambient_authority(),
    )?);

    let client = Client::new();

    let tasks = config
        .sources
        .into_iter()
        .map(|source| {
            let dir = dir.clone();
            let client = client.clone();

            spawn(async move { harvest(&dir, &client, source).await })
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

    drop(dir);

    if datasets_path.exists() {
        rename(&datasets_path, &datasets_path_old).await?;
        rename(&datasets_path_new, &datasets_path).await?;
        remove_dir_all(&datasets_path_old).await?;
    } else {
        rename(&datasets_path_new, &datasets_path).await?;
    }

    Ok(())
}

#[tracing::instrument(skip(dir, client))]
async fn harvest(dir: &Dir, client: &Client, source: Source) -> Result<()> {
    tracing::debug!("Harvesting source {}", source.name);

    dir.create_dir(&source.name)?;
    let dir = dir.open_dir(&source.name)?;

    let res = match source.r#type {
        Type::Ckan => ckan::harvest(&dir, client, &source).await,
        Type::CkanSearch => ckan::harvest_search(&dir, client, &source).await,
        Type::Csw => csw::harvest(&dir, client, &source).await,
    };

    res.with_context(move || format!("Failed to harvest source {}", source.name))
}
