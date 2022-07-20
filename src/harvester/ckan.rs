use anyhow::{ensure, Result};
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use uuid::Uuid;

use crate::{dataset::Dataset, harvester::Source};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<()> {
    let url = source.url.join("/api/3/action/package_list")?;

    let package_list = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<PackageList>()
        .await?;

    ensure!(
        package_list.success,
        "Failed to retrieve package list: {}",
        package_list
            .error
            .as_ref()
            .map_or("Malformed response", |err| &err.message)
    );

    let count = package_list.result.len();
    tracing::info!("Harvesting {} datasets", count);

    let errors = iter(package_list.result)
        .map(|package_id| fetch_dataset(dir, client, source, package_id))
        .buffer_unordered(source.concurrency.unwrap_or(1))
        .fold(0, |mut errors, res| async move {
            if let Err(err) = res {
                tracing::error!("{:#}", err);

                errors += 1;
            }

            errors
        })
        .await;

    if errors != 0 {
        tracing::error!("Failed to harvest {} out of {} datasets", errors, count);
    }

    Ok(())
}

#[tracing::instrument(skip(dir, client))]
async fn fetch_dataset(
    dir: &Dir,
    client: &Client,
    source: &Source,
    package_id: String,
) -> Result<()> {
    tracing::debug!("Fetching dataset {}", package_id);

    let url = source.url.join("/api/3/action/package_show")?;

    let package_show = client
        .get(url)
        .query(&[("id", &package_id)])
        .send()
        .await?
        .error_for_status()?
        .json::<PackageShow>()
        .await?;

    ensure!(
        package_show.success,
        "Failed to fetch package: {}",
        package_show
            .error
            .as_ref()
            .map_or("Malformed response", |err| &err.message)
    );

    let dataset = Dataset {
        title: package_show.result.title,
        description: package_show.result.notes,
        source_url: source.url.join(&format!("/dataset/{}", package_id))?,
    };

    let file = dir.create(package_show.result.id.to_string())?;

    dataset.write(file).await?;

    Ok(())
}

#[derive(Deserialize)]
struct PackageList {
    success: bool,
    error: Option<Error>,
    result: Vec<String>,
}

#[derive(Deserialize)]
struct PackageShow {
    success: bool,
    error: Option<Error>,
    result: Package,
}

#[derive(Deserialize)]
struct Package {
    id: Uuid,
    title: String,
    notes: String,
}

#[derive(Deserialize)]
struct Error {
    message: String,
}
