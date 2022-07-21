use anyhow::{ensure, Result};
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use uuid::Uuid;

use crate::{dataset::Dataset, harvester::Source};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<()> {
    let url = source.url.join("api/3/action/package_list")?;

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

    let url = source.url.join("api/3/action/package_show")?;

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

    write_dataset(dir, source, package_show.result).await?;

    Ok(())
}

pub async fn harvest_search(dir: &Dir, client: &Client, source: &Source) -> Result<()> {
    let concurrency = source.concurrency.unwrap_or(1);
    let rows = source.batch_size.unwrap_or(100);

    let (count, results, errors) = fetch_datasets_search(dir, client, source, 0, rows).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + rows - 1) / rows;
    let start = (1..requests).map(|request| request * rows);

    let (results, errors) = iter(start)
        .map(|start| fetch_datasets_search(dir, client, source, start, rows))
        .buffer_unordered(concurrency)
        .fold(
            (results, errors),
            |(mut results, mut errors), res| async move {
                match res {
                    Ok((_count, results1, errors1)) => {
                        results += results1;
                        errors += errors1;
                    }
                    Err(err) => {
                        tracing::error!("{:#}", err);

                        errors += 1;
                    }
                }

                (results, errors)
            },
        )
        .await;

    if errors != 0 {
        tracing::error!("Failed to harvest {} out of {} datasets", errors, results);
    }

    Ok(())
}

#[tracing::instrument(skip(dir, client))]
async fn fetch_datasets_search(
    dir: &Dir,
    client: &Client,
    source: &Source,
    start: usize,
    rows: usize,
) -> Result<(usize, usize, usize)> {
    tracing::debug!("Fetching {} datasets starting at {}", rows, start);

    let url = source.url.join("api/3/action/package_search")?;

    let package_search = client
        .get(url)
        .query(&[("start", start.to_string()), ("rows", rows.to_string())])
        .send()
        .await?
        .error_for_status()?
        .json::<PackageSearch>()
        .await?;

    ensure!(
        package_search.success,
        "Failed to fetch packages: {}",
        package_search
            .error
            .as_ref()
            .map_or("Malformed response", |err| &err.message)
    );

    let count = package_search.result.count;
    let results = package_search.result.results.len();
    let mut errors = 0;

    for package in package_search.result.results {
        if let Err(err) = write_dataset(dir, source, package).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

async fn write_dataset(dir: &Dir, source: &Source, package: Package) -> Result<()> {
    let dataset = Dataset {
        title: package.title,
        description: package.notes.unwrap_or_default(),
        source_url: source.url.join(&format!("dataset/{}", package.id))?,
    };

    let file = dir.create(package.id.to_string())?;

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
struct PackageSearch {
    success: bool,
    error: Option<Error>,
    result: PackageSearchResult,
}

#[derive(Deserialize)]
struct PackageSearchResult {
    count: usize,
    results: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    id: Uuid,
    title: String,
    notes: Option<String>,
}

#[derive(Deserialize)]
struct Error {
    message: String,
}
