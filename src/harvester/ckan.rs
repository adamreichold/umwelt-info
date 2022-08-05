use anyhow::{ensure, Result};
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use parking_lot::Mutex;
use reqwest::Client;
use serde::Deserialize;

use crate::{
    dataset::Dataset,
    harvester::{with_retry, write_dataset, Source},
    metrics::Metrics,
};

pub async fn harvest(
    dir: &Dir,
    client: &Client,
    metrics: &Mutex<Metrics>,
    source: &Source,
) -> Result<(usize, usize, usize)> {
    let rows = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, metrics, source, 0, rows).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + rows - 1) / rows;
    let start = (1..requests).map(|request| request * rows);

    let (results, errors) = iter(start)
        .map(|start| fetch_datasets(dir, client, metrics, source, start, rows))
        .buffer_unordered(source.concurrency)
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

    Ok((count, results, errors))
}

#[tracing::instrument(skip(dir, client, metrics, source))]
async fn fetch_datasets(
    dir: &Dir,
    client: &Client,
    metrics: &Mutex<Metrics>,
    source: &Source,
    start: usize,
    rows: usize,
) -> Result<(usize, usize, usize)> {
    tracing::debug!("Fetching {} datasets starting at {}", rows, start);

    let url = source.url.join("api/3/action/package_search")?;

    let response = with_retry(|| async {
        let response = client
            .get(url.clone())
            .query(&[("start", start.to_string()), ("rows", rows.to_string())])
            .send()
            .await?
            .error_for_status()?
            .json::<PackageSearch>()
            .await?;

        Ok(response)
    })
    .await?;

    ensure!(
        response.success,
        "Failed to fetch packages: {}",
        response
            .error
            .as_ref()
            .map_or("Malformed response", |err| &err.message)
    );

    let count = response.result.count;
    let results = response.result.results.len();
    let mut errors = 0;

    for package in response.result.results {
        if let Err(err) = translate_dataset(dir, metrics, source, package).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

async fn translate_dataset(
    dir: &Dir,
    metrics: &Mutex<Metrics>,
    source: &Source,
    package: Package,
) -> Result<()> {
    let dataset = Dataset {
        title: package.title,
        description: package.notes.unwrap_or_default(),
        license: package.license_id.into(),
        source_url: source.source_url().replace("{{name}}", &package.name),
    };

    write_dataset(dir, metrics, package.id, dataset).await
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
    id: String,
    name: String,
    title: String,
    notes: Option<String>,
    license_id: Option<String>,
}

#[derive(Deserialize)]
struct Error {
    message: String,
}
