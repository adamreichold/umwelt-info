use std::cmp::Ordering;
use std::mem::take;

use anyhow::{ensure, Result};
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use reqwest::Client;
use serde::Deserialize;

use crate::{
    dataset::Dataset,
    harvester::{with_retry, write_dataset, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
    let rows = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, source, 0, rows).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + rows - 1) / rows;
    let start = (1..requests).map(|request| request * rows);

    let (results, errors) = iter(start)
        .map(|start| fetch_datasets(dir, client, source, start, rows))
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

#[tracing::instrument(skip(dir, client, source))]
async fn fetch_datasets(
    dir: &Dir,
    client: &Client,
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
        if let Err(err) = translate_dataset(dir, source, package).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

async fn translate_dataset(dir: &Dir, source: &Source, mut package: Package) -> Result<()> {
    let license = package.take_license().into();

    let dataset = Dataset {
        title: package.title,
        description: package.notes.unwrap_or_default(),
        license,
        source_url: source.source_url().replace("{{name}}", &package.name),
    };

    write_dataset(dir, package.id, dataset).await
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

#[derive(Default, Deserialize)]
struct Package {
    id: String,
    name: String,
    title: String,
    notes: Option<String>,
    license_id: Option<String>,
    resources: Vec<Resource>,
}

impl Package {
    fn take_license(&mut self) -> Option<String> {
        if self.license_id.as_deref().map_or(0, str::len) != 0 {
            return take(&mut self.license_id);
        }

        match self.resources.len().cmp(&1) {
            Ordering::Less => None,
            Ordering::Equal => take(&mut self.resources[0].license),
            Ordering::Greater => {
                let (head, tail) = self.resources.split_first_mut().unwrap();

                if tail.iter().all(|resource| resource.license == head.license) {
                    take(&mut head.license)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct Resource {
    license: Option<String>,
}

#[derive(Deserialize)]
struct Error {
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_license_no_resources() {
        let mut package = Package::default();

        assert_eq!(package.take_license(), None);
    }

    #[test]
    fn non_empty_license_no_resources() {
        let mut package = Package {
            license_id: Some("foobar".to_owned()),
            ..Default::default()
        };

        assert_eq!(package.take_license(), Some("foobar".to_owned()));
    }

    #[test]
    fn empty_license_single_resource() {
        let mut package = Package {
            license_id: Some("".to_owned()),
            resources: vec![Resource {
                license: Some("foobar".to_owned()),
            }],
            ..Default::default()
        };

        assert_eq!(package.take_license(), Some("foobar".to_owned()));
    }

    #[test]
    fn empty_license_multiple_matching_resources() {
        let mut package = Package {
            license_id: Some("".to_owned()),
            resources: vec![
                Resource {
                    license: Some("foobar".to_owned()),
                },
                Resource {
                    license: Some("foobar".to_owned()),
                },
            ],
            ..Default::default()
        };

        assert_eq!(package.take_license(), Some("foobar".to_owned()));
    }

    #[test]
    fn empty_license_multiple_distinct_resources() {
        let mut package = Package {
            license_id: None,
            resources: vec![
                Resource {
                    license: Some("foo".to_owned()),
                },
                Resource {
                    license: Some("bar".to_owned()),
                },
            ],
            ..Default::default()
        };

        assert_eq!(package.take_license(), None);
    }

    #[test]
    fn non_empty_license_multiple_distinct_resources() {
        let mut package = Package {
            license_id: Some("foobar".to_owned()),
            resources: vec![
                Resource {
                    license: Some("foo".to_owned()),
                },
                Resource {
                    license: Some("bar".to_owned()),
                },
            ],
            ..Default::default()
        };

        assert_eq!(package.take_license(), Some("foobar".to_owned()));
    }
}
