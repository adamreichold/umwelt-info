use std::cmp::Ordering;

use anyhow::{ensure, Result};
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::from_slice;

use crate::{
    dataset::Dataset,
    harvester::{client::Client, write_dataset, Source},
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

    #[derive(Serialize)]
    struct Params {
        start: usize,
        rows: usize,
    }

    let body = client
        .make_request(&format!("{}-{}", source.name, start), |client| async {
            client
                .get(url.clone())
                .query(&Params { start, rows })
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await
        })
        .await?;

    let response = from_slice::<PackageSearch>(&body)?;

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

async fn translate_dataset(dir: &Dir, source: &Source, package: Package) -> Result<()> {
    let license = package.license().into();

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
    fn license(&self) -> Option<&str> {
        if let Some(license_id) = &self.license_id {
            if !license_id.is_empty() {
                return Some(license_id);
            }
        }

        match self.resources.len().cmp(&1) {
            Ordering::Less => None,
            Ordering::Equal => self.resources[0].license.as_deref(),
            Ordering::Greater => {
                let (head, tail) = self.resources.split_first().unwrap();

                if tail.iter().all(|resource| resource.license == head.license) {
                    head.license.as_deref()
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
        let package = Package::default();

        assert_eq!(package.license(), None);
    }

    #[test]
    fn non_empty_license_no_resources() {
        let package = Package {
            license_id: Some("foobar".to_owned()),
            ..Default::default()
        };

        assert_eq!(package.license(), Some("foobar"));
    }

    #[test]
    fn empty_license_single_resource() {
        let package = Package {
            license_id: Some("".to_owned()),
            resources: vec![Resource {
                license: Some("foobar".to_owned()),
            }],
            ..Default::default()
        };

        assert_eq!(package.license(), Some("foobar"));
    }

    #[test]
    fn empty_license_multiple_matching_resources() {
        let package = Package {
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

        assert_eq!(package.license(), Some("foobar"));
    }

    #[test]
    fn empty_license_multiple_distinct_resources() {
        let package = Package {
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

        assert_eq!(package.license(), None);
    }

    #[test]
    fn non_empty_license_multiple_distinct_resources() {
        let package = Package {
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

        assert_eq!(package.license(), Some("foobar"));
    }
}
