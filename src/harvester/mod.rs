pub mod ckan;
pub mod client;
pub mod csw;
pub mod doris_bfs;
pub mod geo_network_q;
pub mod smart_finder;
pub mod wasser_de;

use std::fmt;
use std::future::Future;
use std::io::Read;

use anyhow::{ensure, Result};
use cap_std::fs::{Dir, OpenOptions as FsOpenOptions};
use futures_util::stream::{iter, StreamExt};
use hashbrown::HashSet;
use serde::Deserialize;
use toml::from_str;
use url::Url;

use crate::dataset::Dataset;

async fn write_dataset(dir: &Dir, id: &str, dataset: Dataset) -> Result<()> {
    let file = match dir.open_with(id, FsOpenOptions::new().write(true).create_new(true)) {
        Ok(file) => file,
        Err(_err) => {
            let file = dir.create(id)?;
            tracing::warn!("Overwriting duplicate dataset {id}");
            file
        }
    };

    dataset.write(file).await?;

    Ok(())
}

async fn fetch_many<R, T, M, F>(
    source: &Source,
    results: usize,
    errors: usize,
    requests: R,
    make_request: M,
) -> (usize, usize)
where
    R: Iterator<Item = T>,
    M: Fn(T) -> F,
    F: Future<Output = Result<(usize, usize, usize)>>,
{
    iter(requests)
        .map(make_request)
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

                        errors += source.batch_size;
                    }
                }

                (results, errors)
            },
        )
        .await
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub sources: Vec<Source>,
}

impl Config {
    pub fn read(dir: &Dir) -> Result<Self> {
        let mut file = dir.open("harvester.toml")?;

        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        let val = from_str::<Self>(&buf)?;

        {
            let mut source_names = HashSet::new();

            for source in &val.sources {
                ensure!(
                    source_names.insert(&source.name),
                    "Source names must be unique but {} was used twice",
                    source.name
                );
            }
        }

        Ok(val)
    }
}

#[derive(Deserialize)]
pub struct Source {
    pub name: String,
    pub r#type: Type,
    url: Url,
    filter: Option<String>,
    source_url: Option<String>,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

fn default_concurrency() -> usize {
    1
}

fn default_batch_size() -> usize {
    100
}

impl Source {
    pub fn source_url(&self) -> &str {
        self.source_url
            .as_deref()
            .unwrap_or_else(|| self.url.as_str())
    }
}

impl fmt::Debug for Source {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let Self {
            name,
            r#type,
            url,
            filter,
            source_url,
            concurrency,
            batch_size,
        } = self;

        fmt.debug_struct("Source")
            .field("name", name)
            .field("type", r#type)
            // The default format of `Url` is too verbose for the logs.
            .field("url", &url.as_str())
            .field("filter", filter)
            .field("source_url", source_url)
            .field("concurrency", concurrency)
            .field("batch_size", batch_size)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Ckan,
    Csw,
    WasserDe,
    GeoNetworkQ,
    DorisBfs,
    SmartFinder,
}
