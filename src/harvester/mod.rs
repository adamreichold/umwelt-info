pub mod ckan;
pub mod csw;
pub mod doris_bfs;
pub mod geo_network_q;
pub mod wasser_de;

use std::fmt;
use std::future::Future;
use std::io::Read;

use anyhow::Result;
use cap_std::fs::{Dir, OpenOptions as FsOpenOptions};
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use toml::from_str;
use url::Url;

use crate::dataset::Dataset;

async fn write_dataset(dir: &Dir, id: String, dataset: Dataset) -> Result<()> {
    let file = match dir.open_with(&id, FsOpenOptions::new().write(true).create_new(true)) {
        Ok(file) => file,
        Err(_err) => {
            let file = dir.create(&id)?;
            tracing::warn!("Overwriting duplicate dataset {id}");
            file
        }
    };

    dataset.write(file).await?;

    Ok(())
}

async fn with_retry<A, F, T>(mut action: A) -> Result<T>
where
    A: FnMut() -> F,
    F: Future<Output = Result<T>>,
{
    let mut attempts = 0;
    let mut duration = Duration::from_secs(1);

    loop {
        match action().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                if attempts < 3 {
                    tracing::warn!("Request failed but will be retried: {:#}", err);

                    sleep(duration).await;

                    attempts += 1;
                    duration *= 10;
                } else {
                    return Err(err);
                }
            }
        }
    }
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
        let val = from_str(&buf)?;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use tokio::time::{pause, Instant};

    #[tokio::test]
    async fn with_retry_fowards_success() {
        pause();
        let start = Instant::now();

        with_retry::<_, _, ()>(|| async { Ok(()) }).await.unwrap();

        assert_eq!(start.elapsed().as_secs(), 0);
    }

    #[tokio::test]
    async fn with_retry_fowards_failure() {
        pause();
        let start = Instant::now();

        with_retry::<_, _, ()>(|| async { Err(anyhow!("failure")) })
            .await
            .unwrap_err();

        assert_eq!(start.elapsed().as_secs(), 1 + 10 + 100);
    }

    #[tokio::test]
    async fn with_retry_retries_three_times() {
        pause();
        let start = Instant::now();

        let mut count = 0;

        with_retry::<_, _, ()>(|| {
            count += 1;

            async move {
                if count > 3 {
                    Ok(())
                } else {
                    Err(anyhow!("failure"))
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(start.elapsed().as_secs(), 1 + 10 + 100);
    }
}
