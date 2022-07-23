pub mod ckan;
pub mod csw;
pub mod wasser_de;

use std::fmt;
use std::fs::read_to_string;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;
use toml::from_str;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub sources: Vec<Source>,
}

impl Config {
    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let val = from_str(&read_to_string(path)?)?;

        Ok(val)
    }
}

#[derive(Deserialize)]
pub struct Source {
    pub name: String,
    pub r#type: Type,
    url: Url,
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
        fmt.debug_struct("Source")
            .field("name", &self.name)
            .field("type", &self.r#type)
            // The default format of `Url` is too verbose for the logs.
            .field("url", &self.url.as_str())
            .field("source_url", &self.source_url)
            .field("concurrency", &self.concurrency)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Ckan,
    Csw,
    WasserDe,
}
