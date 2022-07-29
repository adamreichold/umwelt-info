pub mod ckan;
pub mod csw;

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

#[derive(Debug, Deserialize)]
pub struct Source {
    pub name: String,
    pub r#type: Type,
    url: Url,
    source_url: Option<Url>,
    concurrency: Option<usize>,
    batch_size: Option<usize>,
}

impl Source {
    fn source_url(&self) -> &Url {
        self.source_url.as_ref().unwrap_or(&self.url)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Ckan,
    CkanSearch,
    Csw,
}
