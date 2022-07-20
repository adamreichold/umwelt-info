pub mod ckan;

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
    pub url: Url,
    pub concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    CKAN,
}
