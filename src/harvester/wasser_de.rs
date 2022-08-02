use anyhow::Result;
use cap_std::fs::Dir;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{dataset::Dataset, harvester::Source};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<()> {
    let url = source
        .url
        .join("rest/BaseController/FilterElements/V_REP_BASE_VALID")?;

    let response = client
        .post(url)
        .json(&Request { filter: Filter {} })
        .send()
        .await?
        .error_for_status()?
        .json::<Response>()
        .await?;

    tracing::info!("Retrieved {} documents", response.results.len().to_string());

    for document in response.results {
        if let Err(err) = write_dataset(dir, source, document).await {
            tracing::error!("{:#}", err);
        }
    }

    Ok(())
}

async fn write_dataset(dir: &Dir, source: &Source, document: Document) -> Result<()> {
    let title = match document.name {
        Some(name) => name,
        None => {
            tracing::warn!("Document {} has no valid entry for 'NAME' ", document.id);
            return Ok(());
        }
    };

    let teaser = document
        .teasertext
        .unwrap_or(document.autoteasertext.unwrap_or("".to_string()));

    let dataset = Dataset {
        title,
        description: teaser,
        source_url: source.url.clone(),
    };

    let file = dir.create(document.id.to_string())?;

    dataset.write(file).await?;

    Ok(())
}

#[derive(Serialize)]
struct Request {
    filter: Filter,
}

#[derive(Serialize)]
struct Filter {}

#[derive(Deserialize)]
struct Response {
    #[serde(rename = "V_REP_BASE_VALID")]
    results: Vec<Document>,
}

#[derive(Deserialize)]
struct Document {
    #[serde(rename = "ID")]
    id: usize,
    #[serde(rename = "NAME")]
    name: Option<String>,
    #[serde(rename = "TEASERTEXT")]
    teasertext: Option<String>,
    #[serde(rename = "AUTOTEASERTEXT")]
    autoteasertext: Option<String>,
}
