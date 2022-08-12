use anyhow::{anyhow, Result};
use cap_std::fs::Dir;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{
    dataset::{Dataset, License},
    harvester::{with_retry, write_dataset, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
    let url = source
        .url
        .join("rest/BaseController/FilterElements/V_REP_BASE_VALID")?;

    let response = with_retry(|| async {
        let response = client
            .post(url.clone())
            .json(&Request { filter: Filter {} })
            .send()
            .await?
            .error_for_status()?
            .json::<Response>()
            .await?;

        Ok(response)
    })
    .await?;

    let count = response.results.len();
    tracing::info!("Retrieved {count} documents");

    let mut errors = 0;

    for document in response.results {
        if let Err(err) = translate_dataset(dir, source, document).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, count, errors))
}

async fn translate_dataset(dir: &Dir, source: &Source, document: Document) -> Result<()> {
    let title = document
        .name
        .ok_or_else(|| anyhow!("Document {} has no valid entry for 'NAME'", document.id))?;

    let description = document
        .teaser_text
        .or(document.auto_teaser_text)
        .unwrap_or_default();

    let dataset = Dataset {
        title,
        description,
        license: License::Unknown,
        source_url: source.url.clone().into(),
    };

    write_dataset(dir, document.id.to_string(), dataset).await
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
    teaser_text: Option<String>,
    #[serde(rename = "AUTOTEASERTEXT")]
    auto_teaser_text: Option<String>,
}
