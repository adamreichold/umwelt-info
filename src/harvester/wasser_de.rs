use anyhow::{anyhow, Result};
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use serde_json::from_slice;

use crate::{
    dataset::Dataset,
    harvester::{client::Client, write_dataset, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
    let url = source
        .url
        .join("rest/BaseController/FilterElements/V_REP_BASE_VALID")?;

    let body = client
        .make_request(&source.name, |client| async {
            client
                .post(url.clone())
                .json(&Request { filter: Filter {} })
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await
        })
        .await?;

    let response = from_slice::<Response>(&body)?;

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
        license: document.license.as_str().into(),
        source_url: source.url.clone().into(),
    };

    write_dataset(dir, &document.id.to_string(), dataset).await
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
    /// An alternative text to TEASERTEXT.
    #[serde(rename = "AUTOTEASERTEXT")]
    auto_teaser_text: Option<String>,
    #[serde(rename = "LICENSE_NAME_KURZ")]
    license: String,
}
