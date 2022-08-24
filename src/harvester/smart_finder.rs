use std::borrow::Cow;

use anyhow::Result;
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use smallvec::SmallVec;

use crate::{
    dataset::{Dataset, License},
    harvester::{client::Client, fetch_many, write_dataset, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
    let rows = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, source, rows, 0).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + rows - 1) / rows;
    let start = (1..requests).map(|request| request * rows);

    let (results, errors) = fetch_many(source, results, errors, start, |start| {
        fetch_datasets(dir, client, source, rows, start)
    })
    .await;

    Ok((count, results, errors))
}

#[tracing::instrument(skip(dir, client, source))]
async fn fetch_datasets(
    dir: &Dir,
    client: &Client,
    source: &Source,
    rows: usize,
    start: usize,
) -> Result<(usize, usize, usize)> {
    tracing::debug!("Fetching {} datasets starting at {}", rows, start);

    let body = client
        .make_request(&format!("{}-{}", source.name, start), |client| async {
            client
                .get(source.url.clone())
                .query(&SelectParams {
                    q: "*",
                    rows,
                    start,
                })
                .send()
                .await?
                .error_for_status()?
                .text()
                .await
        })
        .await?;

    let response = from_str::<SelectResponse>(&body)?;

    let count = response.results.num_found;
    let results = response.results.docs.len();
    let mut errors = 0;

    for doc in response.results.docs {
        if let Err(err) = translate_dataset(dir, source, doc).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

async fn translate_dataset(dir: &Dir, source: &Source, doc: Document<'_>) -> Result<()> {
    let dataset = Dataset {
        title: doc.title,
        description: doc.description,
        license: License::Unknown,
        tags: Vec::new(),
        source_url: source.source_url().replace("{{id}}", &doc.id),
        resources: SmallVec::new(),
        issued: None,
    };

    write_dataset(dir, &doc.id, dataset).await
}

#[derive(Debug, Serialize)]
struct SelectParams<'a> {
    q: &'a str,
    rows: usize,
    start: usize,
}

#[derive(Debug, Deserialize)]
struct SelectResponse<'a> {
    #[serde(rename = "response", borrow)]
    results: Results<'a>,
}

#[derive(Debug, Deserialize)]
struct Results<'a> {
    #[serde(rename = "numFound")]
    num_found: usize,
    #[serde(borrow)]
    docs: Vec<Document<'a>>,
}

#[derive(Debug, Deserialize)]
struct Document<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
    title: String,
    description: String,
}
