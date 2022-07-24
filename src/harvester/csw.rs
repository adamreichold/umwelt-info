use anyhow::Result;
use askama::Template;
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use quick_xml::de::from_slice;
use reqwest::{header::CONTENT_TYPE, Client};
use serde::Deserialize;

use crate::{
    dataset::{Dataset, License},
    harvester::Source,
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<()> {
    let max_records = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, source, max_records, 1).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + max_records - 1) / max_records;
    let start_pos = (1..requests).map(|request| 1 + request * max_records);

    let (results, errors) = iter(start_pos)
        .map(|start_pos| fetch_datasets(dir, client, source, max_records, start_pos))
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

    if errors != 0 {
        tracing::error!(
            "Failed to harvest {} out of {} datasets ({} were transmitted)",
            errors,
            count,
            results
        );
    }

    Ok(())
}

#[tracing::instrument(skip(dir, client, source))]
async fn fetch_datasets(
    dir: &Dir,
    client: &Client,
    source: &Source,
    max_records: usize,
    start_pos: usize,
) -> Result<(usize, usize, usize)> {
    tracing::debug!(
        "Fetching {} datasets starting at {}",
        max_records,
        start_pos
    );

    let body = GetRecordsRequest {
        max_records,
        start_pos,
    }
    .render()
    .unwrap();

    let response = client
        .post(source.url.clone())
        .header(CONTENT_TYPE, "application/xml")
        .body(body)
        .send()
        .await?
        .error_for_status()?;

    let body = response.bytes().await?;

    let response: GetRecordsResponse = from_slice(&body)?;

    let count = response.results.num_records_matched;
    let results = response.results.num_records_returned;
    let mut errors = 0;

    for record in response.results.records {
        if let Err(err) = write_dataset(dir, source, record).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

async fn write_dataset(dir: &Dir, source: &Source, record: SummaryRecord) -> Result<()> {
    let dataset = Dataset {
        title: record.title,
        description: record.r#abstract,
        license: License::Unknown,
        source_url: source.source_url().join(&record.identifier)?,
    };

    let file = dir.create(record.identifier)?;

    dataset.write(file).await?;

    Ok(())
}

#[derive(Template)]
#[template(path = "csw_get_records.xml")]
struct GetRecordsRequest {
    max_records: usize,
    start_pos: usize,
}

#[derive(Debug, Deserialize)]
struct GetRecordsResponse {
    #[serde(rename = "SearchResults")]
    results: SearchResults,
}

#[derive(Debug, Deserialize)]
struct SearchResults {
    #[serde(rename = "numberOfRecordsMatched")]
    num_records_matched: usize,
    #[serde(rename = "numberOfRecordsReturned")]
    num_records_returned: usize,
    #[serde(rename = "SummaryRecord")]
    records: Vec<SummaryRecord>,
}

#[derive(Debug, Deserialize)]
struct SummaryRecord {
    identifier: String,
    title: String,
    r#abstract: String,
}
