use anyhow::Result;
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_roxmltree::from_str;

use crate::harvester::{csw, with_retry, Source};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
    let entries = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, source, true, 1, entries).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + entries - 1) / entries;
    let from = (1..requests).map(|request| 1 + request * entries);
    let to = from.clone().map(|from| from + entries - 1);

    iter(from.zip(to))
        .map(|(from, to)| fetch_datasets(dir, client, source, false, from, to))
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

    Ok((count, results, errors))
}

#[tracing::instrument(skip(dir, client, source))]
async fn fetch_datasets(
    dir: &Dir,
    client: &Client,
    source: &Source,
    summary: bool,
    from: usize,
    to: usize,
) -> Result<(usize, usize, usize)> {
    tracing::debug!("Fetching datasets from {} to {}", from, to);

    let response = with_retry(|| async {
        let body = client
            .get(source.url.clone())
            .query(&SearchParams {
                fast: false,
                summary,
                from,
                to,
                topic: source.filter.as_deref(),
            })
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        let response: SearchResults = from_str(&body)?;

        Ok(response)
    })
    .await?;

    let count = if let Some(summary) = response.summary {
        summary.count.parse()?
    } else {
        0
    };

    let results = response.records.len();
    let mut errors = 0;

    for record in response.records {
        if let Err(err) = csw::translate_dataset(dir, source, record).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

#[derive(Debug, Serialize)]
struct SearchParams<'a> {
    fast: bool,
    #[serde(rename = "buildSummary")]
    summary: bool,
    from: usize,
    to: usize,
    #[serde(rename = "topicCat", skip_serializing_if = "Option::is_none")]
    topic: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct SearchResults {
    summary: Option<Summary>,
    #[serde(rename = "MD_Metadata")]
    records: Vec<csw::Record>,
}

#[derive(Debug, Deserialize)]
struct Summary {
    count: String,
}
