use anyhow::Result;
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use quick_xml::de::from_slice;
use reqwest::Client;
use serde::Deserialize;

use crate::{
    dataset::Dataset,
    harvester::{with_retry, Source},
};

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
            .query(&[
                ("fast", "false"),
                ("buildSummary", &summary.to_string()),
                ("from", &from.to_string()),
                ("to", &to.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        let response: SearchResults = from_slice(&body)?;

        Ok(response)
    })
    .await?;

    let count = if let Some(summary) = response.summary {
        summary.count.parse()?
    } else {
        0
    };

    let results = response.metadata.len();
    let mut errors = 0;

    for entry in response.metadata {
        if let Err(err) = write_dataset(dir, source, entry).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

async fn write_dataset(dir: &Dir, source: &Source, entry: Metadata) -> Result<()> {
    let identifier = entry.file_identifier.text;

    let identification = entry.identification_info.inner.identification();
    let title = identification.citation.inner.title.text;
    let description = identification.r#abstract.text.unwrap_or_default();

    let dataset = Dataset {
        title,
        description,
        source_url: source.source_url().replace("{{id}}", &identifier),
    };

    let file = dir.create(identifier)?;

    dataset.write(file).await?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct SearchResults {
    summary: Option<Summary>,
    #[serde(rename = "MD_Metadata")]
    metadata: Vec<Metadata>,
}

#[derive(Debug, Deserialize)]
struct Summary {
    count: String,
}

#[derive(Debug, Deserialize)]
struct Metadata {
    #[serde(rename = "fileIdentifier")]
    file_identifier: FileIdentifier,
    #[serde(rename = "identificationInfo")]
    identification_info: IdentificationInfo,
}

#[derive(Debug, Deserialize)]
struct FileIdentifier {
    #[serde(rename = "CharacterString")]
    text: String,
}

#[derive(Debug, Deserialize)]
struct IdentificationInfo {
    #[serde(rename = "$value")]
    inner: IdentificationInfoInner,
}

#[derive(Debug, Deserialize)]
enum IdentificationInfoInner {
    #[serde(rename = "gmd:MD_DataIdentification")]
    Data(Identification),
    #[serde(rename = "srv:SV_ServiceIdentification")]
    Service(Identification),
}

impl IdentificationInfoInner {
    fn identification(self) -> Identification {
        match self {
            Self::Data(identification) => identification,
            Self::Service(identification) => identification,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Identification {
    citation: Citation,
    r#abstract: Abstract,
}

#[derive(Debug, Deserialize)]
struct Abstract {
    #[serde(rename = "CharacterString")]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Citation {
    #[serde(rename = "CI_Citation")]
    inner: CitationInner,
}

#[derive(Debug, Deserialize)]
struct CitationInner {
    title: Title,
}

#[derive(Debug, Deserialize)]
struct Title {
    #[serde(rename = "CharacterString")]
    text: String,
}
