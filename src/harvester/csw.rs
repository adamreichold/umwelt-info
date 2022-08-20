use std::borrow::Cow;

use anyhow::Result;
use askama::Template;
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde_json::from_str as from_json_str;
use serde_roxmltree::from_str as from_xml_str;

use crate::{
    dataset::Dataset,
    harvester::{client::Client, write_dataset, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
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

    Ok((count, results, errors))
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

    let body = client
        .make_request(&format!("{}-{}", source.name, start_pos), |client| async {
            client
                .post(source.url.clone())
                .header(CONTENT_TYPE, "application/xml")
                .body(body.clone())
                .send()
                .await?
                .error_for_status()?
                .text()
                .await
        })
        .await?;

    let response = from_xml_str::<GetRecordsResponse>(&body)?;

    let count = response.results.num_records_matched;
    let results = response.results.records.len();
    let mut errors = 0;

    for record in response.results.records {
        if let Err(err) = translate_dataset(dir, source, record).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

pub async fn translate_dataset(dir: &Dir, source: &Source, record: Record) -> Result<()> {
    let identifier = record.file_identifier.text;

    let identification = record.identification_info.identification();

    let license = identification.license().as_deref().into();

    let title = identification.citation.inner.title.text;
    let description = identification.r#abstract.text.unwrap_or_default();

    let dataset = Dataset {
        title,
        description,
        license,
        source_url: source.source_url().replace("{{id}}", &identifier),
    };

    write_dataset(dir, &identifier, dataset).await
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
    #[serde(rename = "MD_Metadata")]
    records: Vec<Record>,
}

#[derive(Debug, Deserialize)]
pub struct Record {
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
enum IdentificationInfo {
    #[serde(rename = "MD_DataIdentification")]
    Data(Identification),
    #[serde(rename = "SV_ServiceIdentification")]
    Service(Identification),
}

impl IdentificationInfo {
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
    #[serde(rename = "resourceConstraints", default)]
    resource_constraints: Vec<ResourceConstraints>,
}

impl Identification {
    /// Extract the license ID for Open Data licenses
    ///
    /// Based on section 3.6 from [Konventionen zu Metadaten][https://www.gdi-de.org/download/AK_Metadaten_Konventionen_zu_Metadaten.pdf].
    fn license(&self) -> Option<Cow<str>> {
        for resource_constraints in &self.resource_constraints {
            if let Some(legal_constraints) = resource_constraints.legal_constraints.as_ref() {
                for use_constraints in &legal_constraints.use_constraints {
                    if use_constraints.restriction_code.value == "otherRestrictions" {
                        for other_constraints in &legal_constraints.other_constraints {
                            if let Some(text) = &other_constraints.text {
                                if let Ok(license) = from_json_str::<License>(text) {
                                    return Some(license.id);
                                }
                            }
                        }

                        break;
                    }
                }
            }
        }

        None
    }
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

#[derive(Debug, Deserialize)]
struct Abstract {
    #[serde(rename = "CharacterString")]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResourceConstraints {
    #[serde(rename = "MD_LegalConstraints")]
    legal_constraints: Option<LegalConstraints>,
}

#[derive(Debug, Deserialize)]
struct LegalConstraints {
    #[serde(rename = "useConstraints", default)]
    use_constraints: Vec<UseConstraints>,
    #[serde(rename = "otherConstraints", default)]
    other_constraints: Vec<OtherConstraints>,
}

#[derive(Debug, Deserialize)]
struct UseConstraints {
    #[serde(rename = "MD_RestrictionCode")]
    restriction_code: RestrictionCode,
}

#[derive(Debug, Deserialize)]
struct RestrictionCode {
    #[serde(rename = "codeListValue")]
    value: String,
}

#[derive(Debug, Deserialize)]
struct OtherConstraints {
    #[serde(rename = "CharacterString")]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct License<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
}
