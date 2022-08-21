use std::borrow::Cow;

use anyhow::Result;
use askama::Template;
use cap_std::fs::Dir;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde_json::from_str as from_json_str;
use serde_roxmltree::{from_doc as from_xml_doc, roxmltree::Document};
use smallvec::SmallVec;

use crate::{
    dataset::Dataset,
    harvester::{client::Client, fetch_many, write_dataset, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
    let max_records = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, source, max_records, 1).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + max_records - 1) / max_records;
    let start_pos = (1..requests).map(|request| 1 + request * max_records);

    let (results, errors) = fetch_many(source, results, errors, start_pos, |start_pos| {
        fetch_datasets(dir, client, source, max_records, start_pos)
    })
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

    let document = Document::parse(&body)?;

    let response = from_xml_doc::<GetRecordsResponse>(&document)?;

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

pub async fn translate_dataset(dir: &Dir, source: &Source, record: Record<'_>) -> Result<()> {
    let identifier = record.file_identifier.text;

    let identification = record.identification_info.identification();

    let license = identification.license().as_deref().into();

    let title = identification.citation.inner.title.text;
    let description = identification.r#abstract.text.unwrap_or_default();

    let dataset = Dataset {
        title,
        description,
        license,
        tags: Vec::new(),
        source_url: source.source_url().replace("{{id}}", identifier),
        resources: SmallVec::new(),
        issued: None,
    };

    write_dataset(dir, identifier, dataset).await
}

#[derive(Template)]
#[template(path = "csw_get_records.xml")]
struct GetRecordsRequest {
    max_records: usize,
    start_pos: usize,
}

#[derive(Debug, Deserialize)]
struct GetRecordsResponse<'a> {
    #[serde(rename = "SearchResults", borrow)]
    results: SearchResults<'a>,
}

#[derive(Debug, Deserialize)]
struct SearchResults<'a> {
    #[serde(rename = "numberOfRecordsMatched")]
    num_records_matched: usize,
    #[serde(rename = "MD_Metadata", borrow)]
    records: Vec<Record<'a>>,
}

#[derive(Debug, Deserialize)]
pub struct Record<'a> {
    #[serde(rename = "fileIdentifier", borrow)]
    file_identifier: FileIdentifier<'a>,
    #[serde(rename = "identificationInfo", borrow)]
    identification_info: IdentificationInfo<'a>,
}

#[derive(Debug, Deserialize)]
struct FileIdentifier<'a> {
    #[serde(rename = "CharacterString")]
    text: &'a str,
}

#[derive(Debug, Deserialize)]
enum IdentificationInfo<'a> {
    #[serde(rename = "MD_DataIdentification", borrow)]
    Data(Identification<'a>),
    #[serde(rename = "SV_ServiceIdentification", borrow)]
    Service(Identification<'a>),
}

impl<'a> IdentificationInfo<'a> {
    fn identification(self) -> Identification<'a> {
        match self {
            Self::Data(identification) => identification,
            Self::Service(identification) => identification,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Identification<'a> {
    citation: Citation,
    r#abstract: Abstract,
    #[serde(rename = "resourceConstraints", default, borrow)]
    resource_constraints: Vec<ResourceConstraints<'a>>,
}

impl Identification<'_> {
    /// Extract the license ID for Open Data licenses
    ///
    /// Based on section 3.6 from [Konventionen zu Metadaten][https://www.gdi-de.org/download/AK_Metadaten_Konventionen_zu_Metadaten.pdf].
    fn license(&self) -> Option<Cow<str>> {
        for resource_constraints in &self.resource_constraints {
            if let Some(legal_constraints) = &resource_constraints.legal_constraints {
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
struct ResourceConstraints<'a> {
    #[serde(rename = "MD_LegalConstraints", borrow)]
    legal_constraints: Option<LegalConstraints<'a>>,
}

#[derive(Debug, Deserialize)]
struct LegalConstraints<'a> {
    #[serde(rename = "useConstraints", default, borrow)]
    use_constraints: Vec<UseConstraints<'a>>,
    #[serde(rename = "otherConstraints", default, borrow)]
    other_constraints: Vec<OtherConstraints<'a>>,
}

#[derive(Debug, Deserialize)]
struct UseConstraints<'a> {
    #[serde(rename = "MD_RestrictionCode", borrow)]
    restriction_code: RestrictionCode<'a>,
}

#[derive(Debug, Deserialize)]
struct RestrictionCode<'a> {
    #[serde(rename = "codeListValue", borrow)]
    value: &'a str,
}

#[derive(Debug, Deserialize)]
struct OtherConstraints<'a> {
    #[serde(rename = "CharacterString", borrow)]
    text: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct License<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
}
