//! This harvester maps the "Recherche" function available at Wasser-DE into our catalogue.
//!
//! | Original field         | Mapped field       | Comment                                                      |
//! | -----------------------| -------------------| ------------------------------------------------------------ |
//! | ID                     | id                 | Assumed to be numeric and redundant                          |
//! | metadataid             |                    |                                                              |
//! | NAME                   | title              | Document skipped if missing                                  |
//! | TEASERTEXT             | description        | TEASERTEXT preferred over AUTOTEASERTEXT if both are present |
//! | AUTOTEASERTEXT         |                    |                                                              |
//! | LICENSE_ID             |                    |                                                              |
//! | LICENSE_NAME_KURZ      | license            | LICENSE_ID and LICENSE_NAME_LANG considered redundant        |
//! | LICENSE_NAME_LANG      |                    |                                                              |
//! | RICHTLINIE_IDS         | tags               |                                                              |
//! | URL                    | resource           |                                                              |
//! | JAHR_VEROEFFENTLICHUNG | issued             | http://purl.org/dc/terms/issued                              |
//!
use anyhow::{anyhow, Result};
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use serde_json::from_slice;
use smallvec::smallvec;
use time::Date;

use crate::{
    dataset::{Dataset, Resource},
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
    let tags = document.tags();

    let title = document
        .name
        .ok_or_else(|| anyhow!("Document {} has no title", document.id))?;

    let description = document
        .teaser_text
        .or(document.auto_teaser_text)
        .unwrap_or_default();

    let issued = document
        .year_issued
        .map(|year_issued| Date::from_ordinal_date(year_issued, 1))
        .transpose()?;

    let dataset = Dataset {
        title,
        description,
        license: document.license.as_str().into(),
        tags,
        source_url: source.url.clone().into(),
        resources: smallvec![Resource::unknown(document.url)],
        issued,
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
    #[serde(rename = "RICHTLINIE_IDS")]
    directive: Option<String>,
    #[serde(rename = "URL")]
    url: String,
    #[serde(rename = "JAHR_VEROEFFENTLICHUNG")]
    year_issued: Option<i32>,
}

impl Document {
    fn tags(&self) -> Vec<String> {
        let mut tags = Vec::new();

        if let Some(directive) = &self.directive {
            if directive.contains("1#") {
                tags.push("WRRL".to_owned());
                tags.push("Wasserrahmenrichtlinie".to_owned());
            }
            if directive.contains("2#") {
                tags.push("HWRM-RL".to_owned());
                tags.push("Hochwasserrisikomanagement-Richtlinie".to_owned());
            }
            if directive.contains("3#") {
                tags.push("MSR-RL".to_owned());
                tags.push("Meeresstrategie-Rahmenrichtlinie".to_owned());
            }
            if directive.contains("4#") {
                tags.push("BG-RL".to_owned());
                tags.push("Badegewässer-Richtlinie".to_owned());
            }
        }

        tags
    }
}
