//! This harvester maps the "Recherche" function available at Wasser-DE into our catalogue.
//!
//! | Original field            | Mapped field       | Comment                                                      |
//! | ------------------------- | ------------------ | ------------------------------------------------------------ |
//! | ID                        | id                 | Assumed to be numeric and redundant                          |
//! | metadataid                |                    |                                                              |
//! | NAME                      | title              | Document skipped if missing                                  |
//! | TEASERTEXT                | description        | TEASERTEXT preferred over AUTOTEASERTEXT if both are present |
//! | AUTOTEASERTEXT            |                    |                                                              |
//! | LICENSE_ID                |                    |                                                              |
//! | LICENSE_NAME_KURZ         | license            | LICENSE_ID and LICENSE_NAME_LANG considered redundant        |
//! | LICENSE_NAME_LANG         |                    |                                                              |
//! | RICHTLINIE_IDS            | tags               |                                                              |
//! | URL                       | resource           |                                                              |
//! | CONTENTTYPE               | resource.type      | Mime-type of the linked ressource, reduced to category       |
//! | JAHR_VEROEFFENTLICHUNG    | issued             | http://purl.org/dc/terms/issued                              |
//! | KOMMENTAR                 | comment            |                                                              |
//! | LAST_CHECKED              | last_checked       | Last time the Wasser-DE staff checked this document          |
//! | REGION_NAME               | region             | Geographic region. Can be a city, country, state or river    |
//! | REGION_ID                 |                    |                                                              |
//! | ANSPRECHPARTNER_NAME      | contact_names      |                                                              |
//! | ANSPRECHPARTNER_EMAIL     | contact_emails     |                                                              |
//! | AP_NAME                   | contact_names      |                                                              |
//! | AP_EMAIL                  | contact_emails     |                                                              |
//! | ANSPRECHPARTNER_NAME_RL1  | contact_names      |                                                              |
//! | ANSPRECHPARTNER_EMAIL_RL1 | contact_emails     |                                                              |
//! | ANSPRECHPARTNER_NAME_RL2  | contact_names      |                                                              |
//! | ANSPRECHPARTNER_EMAIL_RL2 | contact_emails     |                                                              |
//! | ANSPRECHPARTNER_NAME_RL3  | contact_names      |                                                              |
//! | ANSPRECHPARTNER_EMAIL_RL3 | contact_emails     |                                                              |
//! | ANSPRECHPARTNER_NAME_RL4  | contact_names      |                                                              |
//! | ANSPRECHPARTNER_EMAIL_RL4 | contact_emails     |                                                              |
//!  
use anyhow::{anyhow, Result};
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use serde_json::from_slice;
use smallvec::smallvec;
use time::{macros::format_description, Date};

use crate::{
    dataset::{Contact, Dataset, Resource, Tag},
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

    let description = document.teaser_text.or(document.auto_teaser_text);

    let issued = document
        .year_issued
        .map(|year_issued| Date::from_ordinal_date(year_issued, 1))
        .transpose()?;

    let last_checked = document
        .last_checked
        .map(|last_checked| Date::parse(&last_checked, format_description!("[year]-[month]-[day]")))
        .transpose()?;

    let mut contacts = Vec::new();

    let mut push_contact = |name: Option<String>, email: Option<String>| {
        contacts.extend(name.map(|name| Contact {
            name,
            emails: email.into_iter().collect(),
        }));
    };

    push_contact(document.contact_name, document.contact_email);
    push_contact(document.contact_name_ap, document.contact_email_ap);
    push_contact(document.contact_name_rl1, document.contact_email_rl1);
    push_contact(document.contact_name_rl2, document.contact_email_rl2);
    push_contact(document.contact_name_rl3, document.contact_email_rl3);
    push_contact(document.contact_name_rl4, document.contact_email_rl4);

    let dataset = Dataset {
        title,
        description,
        comment: document.comment,
        provenance: source.provenance.clone(),
        license: document.license.as_str().into(),
        contacts,
        tags,
        region: document.region_name,
        issued,
        last_checked,
        source_url: source.url.clone().into(),
        resources: smallvec![Resource {
            r#type: document.content_type.as_str().into(),
            url: document.url
        }],
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
    #[serde(rename = "KOMMENTAR")]
    comment: Option<String>,
    #[serde(rename = "LAST_CHECKED")]
    last_checked: Option<String>,
    #[serde(rename = "REGION_NAME")]
    region_name: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_NAME")]
    contact_name: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_EMAIL")]
    contact_email: Option<String>,
    #[serde(rename = "AP_NAME")]
    contact_name_ap: Option<String>,
    #[serde(rename = "AP_EMAIL")]
    contact_email_ap: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_NAME_RL1")]
    contact_name_rl1: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_EMAIL_RL1")]
    contact_email_rl1: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_NAME_RL2")]
    contact_name_rl2: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_EMAIL_RL2")]
    contact_email_rl2: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_NAME_RL3")]
    contact_name_rl3: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_EMAIL_RL3")]
    contact_email_rl3: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_NAME_RL4")]
    contact_name_rl4: Option<String>,
    #[serde(rename = "ANSPRECHPARTNER_EMAIL_RL4")]
    contact_email_rl4: Option<String>,
    #[serde(rename = "CONTENTTYPE")]
    content_type: String,
}

impl Document {
    fn tags(&self) -> Vec<Tag> {
        let mut tags = Vec::new();

        if let Some(directive) = &self.directive {
            if directive.contains("1#") {
                tags.push(Tag::Wrrl);
            }
            if directive.contains("2#") {
                tags.push(Tag::HwrmRl);
            }
            if directive.contains("3#") {
                tags.push(Tag::MsrRl);
            }
            if directive.contains("4#") {
                tags.push(Tag::BgRl);
            }
        }

        tags
    }
}
