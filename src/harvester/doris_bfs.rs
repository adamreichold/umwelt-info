use anyhow::{anyhow, ensure, Result};
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
use scraper::{Html, Selector};
use serde::Serialize;

use crate::{
    dataset::{Dataset, License},
    harvester::{client::Client, write_dataset, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<(usize, usize, usize)> {
    let rpp = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, source, rpp, 0).await?;
    tracing::info!("Harvesting {} datasets", count);

    let requests = (count + rpp - 1) / rpp;
    let offset = (1..requests).map(|request| request * rpp);

    let (results, errors) = iter(offset)
        .map(|offset| fetch_datasets(dir, client, source, rpp, offset))
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
    rpp: usize,
    offset: usize,
) -> Result<(usize, usize, usize)> {
    tracing::debug!("Fetching {} datasets starting at {}", rpp, offset);

    let url = source.url.join("/jspui/browse")?;

    #[derive(Serialize)]
    struct Params {
        rpp: usize,
        offset: usize,
    }

    let body = client
        .make_request(
            &format!("{}-browse-{}", source.name, offset),
            |client| async {
                client
                    .get(url.clone())
                    .query(&Params { rpp, offset })
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await
            },
        )
        .await?;

    let count;
    let handles;

    {
        let document = Html::parse_document(&body);

        count = parse_count(&document)?;
        handles = parse_handles(&document)?;
    }

    ensure!(
        !handles.is_empty(),
        "Could not parse handles at offset {}",
        offset
    );

    let results = handles.len();
    let mut errors = 0;

    for handle in &handles {
        if let Err(err) = fetch_dataset(dir, client, source, handle).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, results, errors))
}

async fn fetch_dataset(dir: &Dir, client: &Client, source: &Source, handle: &str) -> Result<()> {
    tracing::debug!("Fetching dataset at {}", handle);

    let url = source.url.join(handle)?;

    let body = client
        .make_request(
            &format!(
                "{}-handle-{}",
                source.name,
                handle.rsplit('/').next().unwrap()
            ),
            |client| async {
                client
                    .get(url.clone())
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await
            },
        )
        .await?;

    let identifier;
    let title;
    let r#abstract;

    {
        let document = Html::parse_document(&body);

        identifier = document
            .select(&SELECTORS.identifier_selector)
            .filter_map(|element| element.value().attr("content"))
            .find(|identifier| identifier.starts_with("urn:"))
            .ok_or_else(|| anyhow!("Missing identifier"))?
            .to_owned();

        title = document
            .select(&SELECTORS.title_selector)
            .next()
            .and_then(|element| element.value().attr("content"))
            .ok_or_else(|| anyhow!("Missing title"))?
            .to_owned();

        r#abstract = document
            .select(&SELECTORS.abstract_selector)
            .next()
            .and_then(|element| element.value().attr("content"))
            .unwrap_or_default()
            .to_owned();
    }

    let dataset = Dataset {
        title,
        description: r#abstract,
        license: License::DorisBfs,
        tags: Vec::new(),
        source_url: url.into(),
    };

    write_dataset(dir, &identifier, dataset).await
}

fn parse_count(document: &Html) -> Result<usize> {
    let element = document
        .select(&SELECTORS.range_selector)
        .next()
        .ok_or_else(|| anyhow!("Missing number of documents"))?;

    let text = element.text().collect::<String>();

    let captures = SELECTORS
        .range_regex
        .captures(&text)
        .ok_or_else(|| anyhow!("Could not parse number of documents"))?;

    let count = captures[3].parse::<usize>()?;

    Ok(count)
}

fn parse_handles(document: &Html) -> Result<Vec<String>> {
    let mut handles = Vec::new();

    for element in document.select(&SELECTORS.handle_selector) {
        handles.push(
            element
                .value()
                .attr("href")
                .ok_or_else(|| anyhow!("Missing handle reference"))?
                .to_owned(),
        );
    }

    Ok(handles)
}

static SELECTORS: Lazy<Selectors> = Lazy::new(Selectors::default);

struct Selectors {
    range_selector: Selector,
    range_regex: Regex,
    handle_selector: Selector,
    identifier_selector: Selector,
    title_selector: Selector,
    abstract_selector: Selector,
}

impl Default for Selectors {
    fn default() -> Self {
        Self {
            range_selector: Selector::parse("div.browse_range").unwrap(),
            range_regex: Regex::new(r#"Anzeige der Treffer (\d+) bis (\d+) von (\d+)"#).unwrap(),
            handle_selector: Selector::parse("td[headers=t2] > a").unwrap(),
            identifier_selector: Selector::parse(r#"head > meta[name="DC.identifier"]"#).unwrap(),
            title_selector: Selector::parse(r#"head > meta[name="DC.title"]"#).unwrap(),
            abstract_selector: Selector::parse(r#"head > meta[name="DCTERMS.abstract"]"#).unwrap(),
        }
    }
}
