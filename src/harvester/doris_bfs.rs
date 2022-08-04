use anyhow::{anyhow, ensure, Result};
use cap_std::fs::Dir;
use futures_util::stream::{iter, StreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};

use crate::{
    dataset::Dataset,
    harvester::{with_retry, Source},
};

pub async fn harvest(dir: &Dir, client: &Client, source: &Source) -> Result<()> {
    let rpp = source.batch_size;

    let (count, results, errors) = fetch_datasets(dir, client, source, rpp, 0).await?;

    let requests = (count + rpp - 1) / rpp;
    let offset = (1..requests).map(|request| request * rpp);

    let (_results, _errors) = iter(offset)
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

    Ok(())
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

    let response = with_retry(|| async {
        let response = client
            .get(url.clone())
            .query(&[
                ("type", "title"),
                ("rpp", &rpp.to_string()),
                ("offset", &offset.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        Ok(response)
    })
    .await?;

    let count;
    let handles;

    {
        let document = Html::parse_document(&response);

        count = parse_count(&document)?;
        handles = parse_handles(&document)?;
    }

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

    let response = with_retry(|| async {
        let response = client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        Ok(response)
    })
    .await?;

    let mut urn = String::new();
    let mut title = String::new();
    let mut description = String::new();

    {
        let document = Html::parse_document(&response);

        for element in document.select(&SELECTORS.row_selector) {
            let label_element = match element.select(&SELECTORS.label_selector).next() {
                Some(label_element) => label_element,
                None => continue,
            };

            let label = label_element.text().collect::<String>();

            let value_element = match element.select(&SELECTORS.value_selector).next() {
                Some(value_element) => value_element,
                None => continue,
            };

            let value = value_element.text().collect::<String>();

            match label.trim() {
                "URN(s):" => urn = value.trim().to_owned(),
                "Titel:" => title = value.trim().to_owned(),
                "Zusammenfassung:" => description = value.trim().to_owned(),
                _ => (),
            }
        }
    }

    ensure!(!urn.is_empty(), "Could not parse URN at handle {}", handle);

    ensure!(
        !title.is_empty(),
        "Could not parse title at handle {}",
        handle
    );

    let dataset = Dataset {
        title,
        description,
        source_url: url.as_str().to_owned(),
    };

    let file = dir.create(urn)?;

    dataset.write(file).await?;

    Ok(())
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
    row_selector: Selector,
    label_selector: Selector,
    value_selector: Selector,
}

impl Default for Selectors {
    fn default() -> Self {
        Self {
            range_selector: Selector::parse("div.browse_range").unwrap(),
            range_regex: Regex::new(r#"Anzeige der Treffer (\d+) bis (\d+) von (\d+)"#).unwrap(),
            handle_selector: Selector::parse("strong > a").unwrap(),
            row_selector: Selector::parse("table.itemDisplayTable > tbody > tr").unwrap(),
            label_selector: Selector::parse("td.metadataFieldLabel").unwrap(),
            value_selector: Selector::parse("td.metadataFieldValue > span").unwrap(),
        }
    }
}
