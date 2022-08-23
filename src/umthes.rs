use std::borrow::Cow;

use anyhow::Result;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;

pub async fn fetch_similar_terms(client: &Client, terms: &[String]) -> Result<Vec<String>> {
    if terms.is_empty() {
        return Ok(Vec::new());
    }

    let response = client
        .get("https://sns.uba.de/umthes/de/similar.json")
        .query(&[("terms", terms.join(","))])
        .send()
        .await?
        .error_for_status()?
        .json::<SimilarTerms>()
        .await?;

    let results = response
        .results
        .into_iter()
        .map(strip_disambiguation)
        .collect();

    Ok(results)
}

#[derive(Deserialize)]
struct SimilarTerms {
    results: Vec<String>,
}

fn strip_disambiguation(term: String) -> String {
    static DISAMBIGUATION: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\s+\[.+\]"#).unwrap());

    match DISAMBIGUATION.replace_all(&term, "") {
        Cow::Borrowed(_term) => term,
        Cow::Owned(term) => term,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_results_but_remove_disambiguations() {
        assert_eq!(strip_disambiguation("Aller [Fluss]".to_owned()), "Aller");

        assert_eq!(
            strip_disambiguation("Binnenschifffahrt".to_owned()),
            "Binnenschifffahrt"
        );
    }
}
