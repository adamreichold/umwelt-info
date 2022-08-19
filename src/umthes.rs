use anyhow::Result;
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

    Ok(response.results)
}

#[derive(Deserialize)]
struct SimilarTerms {
    results: Vec<String>,
}
