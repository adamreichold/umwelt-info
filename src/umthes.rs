use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use hashbrown::HashSet;
use moka::future::Cache;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;

pub type SimilarTermsCache = Cache<String, Arc<[String]>>;

pub fn similar_terms_cache() -> SimilarTermsCache {
    Cache::builder()
        .time_to_live(Duration::from_secs(24 * 60 * 60))
        .build()
}

pub async fn fetch_similar_terms<'a, T>(
    client: &Client,
    cache: &SimilarTermsCache,
    terms: T,
) -> HashSet<String>
where
    T: Iterator<Item = &'a String>,
{
    let mut results = HashSet::new();

    for term in terms {
        let results1 = cache
            .try_get_with::<_, Error>(term.clone(), async {
                tracing::debug!("Fetching terms similar to {} from SNS", term);

                let response = client
                    .get("https://sns.uba.de/umthes/de/similar.json")
                    .query(&[("terms", term)])
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
            })
            .await;

        match results1 {
            Ok(results1) => {
                for term in results1.iter() {
                    results.get_or_insert_owned(term);
                }
            }
            Err(err) => tracing::warn!("Failed to fetch similar terms: {:#}", err),
        }
    }

    results
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
