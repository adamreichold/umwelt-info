use std::cmp::Reverse;

use askama::Template;
use axum::{extract::Extension, response::Html};
use cap_std::fs::Dir;
use hashbrown::HashMap;
use tokio::task::spawn_blocking;

use crate::{
    dataset::License,
    metrics::{Harvest as HarvestMetrics, Metrics},
    server::{filters, stats::Stats, ServerError},
};

pub async fn metrics(Extension(dir): Extension<&'static Dir>) -> Result<Html<String>, ServerError> {
    fn inner(dir: &Dir) -> Result<Html<String>, ServerError> {
        let stats = Stats::read(dir)?;

        let mut accesses = stats
            .accesses
            .into_iter()
            .map(|(source_name, accesses)| (source_name, accesses.values().sum()))
            .collect::<Vec<_>>();

        accesses.sort_unstable_by_key(|(_, accesses)| Reverse(*accesses));

        let sum_accesses = accesses.iter().map(|(_, accesses)| accesses).sum();

        let metrics = Metrics::read(dir)?;

        let mut harvests = metrics.harvests.into_iter().collect::<Vec<_>>();

        harvests.sort_unstable_by_key(|(_, harvest)| Reverse(harvest.failed));

        let (sum_count, sum_transmitted, sum_failed) = harvests.iter().fold(
            (0, 0, 0),
            |(sum_count, sum_transmitted, sum_failed), (_, harvest)| {
                (
                    sum_count + harvest.count,
                    sum_transmitted + harvest.transmitted,
                    sum_failed + harvest.failed,
                )
            },
        );

        let mut licenses_by_source = metrics
            .licenses
            .iter()
            .map(|(source, licenses)| {
                let (count, unknown, other) = licenses.iter().fold(
                    (0, 0, 0),
                    |(mut count, mut unknown, mut other), (license, count1)| {
                        count += count1;

                        match license {
                            License::Unknown => unknown += count1,
                            License::Other(_) => other += count1,
                            _ => (),
                        }

                        (count, unknown, other)
                    },
                );

                (
                    source.clone(),
                    unknown as f64 / count as f64,
                    other as f64 / (count - unknown) as f64,
                )
            })
            .collect::<Vec<_>>();

        licenses_by_source.sort_unstable_by(
            |(_, lhs_unknown, lhs_other), (_, rhs_unknown, rhs_other)| {
                (rhs_unknown, rhs_other)
                    .partial_cmp(&(lhs_unknown, lhs_other))
                    .unwrap()
            },
        );

        let mut licenses = metrics
            .licenses
            .into_iter()
            .fold(HashMap::new(), |mut licenses, (_, licenses1)| {
                for (license, count) in licenses1 {
                    *licenses.entry(license).or_default() += count;
                }

                licenses
            })
            .into_iter()
            .collect::<Vec<_>>();

        licenses.sort_unstable_by_key(|(_, count)| Reverse(*count));

        let sum_other = licenses
            .iter()
            .filter(|(license, _)| license.is_other())
            .map(|(_, count)| *count)
            .sum();

        let page = MetricsPage {
            accesses,
            sum_accesses,
            harvests,
            sum_count,
            sum_transmitted,
            sum_failed,
            licenses,
            sum_other,
            licenses_by_source,
        };

        let page = Html(page.render().unwrap());

        Ok(page)
    }

    spawn_blocking(|| inner(dir)).await?
}

#[derive(Template)]
#[template(path = "metrics.html")]
struct MetricsPage {
    accesses: Vec<(String, u64)>,
    sum_accesses: u64,
    harvests: Vec<(String, HarvestMetrics)>,
    sum_count: usize,
    sum_transmitted: usize,
    sum_failed: usize,
    licenses: Vec<(License, usize)>,
    sum_other: usize,
    licenses_by_source: Vec<(String, f64, f64)>,
}
