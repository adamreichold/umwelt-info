use std::cmp::Reverse;

use askama::Template;
use axum::{extract::Extension, response::Html};
use cap_std::fs::Dir;
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
            .iter()
            .map(|(source_name, accesses)| (source_name, accesses.values().sum()))
            .collect::<Vec<_>>();

        accesses.sort_unstable_by_key(|(_, accesses)| Reverse(*accesses));

        let sum_accesses = accesses.iter().map(|(_, accesses)| accesses).sum();

        let metrics = Metrics::read(dir)?;

        let mut harvests = metrics.harvests.iter().collect::<Vec<_>>();

        harvests.sort_unstable_by_key(|(_, harvest)| Reverse(harvest.start));

        let (sum_count, sum_transmitted, sum_failed) = metrics.harvests.values().fold(
            (0, 0, 0),
            |(sum_count, sum_transmitted, sum_failed), harvest| {
                (
                    sum_count + harvest.count,
                    sum_transmitted + harvest.transmitted,
                    sum_failed + harvest.failed,
                )
            },
        );

        let mut licenses = metrics
            .licenses
            .iter()
            .map(|(license, count)| (license.to_string(), *count))
            .collect::<Vec<_>>();

        licenses.sort_unstable_by_key(|(_, count)| Reverse(*count));

        let sum_other = metrics
            .licenses
            .iter()
            .filter(|(license, _)| matches!(license, License::Other(_)))
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
        };

        let page = Html(page.render().unwrap());

        Ok(page)
    }

    spawn_blocking(|| inner(dir)).await?
}

#[derive(Template)]
#[template(path = "metrics.html")]
struct MetricsPage<'a> {
    accesses: Vec<(&'a String, u64)>,
    sum_accesses: u64,
    harvests: Vec<(&'a String, &'a HarvestMetrics)>,
    sum_count: usize,
    sum_transmitted: usize,
    sum_failed: usize,
    licenses: Vec<(String, usize)>,
    sum_other: usize,
}
