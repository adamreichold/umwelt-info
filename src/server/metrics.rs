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
            .into_iter()
            .map(|(source_name, accesses)| (source_name, accesses.values().sum()))
            .collect::<Vec<_>>();

        accesses.sort_unstable_by_key(|(_, accesses)| Reverse(*accesses));

        let sum_accesses = accesses.iter().map(|(_, accesses)| accesses).sum();

        let metrics = Metrics::read(dir)?;

        let mut harvests = metrics.harvests.into_iter().collect::<Vec<_>>();

        harvests.sort_unstable_by_key(|(_, harvest)| Reverse(harvest.start));

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

        let mut licenses = metrics.licenses.into_iter().collect::<Vec<_>>();

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
}
