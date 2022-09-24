use askama::Template;
use axum::{
    extract::{Path, State},
    response::Response,
};
use cap_std::fs::Dir;
use parking_lot::Mutex;
use serde::Serialize;

use crate::{
    dataset::Dataset,
    server::{stats::Stats, Accept, ServerError},
};

pub async fn dataset(
    Path((source, id)): Path<(String, String)>,
    accept: Accept,
    State(dir): State<&'static Dir>,
    State(stats): State<&'static Mutex<Stats>>,
) -> Result<Response, ServerError> {
    fn inner(
        source: String,
        id: String,
        dir: &Dir,
        stats: &Mutex<Stats>,
    ) -> Result<DatasetPage, ServerError> {
        let dir = dir.open_dir("datasets")?;

        let dataset = Dataset::read(dir.open_dir(&source)?.open(&id)?)?;

        let accesses = stats.lock().record_access(&source, &id);

        let page = DatasetPage {
            source,
            id,
            dataset,
            accesses,
        };

        Ok(page)
    }

    let page = inner(source, id, dir, stats)?;

    Ok(accept.into_repsonse(page))
}

#[derive(Template, Serialize)]
#[template(path = "dataset.html")]
struct DatasetPage {
    source: String,
    id: String,
    dataset: Dataset,
    accesses: u64,
}
