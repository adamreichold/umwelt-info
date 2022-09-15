use askama::Template;
use axum::{
    extract::{Extension, Path},
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
    Extension(dir): Extension<&'static Dir>,
    Extension(stats): Extension<&'static Mutex<Stats>>,
) -> Result<Response, ServerError> {
    let dir = dir.open_dir("datasets")?;

    let dataset = Dataset::read(dir.open_dir(&source)?.open(&id)?).await?;

    let accesses = stats.lock().record_access(&source, &id);

    let page = DatasetPage {
        source,
        id,
        dataset,
        accesses,
    };

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
