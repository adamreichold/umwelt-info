use anyhow::{anyhow, Result};
use cap_std::fs::Dir;
use serde::Deserialize;
use tokio::fs::read_to_string;
use toml::from_str;

use crate::{
    dataset::Dataset,
    harvester::{write_dataset, Source},
};

pub async fn harvest(dir: &Dir, source: &Source) -> Result<(usize, usize, usize)> {
    let path = source
        .url
        .to_file_path()
        .map_err(|()| anyhow!("{} is not a valid path", source.url))?;

    let contents = from_str::<FileContents>(&read_to_string(path).await?)?;

    let count = contents.datasets.len();
    let mut errors = 0;

    tracing::info!("Harvesting {} datasets", count);

    for dataset in contents.datasets {
        if let Err(err) = write_dataset(dir, &dataset.id, dataset.dataset).await {
            tracing::error!("{:#}", err);

            errors += 1;
        }
    }

    Ok((count, count, errors))
}

#[derive(Deserialize)]
struct FileContents {
    datasets: Vec<ManualDataset>,
}

#[derive(Deserialize)]
struct ManualDataset {
    id: String,
    #[serde(flatten)]
    dataset: Dataset,
}
