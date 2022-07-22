use anyhow::Result;
use cap_std::{ambient_authority, fs::Dir};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use umwelt_info::{data_path_from_env, dataset::Dataset, index::Indexer, server::Stats};

fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_path = data_path_from_env();

    let indexer = Indexer::start(&data_path)?;

    let dir = Dir::open_ambient_dir(data_path, ambient_authority())?;

    let stats = Stats::read(&dir)?;

    for source in dir.read_dir("datasets")? {
        let source = source?;
        let source_id = source.file_name().into_string().unwrap();

        let accesses = stats.accesses.get(&source_id);

        for dataset in source.open_dir()?.entries()? {
            let dataset = dataset?;
            let dataset_id = dataset.file_name().into_string().unwrap();

            let dataset = Dataset::read(dataset.open()?)?;

            let accesses = accesses.and_then(|accesses| accesses.get(&dataset_id));

            indexer.add_document(
                source_id.clone(),
                dataset_id,
                dataset,
                *accesses.unwrap_or(&0),
            )?;
        }
    }

    indexer.commit()?;

    Ok(())
}
