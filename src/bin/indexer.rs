use anyhow::Result;
use cap_std::{ambient_authority, fs::Dir};
use parking_lot::Mutex;
use rayon::iter::{ParallelBridge, ParallelIterator};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use umwelt_info::{
    data_path_from_env, dataset::Dataset, index::Indexer, metrics::Metrics, server::stats::Stats,
};

fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_path = data_path_from_env();

    let indexer = Indexer::start(&data_path)?;

    let dir = Dir::open_ambient_dir(data_path, ambient_authority())?;

    let stats = Stats::read(&dir)?;

    let mut metrics = Mutex::new(Metrics::read(&dir)?);

    metrics.get_mut().reset_datasets();

    dir.read_dir("datasets")?
        .par_bridge()
        .try_for_each(|source| -> Result<()> {
            let source = source?;
            let source_id = source.file_name().into_string().unwrap();

            let accesses = stats.accesses.get(&source_id);

            source
                .open_dir()?
                .entries()?
                .par_bridge()
                .try_for_each(|dataset| -> Result<()> {
                    let dataset = dataset?;
                    let dataset_id = dataset.file_name().into_string().unwrap();

                    let dataset = Dataset::read(dataset.open()?)?;

                    let accesses = accesses.and_then(|accesses| accesses.get(&dataset_id));

                    metrics.lock().record_dataset(&dataset);

                    indexer.add_document(
                        source_id.clone(),
                        dataset_id,
                        dataset,
                        *accesses.unwrap_or(&0),
                    )?;

                    Ok(())
                })
        })?;

    indexer.commit()?;

    metrics.get_mut().write(&dir)?;

    Ok(())
}
