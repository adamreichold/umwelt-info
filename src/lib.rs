pub mod dataset;
pub mod harvester;
pub mod index;

use std::env::var_os;
use std::fs::{remove_file, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub fn data_path_from_env() -> PathBuf {
    var_os("DATA_PATH")
        .expect("Environment variable DATA_PATH not set")
        .into()
}

pub struct DataPathLock(PathBuf);

impl Drop for DataPathLock {
    fn drop(&mut self) {
        let _ = remove_file(&self.0);
    }
}

pub fn lock_data_path(data_path: &Path) -> Result<DataPathLock> {
    let lock_path = data_path.join("lock");

    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
        .context("Data path is locked")?;

    Ok(DataPathLock(lock_path))
}
