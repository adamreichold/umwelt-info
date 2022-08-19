pub mod dataset;
pub mod harvester;
pub mod index;
pub mod metrics;
pub mod server;
pub mod umthes;

use std::env::var_os;
use std::path::PathBuf;

pub fn data_path_from_env() -> PathBuf {
    var_os("DATA_PATH")
        .expect("Environment variable DATA_PATH not set")
        .into()
}
