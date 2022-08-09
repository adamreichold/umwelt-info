mod license;

use std::io::Read;

use anyhow::Result;
use bincode::{deserialize, serialize};
use cap_std::fs::File;
use serde::{Deserialize, Serialize};
use tokio::{fs::File as AsyncFile, io::AsyncWriteExt};

pub use license::License;

#[derive(Deserialize, Serialize)]
pub struct Dataset {
    pub title: String,
    pub description: String,
    pub license: License,
    pub source_url: String,
}

#[derive(Deserialize, Serialize)]
struct OldDataset {
    pub title: String,
    pub description: String,
    pub source_url: String,
}

impl Dataset {
    pub fn read(mut file: File) -> Result<Self> {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let val = match deserialize::<Dataset>(&buf) {
            Ok(val) => val,
            Err(err) => {
                let old_val = deserialize::<OldDataset>(&buf).map_err(|_old_err| err)?;

                Dataset {
                    title: old_val.title,
                    description: old_val.description,
                    license: License::Unknown,
                    source_url: old_val.source_url,
                }
            }
        };

        Ok(val)
    }

    pub async fn write(&self, file: File) -> Result<()> {
        let mut file = AsyncFile::from_std(file.into_std());

        file.write_all(&serialize(self)?).await?;

        Ok(())
    }
}
