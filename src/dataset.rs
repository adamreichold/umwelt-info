use std::io::Read;

use anyhow::Result;
use bincode::{deserialize, serialize};
use cap_std::fs::File;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use serde::{Deserialize, Serialize};
use tokio::{fs::File as AsyncFile, io::AsyncWriteExt};
use url::Url;

#[derive(Deserialize, Serialize)]
pub struct Dataset {
    pub title: String,
    pub description: String,
    pub source_url: Url,
}

impl Dataset {
    pub fn read(mut file: File) -> Result<Self> {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let val = deserialize(&decompress_size_prepended(&buf)?)?;

        Ok(val)
    }

    pub async fn write(&self, file: File) -> Result<()> {
        let buf = compress_prepend_size(&serialize(self)?);

        let mut file = AsyncFile::from_std(file.into_std());
        file.write_all(&buf).await?;

        Ok(())
    }
}
