mod contact;
mod license;
mod resource;

use std::io::Read;

use anyhow::{Context, Result};
use bincode::{
    config::standard,
    serde::{decode_from_slice, encode_to_vec},
};
use cap_std::fs::File;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use time::Date;
use tokio::{fs::File as AsyncFile, io::AsyncWriteExt};

pub use contact::Contact;
pub use license::License;
pub use resource::{Resource, Type as ResourceType};

#[derive(Debug, Deserialize, Serialize)]
pub struct Dataset {
    pub title: String,
    pub description: Option<String>,
    pub comment: Option<String>,
    pub license: License,
    pub contacts: Vec<Contact>,
    pub tags: Vec<String>,
    pub region: Option<String>,
    pub issued: Option<Date>,
    pub last_checked: Option<Date>,
    pub source_url: String,
    pub resources: SmallVec<[Resource; 4]>,
}

/// Previously deployed version of the above [`Dataset`] type.
///
/// It will be updated when a new harvester has been deployed. Feature branches should only modify [`Dataset`] and the mapping between both types defined by [`Dataset::read`].
#[derive(Debug, Deserialize, Serialize)]
struct OldDataset {
    pub title: String,
    pub description: String,
    pub license: License,
    pub tags: Vec<String>,
    pub source_url: String,
    pub resources: Vec<Resource>,
    pub issued: Option<Date>,
}

impl Dataset {
    pub fn read(file: File) -> Result<Self> {
        Self::read_with(file, &mut Vec::new())
    }

    pub fn read_with(mut file: File, buf: &mut Vec<u8>) -> Result<Self> {
        buf.clear();
        file.read_to_end(buf)?;

        let val = match decode_from_slice::<Dataset, _>(buf, standard()) {
            Ok((val, _)) => val,
            Err(err) => {
                let (old_val, _) = decode_from_slice::<OldDataset, _>(buf, standard())
                    .map_err(|_old_err| err)
                    .context("Failed to deserialize dataset")?;

                Self {
                    title: old_val.title,
                    description: Some(old_val.description),
                    comment: None,
                    license: old_val.license,
                    contacts: Vec::new(),
                    tags: old_val.tags,
                    region: None,
                    issued: old_val.issued,
                    last_checked: None,
                    source_url: old_val.source_url,
                    resources: old_val.resources.into(),
                }
            }
        };

        Ok(val)
    }

    pub async fn write(&self, file: File) -> Result<()> {
        let buf = encode_to_vec(self, standard())?;

        let mut file = AsyncFile::from_std(file.into_std());
        file.write_all(&buf).await?;

        Ok(())
    }
}
