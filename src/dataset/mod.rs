mod contact;
mod license;
mod resource;
mod tag;

use std::io::Read;

use anyhow::{Context, Result};
use bincode::{deserialize, serialize};
use cap_std::fs::File;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use time::Date;
use tokio::{fs::File as AsyncFile, io::AsyncWriteExt};

pub use contact::Contact;
pub use license::License;
pub use resource::{Resource, Type as ResourceType};
pub use tag::Tag;

#[derive(Debug, Deserialize, Serialize)]
pub struct Dataset {
    pub title: String,
    pub description: Option<String>,
    pub comment: Option<String>,
    pub license: License,
    pub contacts: Vec<Contact>,
    pub tags: Vec<Tag>,
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
    pub fn read(mut file: File) -> Result<Self> {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let val = match deserialize::<Dataset>(&buf) {
            Ok(val) => val,
            Err(err) => {
                let old_val = deserialize::<OldDataset>(&buf)
                    .map_err(|_old_err| err)
                    .context("Failed to deserialize dataset")?;

                Self {
                    title: old_val.title,
                    description: Some(old_val.description),
                    comment: None,
                    license: old_val.license,
                    contacts: Vec::new(),
                    tags: old_val.tags.into_iter().map(Into::into).collect(),
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
        let buf = serialize(self)?;

        let mut file = AsyncFile::from_std(file.into_std());
        file.write_all(&buf).await?;

        Ok(())
    }
}
