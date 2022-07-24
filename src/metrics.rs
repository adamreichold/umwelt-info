use std::io::{BufReader, Write};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use bincode::{deserialize_from, serialize};
use cap_std::fs::Dir;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Metrics {
    pub harvests: HashMap<String, Harvest>,
}

impl Metrics {
    pub fn read(dir: &Dir) -> Result<Self> {
        let val = if let Ok(file) = dir.open("metrics") {
            deserialize_from(BufReader::new(file))?
        } else {
            Default::default()
        };

        Ok(val)
    }

    pub fn write(&self, dir: &Dir) -> Result<()> {
        let buf = serialize(self)?;

        let mut file = dir.create("metrics.new")?;
        file.write_all(&buf)?;
        dir.rename("metrics.new", dir, "metrics")?;

        Ok(())
    }

    pub fn record_harvest(
        &mut self,
        source_name: String,
        start: SystemTime,
        duration: Duration,
        count: usize,
        transmitted: usize,
        failed: usize,
    ) {
        self.harvests.insert(
            source_name,
            Harvest {
                start,
                duration,
                count,
                transmitted,
                failed,
            },
        );
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Harvest {
    pub start: SystemTime,
    pub duration: Duration,
    pub count: usize,
    pub transmitted: usize,
    pub failed: usize,
}
