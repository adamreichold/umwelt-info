use std::io::{BufReader, Write};
use std::sync::Mutex;

use anyhow::Result;
use bincode::{deserialize_from, serialize};
use cap_std::fs::Dir;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Stats {
    pub accesses: HashMap<String, HashMap<String, u64>>,
}

impl Stats {
    pub fn read(dir: &Dir) -> Result<Self> {
        let val = if let Ok(file) = dir.open("stats_v1") {
            deserialize_from(BufReader::new(file))?
        } else {
            Default::default()
        };

        Ok(val)
    }

    pub fn write(this: &Mutex<Self>, dir: &Dir) -> Result<()> {
        let buf = serialize(&*this.lock().unwrap())?;

        let mut file = dir.create("stats_v1.new")?;
        file.write_all(&buf)?;
        dir.rename("stats_v1.new", dir, "stats_v1")?;

        Ok(())
    }

    pub fn record_access(&mut self, source: &str, id: &str) -> u64 {
        let accesses = self
            .accesses
            .entry_ref(source)
            .or_default()
            .entry_ref(id)
            .or_default();

        *accesses += 1;

        *accesses
    }
}
