use std::io::{BufReader, Write};
use std::sync::Mutex;

use anyhow::Result;
use bincode::config::{DefaultOptions, Options};
use cap_std::fs::Dir;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Stats {
    pub accesses: HashMap<String, HashMap<String, u64>>,
}

impl Stats {
    pub fn read(dir: &Dir) -> Result<Self> {
        let val = if let Ok(file) = dir.open("stats") {
            DefaultOptions::new()
                .with_fixint_encoding()
                .deserialize_from(BufReader::new(file))?
        } else {
            Default::default()
        };

        Ok(val)
    }

    pub fn write(this: &Mutex<Self>, dir: &Dir) -> Result<()> {
        let buf = DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(&*this.lock().unwrap())?;

        let mut file = dir.create("stats.new")?;
        file.write_all(&buf)?;
        dir.rename("stats.new", dir, "stats")?;

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
