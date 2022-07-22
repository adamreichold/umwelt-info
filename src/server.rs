use std::io::{BufReader, Write};
use std::sync::Mutex;

use anyhow::Result;
use bincode::{deserialize_from, serialize};
use cap_std::fs::Dir;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

pub struct Stats(Mutex<StatsInner>);

impl Stats {
    pub fn read(dir: &Dir) -> Result<Self> {
        let inner = if let Ok(file) = dir.open("stats_v1") {
            deserialize_from(BufReader::new(file))?
        } else {
            Default::default()
        };

        Ok(Self(Mutex::new(inner)))
    }

    pub fn write(&self, dir: &Dir) -> Result<()> {
        let buf = serialize(&*self.0.lock().unwrap())?;
        dir.create("stats_v1")?.write_all(&buf)?;

        Ok(())
    }

    pub fn record_access(&self, source: &str, id: &str) -> u64 {
        let mut inner = self.0.lock().unwrap();

        let accesses = inner
            .accesses
            .entry_ref(source)
            .or_default()
            .entry_ref(id)
            .or_default();

        *accesses += 1;

        *accesses
    }
}

#[derive(Default, Clone, Deserialize, Serialize)]
struct StatsInner {
    accesses: HashMap<String, HashMap<String, u64>>,
}
