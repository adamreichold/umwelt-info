use std::io::{BufReader, Write};

use anyhow::Result;
use bincode::{
    config::standard,
    serde::{decode_from_std_read, encode_to_vec},
};
use cap_std::fs::Dir;
use hashbrown::HashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Stats {
    pub accesses: HashMap<String, HashMap<String, u64>>,
}

impl Stats {
    pub fn read(dir: &Dir) -> Result<Self> {
        let val = if let Ok(file) = dir.open("stats") {
            decode_from_std_read(
                &mut BufReader::new(file),
                standard().with_fixed_int_encoding(),
            )?
        } else {
            Default::default()
        };

        Ok(val)
    }

    pub fn write(this: &Mutex<Self>, dir: &Dir) -> Result<()> {
        let buf = encode_to_vec(&*this.lock(), standard().with_fixed_int_encoding())?;

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
