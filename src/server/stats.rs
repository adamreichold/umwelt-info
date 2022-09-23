use std::io::{BufReader, Write};

use anyhow::{Context, Result};
use bincode::config::{DefaultOptions, Options};
use cap_std::fs::Dir;
use hashbrown::HashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Deserialize, Serialize)]
pub struct Stats {
    pub accesses: HashMap<String, HashMap<String, u64>>,
    pub terms: HashMap<String, u64>,
}

#[derive(Deserialize)]
struct OldStats {
    pub accesses: HashMap<String, HashMap<String, u64>>,
}

impl Stats {
    pub fn read(dir: &Dir) -> Result<Self> {
        let val = if let Ok(mut file) = dir.open("stats") {
            let res = options().deserialize_from::<_, Stats>(BufReader::new(&mut file));

            match res {
                Ok(val) => val,
                Err(err) => {
                    let old_val = options()
                        .deserialize_from::<_, OldStats>(BufReader::new(&mut file))
                        .map_err(|_old_err| err)
                        .context("Failed to deserialize stats")?;

                    Self {
                        accesses: old_val.accesses,
                        terms: HashMap::new(),
                    }
                }
            }
        } else {
            Default::default()
        };

        Ok(val)
    }

    pub fn write(this: &Mutex<Self>, dir: &Dir) -> Result<()> {
        let buf = options().serialize(&*this.lock())?;

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

    pub fn record_terms<'a, T>(&mut self, terms: T)
    where
        T: Iterator<Item = &'a String>,
    {
        terms.for_each(|term| {
            *self.terms.entry_ref(term).or_default() += 1;
        });
    }
}

fn options() -> impl Options {
    DefaultOptions::new().with_fixint_encoding()
}
