use std::fmt;

use serde::{Deserialize, Serialize};

use crate::geonames::GEO_NAMES;

#[derive(Debug, Deserialize, Serialize)]
pub enum Region {
    Other(String),
    GeoName(u64),
}

impl Region {
    pub fn url(&self) -> Option<String> {
        let val = match self {
            Self::Other(_) => return None,
            Self::GeoName(id) => format!("https://www.geonames.org/{}/", id),
        };

        Some(val)
    }
}

impl From<&'_ str> for Region {
    fn from(val: &str) -> Self {
        if let Some(id) = GEO_NAMES.r#match(val) {
            return Self::GeoName(id);
        }

        Self::Other(val.to_owned())
    }
}

impl fmt::Display for Region {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Other(val) => fmt.write_str(val),
            Self::GeoName(id) => {
                let name = GEO_NAMES.resolve(*id);

                fmt.write_str(&name)
            }
        }
    }
}
