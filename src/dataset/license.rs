use std::fmt;

use hashbrown::HashMap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum License {
    DlDeBy20,
    DlDeZero20,
    Unknown,
    Other(String),
}

impl From<String> for License {
    fn from(val: String) -> Self {
        static LICENSES: Lazy<HashMap<&'static str, License>> = Lazy::new(|| {
            [
                // Datenlizenz Deutschland – Namensnennung – Version 2.0
                ("dl-by-de/2.0", License::DlDeBy20),
                ("dl-de-by-2.0", License::DlDeBy20),
                (
                    "http://dcat-ap.de/def/licenses/dl-by-de/2.0",
                    License::DlDeBy20,
                ),
                (
                    "http://dcat-ap.de/def/licenses/dl-by-de/2_0",
                    License::DlDeBy20,
                ),
                // Datenlizenz Deutschland – Zero – Version 2.0
                ("dl-zero-de/2.0", License::DlDeZero20),
                ("dl-de-zero-2.0", License::DlDeZero20),
                (
                    "http://dcat-ap.de/def/licenses/dl-zero-de/2.0",
                    License::DlDeZero20,
                ),
                (
                    "http://dcat-ap.de/def/licenses/dl-zero-de/2_0",
                    License::DlDeZero20,
                ),
            ]
            .into()
        });

        if val.is_empty() {
            return License::Unknown;
        }

        match LICENSES.get(&*val) {
            Some(license) => license.clone(),
            None => Self::Other(val),
        }
    }
}

impl From<&'_ License> for License {
    fn from(val: &License) -> Self {
        val.clone()
    }
}

impl From<Option<String>> for License {
    fn from(val: Option<String>) -> Self {
        match val {
            Some(val) => val.into(),
            None => Self::Unknown,
        }
    }
}

impl fmt::Display for License {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let val = match self {
            Self::DlDeBy20 => "dl-by-de/2.0",
            Self::DlDeZero20 => "dl-zero-de/2.0",
            Self::Unknown => "unbekannt",
            Self::Other(val) => return write!(fmt, "andere: {val}"),
        };

        fmt.write_str(val)
    }
}
