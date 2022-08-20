use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Resource {
    pub r#type: Type,
    pub url: String,
}

impl Resource {
    pub fn unknown(url: String) -> Self {
        Self {
            r#type: Type::Unknown,
            url,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Type {
    Unknown,
    Pdf,
    Csv,
    JsonLd,
}

impl fmt::Display for Type {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let val = match self {
            Self::Unknown => "unbekannt",
            Self::Pdf => "PDF",
            Self::Csv => "CSV",
            Self::JsonLd => "JSON-LD",
        };

        fmt.write_str(val)
    }
}
