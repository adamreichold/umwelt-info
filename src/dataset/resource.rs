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
    Xml,
    Image,
    Archive,
    PlainText,
}

impl From<&'_ str> for Type {
    fn from(val: &str) -> Self {
        match val {
            "text/plain" => Self::PlainText,
            "application/pdf" => Self::Pdf,
            "application/xml" => Self::Xml,
            "image/png" => Self::Image,
            "wms_xml" => Self::Image,
            "application/zip" => Self::Archive,
            "" => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let val = match self {
            Self::Unknown => "Unknown",
            Self::Pdf => "PDF",
            Self::Csv => "CSV",
            Self::JsonLd => "JSON-LD",
            Self::Xml => "XML",
            Self::Image => "Image",
            Self::Archive => "Compressed archive",
            Self::PlainText => "Plain text",
        };

        fmt.write_str(val)
    }
}
