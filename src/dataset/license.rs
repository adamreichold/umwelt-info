use std::fmt;

use hashbrown::HashMap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum License {
    Unknown,
    Other(String),
    DlDeBy20,
    DlDeZero20,
    CcBy40,
    DorisBfs,
    GeoNutz20130319,
    GeoNutz20131001,
}

impl From<&'_ License> for License {
    fn from(val: &License) -> Self {
        val.clone()
    }
}

impl License {
    pub fn is_other(&self) -> bool {
        matches!(self, Self::Other(_))
    }

    pub fn url(&self) -> Option<&'static str> {
        let val = match self {
            Self::Unknown | Self::Other(_) => return None,
            Self::DlDeBy20 => "https://www.govdata.de/dl-de/by-2-0",
            Self::DlDeZero20 => "https://www.govdata.de/dl-de/zero-2-0",
            Self::CcBy40 => "http://creativecommons.org/licenses/by/4.0/",
            Self::DorisBfs => "https://doris.bfs.de/jspui/impressum/lizenz.html",
            Self::GeoNutz20130319 => {
                "https://sg.geodatenzentrum.de/web_public/gdz/lizenz/geonutzv.pdf"
            }
            Self::GeoNutz20131001 => {
                "http://www.stadtentwicklung.berlin.de/geoinformation/download/nutzIII.pdf"
            }
        };

        Some(val)
    }
}

impl From<&'_ str> for License {
    fn from(val: &str) -> Self {
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
                // Creative Commons Namensnennung – 4.0 International (CC BY 4.0)
                ("cc-by/4.0", License::CcBy40),
                ("http://dcat-ap.de/def/licenses/cc-by/4.0", License::CcBy40),
                ("http://dcat-ap.de/def/licenses/cc-by/4_0", License::CcBy40),
                ("http://dcat-ap.de/def/licenses/CC BY 4.0", License::CcBy40),
                (
                    "https://creativecommons.org/licenses/by/4.0/",
                    License::CcBy40,
                ),
                ("CC-BY-4.0", License::CcBy40),
                // Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes
                ("geoNutz/20130319", License::GeoNutz20130319),
                ("geonutz/20130319", License::GeoNutz20130319),
                ("geonutzv-de-2013-03-19", License::GeoNutz20130319),
                // Nutzungsbestimmungen für die Bereitstellung von Geodaten des Landes Berlin
                ("geoNutz/20131001", License::GeoNutz20131001),
                ("geonutz/20131001", License::GeoNutz20131001),
            ]
            .into()
        });

        let val = val.trim();

        if val.is_empty() {
            return License::Unknown;
        }

        match LICENSES.get(val) {
            Some(license) => license.clone(),
            None => Self::Other(val.to_owned()),
        }
    }
}

impl From<Option<&'_ str>> for License {
    fn from(val: Option<&str>) -> Self {
        match val {
            Some(val) => val.into(),
            None => Self::Unknown,
        }
    }
}

impl fmt::Display for License {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let val = match self {
            Self::Unknown => "unbekannt",
            Self::Other(val) => val,
            Self::DlDeBy20 => "dl-by-de/2.0",
            Self::DlDeZero20 => "dl-zero-de/2.0",
            Self::CcBy40 => "cc-by/4.0",
            Self::DorisBfs => "doris-bfs",
            Self::GeoNutz20130319 => "geoNutz/20130319",
            Self::GeoNutz20131001 => "geoNutz/20131001",
        };

        fmt.write_str(val)
    }
}
