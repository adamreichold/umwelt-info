use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum Tag {
    Other(String),
    Wrrl,
    HwrmRl,
    MsrRl,
    BgRl,
}

impl From<&'_ Tag> for Tag {
    fn from(val: &Tag) -> Self {
        val.clone()
    }
}

impl From<String> for Tag {
    fn from(val: String) -> Self {
        Self::Other(val)
    }
}

impl From<&'_ str> for Tag {
    fn from(val: &str) -> Self {
        val.to_owned().into()
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let val = match self {
            Self::Other(val) => val,
            Self::Wrrl => "WRRL",
            Self::HwrmRl => "HWRM-RL",
            Self::MsrRl => "MSR-RL",
            Self::BgRl => "BG-RL",
        };

        fmt.write_str(val)
    }
}

impl Tag {
    pub fn with_tokens<F>(&self, f: F)
    where
        F: FnOnce(&[&str]),
    {
        let val = match self {
            Self::Other(val) => return f(&[val]),
            Self::Wrrl => &["WRRL", "Wasserrahmenrichtlinie", "Wasserrahmen-Richtlinie"],
            Self::HwrmRl => &[
                "HWRM-RL",
                "Hochwasserrisikomanagement-Richtlinie",
                "Hochwasserrisikomanagementrichtlinie",
            ],
            Self::MsrRl => &[
                "MSR-RL",
                "Meeresstrategie-Rahmenrichtlinie",
                "Meeresstrategierahmenrichtlinie",
            ],
            Self::BgRl => &["BG-RL", "Badegewässer-Richtlinie", "Badegewässerrichtlinie"],
        };

        f(val)
    }
}
