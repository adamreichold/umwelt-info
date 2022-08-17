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
    fn with_tokens<F>(&self, f: F)
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

pub trait TagExt {
    fn join_tokens(&self, separator: &str) -> String;
}

impl TagExt for [Tag] {
    fn join_tokens(&self, separator: &str) -> String {
        let mut val = String::new();

        let mut tags = self.iter();

        if let Some(tag) = tags.next() {
            tag.with_tokens(|tokens| {
                let mut tokens = tokens.iter();

                if let Some(token) = tokens.next() {
                    val.push_str(token);
                }

                for token in tokens {
                    val.push_str(separator);
                    val.push_str(token);
                }
            });
        }

        for tag in tags {
            tag.with_tokens(|tokens| {
                for token in tokens {
                    val.push_str(separator);
                    val.push_str(token);
                }
            });
        }

        val
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_single_tag_single_token() {
        assert_eq!([Tag::from("foobar")].join_tokens(", "), "foobar");
    }

    #[test]
    fn join_single_tag_multiple_tokens() {
        assert_eq!(
            [Tag::Wrrl].join_tokens(", "),
            "WRRL, Wasserrahmenrichtlinie, Wasserrahmen-Richtlinie"
        );
    }

    #[test]
    fn join_multiple_tags_single_token() {
        assert_eq!(
            [Tag::from("foo"), Tag::from("bar")].join_tokens(", "),
            "foo, bar"
        );
    }

    #[test]
    fn join_multiple_tags_multiple_tokens() {
        assert_eq!(
            [Tag::Wrrl, Tag::HwrmRl].join_tokens(", "),
            "WRRL, Wasserrahmenrichtlinie, Wasserrahmen-Richtlinie, HWRM-RL, Hochwasserrisikomanagement-Richtlinie, Hochwasserrisikomanagementrichtlinie"
        );
    }
}
