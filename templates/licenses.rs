use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Hash, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum License {
    {% for license in licenses %}

    {{ license.identifier|ident }},

    {% endfor %}

    Other(String),
    Unknown,
}

impl fmt::Debug for License {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let identifier = match self {
            {% for license in licenses %}

            Self::{{ license.identifier|ident }} => "{{ license.identifier }}",

            {% endfor %}

            Self::Other(_val) => "other",
            Self::Unknown => "unknown",
        };

        write!(fmt, "{}", identifier)
    }
}

impl fmt::Display for License {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let label = match self {
            {% for license in licenses %}

            Self::{{ license.identifier|ident }} => "{{ license.label }}",

            {% endfor %}

            Self::Other(val) => val,
            Self::Unknown => "Unbekannt",
        };

        write!(fmt, "{}", label)
    }
}

impl FromStr for License {
    type Err = Infallible;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        let val = match val {
            {% for license in licenses %}

            "{{ license.identifier }}" => Self::{{ license.identifier|ident }},

            {% endfor %}

            _ => Self::Other(val.to_owned()),
        };

        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_license_identifier() {
        assert_eq!(
            "dl-by-de/2.0".parse::<License>().unwrap(),
            License::dl_by_de_2_0
        );
    }
}
