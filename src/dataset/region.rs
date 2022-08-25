use std::fmt;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{data_path_from_env, geonames::GeoNames};

#[derive(Debug, Deserialize, Serialize)]
pub enum Region {
    Other(String),
    GeoName(u64),
}

impl From<&'_ str> for Region {
    fn from(val: &str) -> Self {
        if let Some(id) = GEO_NAMES.r#match(val).unwrap() {
            Self::GeoName(id)
        } else {
            Self::Other(val.to_owned())
        }
    }
}

impl fmt::Display for Region {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Other(val) => fmt.write_str(val),
            Self::GeoName(id) => {
                let name = GEO_NAMES.resolve(*id).unwrap();

                fmt.write_str(&name)
            }
        }
    }
}

static GEO_NAMES: Lazy<GeoNames> = Lazy::new(|| GeoNames::open(&data_path_from_env()).unwrap());
