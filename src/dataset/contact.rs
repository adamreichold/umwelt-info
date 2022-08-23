use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Debug, Deserialize, Serialize)]
pub struct Contact {
    pub name: String,
    pub emails: SmallVec<[String; 1]>,
}
