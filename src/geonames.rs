use std::path::Path;

use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, TermQuery},
    schema::{Field, IndexRecordOption},
    Index, IndexReader, Term,
};

use crate::data_path_from_env;

pub static GEO_NAMES: Lazy<GeoNames> = Lazy::new(|| GeoNames::open(&data_path_from_env()));

pub struct GeoNames(Option<GeoNamesInner>);

impl GeoNames {
    pub fn open(data_path: &Path) -> Self {
        match GeoNamesInner::open(data_path) {
            Ok(val) => Self(Some(val)),
            Err(err) => {
                tracing::error!("Failed to open GeoNames index: {:#}", err);

                Self(None)
            }
        }
    }

    pub fn r#match(&self, name: &str) -> Option<u64> {
        let this = match self.0.as_ref() {
            Some(this) => this,
            None => return None,
        };

        match this.r#match(name) {
            Ok(val) => val,
            Err(err) => {
                tracing::error!("Failed to match {} against GeoNames: {:#}", name, err);

                None
            }
        }
    }

    pub fn resolve(&self, id: u64) -> String {
        let placeholder = || format!("GeoNames/{}", id);

        let this = match self.0.as_ref() {
            Some(this) => this,
            None => return placeholder(),
        };

        match this.resolve(id) {
            Ok(val) => val,
            Err(err) => {
                tracing::error!("Failed to resolve {} in GeoNames: {:#}", id, err);

                placeholder()
            }
        }
    }
}

struct GeoNamesInner {
    reader: IndexReader,
    id: Field,
    name: Field,
    alt_names: Field,
}

impl GeoNamesInner {
    fn open(data_path: &Path) -> Result<Self> {
        let index = Index::open_in_dir(data_path.join("geonames"))?;

        let reader = index.reader()?;

        let schema = index.schema();

        let id = schema.get_field("id").unwrap();
        let name = schema.get_field("name").unwrap();
        let alt_names = schema.get_field("alt_names").unwrap();

        Ok(Self {
            reader,
            id,
            name,
            alt_names,
        })
    }

    fn r#match(&self, name: &str) -> Result<Option<u64>> {
        let query = BooleanQuery::union(vec![
            Box::new(TermQuery::new(
                Term::from_field_text(self.name, name),
                IndexRecordOption::Basic,
            )),
            Box::new(TermQuery::new(
                Term::from_field_text(self.alt_names, name),
                IndexRecordOption::Basic,
            )),
        ]);

        let searcher = self.reader.searcher();
        let docs = searcher.search(&query, &TopDocs::with_limit(1))?;

        if let Some((_score, doc)) = docs.into_iter().next() {
            let doc = searcher.doc(doc)?;

            let id = doc.get_first(self.id).unwrap().as_u64().unwrap();

            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    fn resolve(&self, id: u64) -> Result<String> {
        let query = TermQuery::new(Term::from_field_u64(self.id, id), IndexRecordOption::Basic);

        let searcher = self.reader.searcher();
        let docs = searcher.search(&query, &TopDocs::with_limit(1))?;

        let (_score, doc) = docs.into_iter().next().ok_or_else(|| anyhow!(""))?;

        let doc = searcher.doc(doc)?;

        let name = doc.get_first(self.name).unwrap().as_text().unwrap();

        Ok(name.to_owned())
    }
}
