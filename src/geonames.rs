use std::path::Path;

use anyhow::{anyhow, Result};
use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, TermQuery},
    schema::{Field, IndexRecordOption},
    Index, IndexReader, Term,
};

pub struct GeoNames {
    reader: IndexReader,
    id: Field,
    name: Field,
    alt_names: Field,
}

impl GeoNames {
    pub fn open(data_path: &Path) -> Result<Self> {
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

    pub fn r#match(&self, name: &str) -> Result<Option<u64>> {
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

    pub fn resolve(&self, id: u64) -> Result<String> {
        let query = TermQuery::new(Term::from_field_u64(self.id, id), IndexRecordOption::Basic);

        let searcher = self.reader.searcher();
        let docs = searcher.search(&query, &TopDocs::with_limit(1))?;

        let (_score, doc) = docs.into_iter().next().ok_or_else(|| anyhow!(""))?;

        let doc = searcher.doc(doc)?;

        let name = doc.get_first(self.name).unwrap().as_text().unwrap();

        Ok(name.to_owned())
    }
}
