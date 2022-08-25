use std::fs::{create_dir, remove_dir_all};
use std::io::stdin;

use anyhow::Result;
use csv::ReaderBuilder;
use serde::Deserialize;
use tantivy::{
    schema::{NumericOptions, Schema, STORED, STRING},
    store::Compressor,
    Document, Index, IndexSettings,
};

use umwelt_info::data_path_from_env;

fn main() -> Result<()> {
    let mut schema = Schema::builder();

    let id = schema.add_u64_field("id", NumericOptions::default().set_indexed().set_stored());
    let name = schema.add_text_field("name", STRING | STORED);
    let alt_names = schema.add_text_field("alt_names", STRING);

    let schema = schema.build();

    let data_path = data_path_from_env();
    let index_path = data_path.join("geonames");

    let _ = remove_dir_all(&index_path);
    create_dir(&index_path)?;

    let index = Index::builder()
        .schema(schema)
        .settings(IndexSettings {
            docstore_compression: Compressor::Zstd,
            ..Default::default()
        })
        .create_in_dir(index_path)?;

    let mut writer = index.writer(128 << 20)?;

    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_reader(stdin());

    for record in reader.deserialize::<Record>() {
        let record = record?;

        let mut doc = Document::default();

        doc.add_u64(id, record.id);

        doc.add_text(name, record.name);

        doc.add_text(alt_names, record.ascii_name);

        for alt_name in record.alt_names.split(',') {
            doc.add_text(alt_names, alt_name);
        }

        writer.add_document(doc)?;
    }

    writer.commit()?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct Record {
    id: u64,
    name: String,
    ascii_name: String,
    alt_names: String,
}
