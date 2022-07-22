use std::fs::create_dir_all;
use std::path::Path;

use anyhow::Result;
use tantivy::{
    collector::{Count, TopDocs},
    directory::MmapDirectory,
    fastfield::FastFieldReader,
    query::QueryParser,
    schema::{
        Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST, STORED,
    },
    tokenizer::{Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer},
    Document, Index, IndexReader, IndexWriter, Score, SegmentReader,
};

use crate::dataset::Dataset;

fn schema() -> Schema {
    let text = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
            .set_tokenizer("de_stem"),
    );

    let mut schema = Schema::builder();

    schema.add_text_field("source", STORED);
    schema.add_text_field("id", STORED);

    schema.add_text_field("title", text.clone());
    schema.add_text_field("description", text);

    schema.add_u64_field("accesses", FAST);

    schema.build()
}

fn register_tokenizer(index: &Index) {
    let de_stem = TextAnalyzer::from(SimpleTokenizer)
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(Stemmer::new(Language::German));

    index.tokenizers().register("de_stem", de_stem);
}

pub struct Searcher {
    reader: IndexReader,
    parser: QueryParser,
    fields: Fields,
}

impl Searcher {
    pub fn open(data_path: &Path) -> Result<Self> {
        let index = Index::open_in_dir(data_path.join("index"))?;
        register_tokenizer(&index);

        let fields = Fields::new(&index.schema());

        let reader = index.reader()?;
        let parser = QueryParser::for_index(&index, vec![fields.title, fields.description]);

        Ok(Self {
            reader,
            parser,
            fields,
        })
    }

    pub fn search(
        &self,
        query: &str,
    ) -> Result<(usize, impl Iterator<Item = Result<(String, String)>> + '_)> {
        let query = self.parser.parse_query(query)?;
        let searcher = self.reader.searcher();
        let accesses = self.fields.accesses;

        let (count, docs) = searcher.search(
            &query,
            &(
                Count,
                TopDocs::with_limit(10).tweak_score(move |reader: &SegmentReader| {
                    let reader = reader.fast_fields().u64(accesses).unwrap();

                    move |doc, score| {
                        let accesses: u64 = reader.get(doc);
                        let boost = ((2 + accesses) as Score).log2();

                        boost * score
                    }
                }),
            ),
        )?;

        let iter = docs.into_iter().map(move |(_score, doc)| {
            let doc = searcher.doc(doc)?;

            let source = match doc.get_first(self.fields.source) {
                Some(Value::Str(source)) => source.clone(),
                _ => unreachable!(),
            };

            let id = match doc.get_first(self.fields.id) {
                Some(Value::Str(id)) => id.clone(),
                _ => unreachable!(),
            };

            Ok((source, id))
        });

        Ok((count, iter))
    }
}

pub struct Indexer {
    writer: IndexWriter,
    fields: Fields,
}

impl Indexer {
    pub fn start(data_path: &Path) -> Result<Self> {
        let index_path = data_path.join("index");
        create_dir_all(&index_path)?;

        let schema = schema();
        let fields = Fields::new(&schema);

        let index = Index::open_or_create(MmapDirectory::open(index_path)?, schema)?;
        register_tokenizer(&index);

        let writer = index.writer(128 << 20)?;
        writer.delete_all_documents()?;

        Ok(Self { writer, fields })
    }

    pub fn add_document(
        &self,
        source: String,
        id: String,
        dataset: Dataset,
        accesses: u64,
    ) -> Result<()> {
        let mut doc = Document::default();

        doc.add_text(self.fields.source, source);
        doc.add_text(self.fields.id, id);

        doc.add_text(self.fields.title, dataset.title);
        doc.add_text(self.fields.description, dataset.description);

        doc.add_u64(self.fields.accesses, accesses);

        self.writer.add_document(doc)?;

        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        self.writer.commit()?;

        Ok(())
    }
}

struct Fields {
    source: Field,
    id: Field,
    title: Field,
    description: Field,
    accesses: Field,
}

impl Fields {
    fn new(schema: &Schema) -> Self {
        let source = schema.get_field("source").unwrap();
        let id = schema.get_field("id").unwrap();

        let title = schema.get_field("title").unwrap();
        let description = schema.get_field("description").unwrap();

        let accesses = schema.get_field("accesses").unwrap();

        Self {
            source,
            id,
            title,
            description,
            accesses,
        }
    }
}
