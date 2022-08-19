use std::fs::create_dir_all;
use std::path::Path;

use anyhow::Result;
use tantivy::{
    collector::{Count, FacetCollector, FacetCounts, TopDocs},
    directory::MmapDirectory,
    fastfield::FastFieldReader,
    query::{BooleanQuery, QueryParser, TermQuery},
    schema::{
        Facet, FacetOptions, Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions,
        Value, FAST, STORED, STRING,
    },
    tokenizer::{Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer},
    Document, Index, IndexReader, IndexWriter, Score, SegmentReader, Term,
};

use crate::dataset::Dataset;

fn schema() -> Schema {
    let text = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
            .set_tokenizer("de_stem"),
    );

    let mut schema = Schema::builder();

    schema.add_text_field("source", STRING | STORED);
    schema.add_text_field("id", STORED);

    schema.add_text_field("title", text.clone());
    schema.add_text_field("description", text.clone());

    schema.add_text_field("comment", text);

    schema.add_facet_field("provenance", FacetOptions::default());
    schema.add_facet_field("license", FacetOptions::default());

    schema.add_text_field("tags", STRING);

    schema.add_u64_field("accesses", FAST);

    schema.build()
}

fn register_tokenizers(index: &Index) {
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
        register_tokenizers(&index);

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
        provenances_root: &Facet,
        licenses_root: &Facet,
        limit: usize,
        offset: usize,
    ) -> Result<Results<impl Iterator<Item = Result<(String, String)>> + '_>> {
        let query = self.parser.parse_query(query)?;

        let mut terms = Default::default();
        query.query_terms(&mut terms);

        let terms = terms
            .into_iter()
            .filter_map(|(term, _)| term.as_str().map(|term| term.to_owned()))
            .collect();

        let provenances_query = TermQuery::new(
            Term::from_facet(self.fields.provenance, provenances_root),
            IndexRecordOption::Basic,
        );

        let licenses_query = TermQuery::new(
            Term::from_facet(self.fields.license, licenses_root),
            IndexRecordOption::Basic,
        );

        let query = BooleanQuery::intersection(vec![
            query,
            Box::new(provenances_query),
            Box::new(licenses_query),
        ]);

        let mut provenances = FacetCollector::for_field(self.fields.provenance);
        provenances.add_facet(provenances_root.clone());

        let mut licenses = FacetCollector::for_field(self.fields.license);
        licenses.add_facet(licenses_root.clone());

        let searcher = self.reader.searcher();
        let accesses = self.fields.accesses;

        let (count, docs, provenances, licenses) = searcher.search(
            &query,
            &(
                Count,
                TopDocs::with_limit(limit).and_offset(offset).tweak_score(
                    move |reader: &SegmentReader| {
                        let reader = reader.fast_fields().u64(accesses).unwrap();

                        move |doc, score| {
                            let accesses: u64 = reader.get(doc);
                            let boost = ((2 + accesses) as Score).log2();

                            boost * score
                        }
                    },
                ),
                provenances,
                licenses,
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

        Ok(Results {
            count,
            iter,
            provenances,
            licenses,
            terms,
        })
    }
}

pub struct Results<I> {
    pub count: usize,
    pub iter: I,
    pub provenances: FacetCounts,
    pub licenses: FacetCounts,
    pub terms: Vec<String>,
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
        register_tokenizers(&index);

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

        if let Some(description) = dataset.description {
            doc.add_text(self.fields.description, description);
        }

        if let Some(comment) = dataset.comment {
            doc.add_text(self.fields.comment, comment);
        }

        doc.add_facet(
            self.fields.provenance,
            Facet::from_text(&dataset.provenance)?,
        );

        doc.add_facet(
            self.fields.license,
            Facet::from_path(dataset.license.facet()),
        );

        for tag in dataset.tags {
            tag.with_tokens(|tokens| {
                for token in tokens {
                    doc.add_text(self.fields.tags, token.to_owned());
                }
            });
        }

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
    comment: Field,
    provenance: Field,
    license: Field,
    tags: Field,
    accesses: Field,
}

impl Fields {
    fn new(schema: &Schema) -> Self {
        let source = schema.get_field("source").unwrap();
        let id = schema.get_field("id").unwrap();

        let title = schema.get_field("title").unwrap();
        let description = schema.get_field("description").unwrap();
        let comment = schema.get_field("comment").unwrap();

        let provenance = schema.get_field("provenance").unwrap();
        let license = schema.get_field("license").unwrap();

        let tags = schema.get_field("tags").unwrap();

        let accesses = schema.get_field("accesses").unwrap();

        Self {
            source,
            id,
            title,
            description,
            comment,
            provenance,
            license,
            tags,
            accesses,
        }
    }
}
