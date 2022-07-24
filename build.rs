use std::env::var_os;
use std::fs::{read_to_string, write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use askama::Template;
use roxmltree::Document;

fn main() -> Result<()> {
    let out_dir = PathBuf::from(var_os("OUT_DIR").unwrap());

    generate_licenses(&out_dir)?;

    Ok(())
}

fn generate_licenses(out_dir: &Path) -> Result<()> {
    #[derive(Default)]
    struct License {
        identifier: String,
        label: String,
    }

    let mut licenses = Vec::new();

    let vocab = read_to_string("vocabulary/licenses.rdf")?;

    let vocab = Document::parse(&vocab)?;

    for node in vocab.root_element().children() {
        if node.has_tag_name("Concept") {
            let mut license = License::default();

            for node in node.children() {
                if node.has_tag_name("identifier") {
                    license.identifier = node
                        .text()
                        .ok_or_else(|| anyhow!("Missing identifier text"))?
                        .to_owned();
                } else if node.has_tag_name((SKOS, "prefLabel")) {
                    license.label = node
                        .text()
                        .ok_or_else(|| anyhow!("Missing label text"))?
                        .to_owned();
                }
            }

            licenses.push(license);
        }
    }

    #[derive(Template)]
    #[template(path = "licenses.rs", escape = "none")]
    struct Licenses {
        licenses: Vec<License>,
    }

    let licenses = Licenses { licenses }.render().unwrap();

    write(out_dir.join("licenses.rs"), licenses.as_bytes())?;

    println!("cargo:rerun-if-changed=vocabulary/licenses.rdf");
    println!("cargo:rerun-if-changed=templates/licenses.rs");

    Ok(())
}

const SKOS: &str = "http://www.w3.org/2004/02/skos/core#";

mod filters {
    use askama::Result;

    pub fn ident(val: &str) -> Result<String> {
        Ok(val.replace(&['-', '/', '.'], "_"))
    }
}
