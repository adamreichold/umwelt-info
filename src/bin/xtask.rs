use std::env::args;
use std::fs::remove_dir_all;
use std::process::Command;

use anyhow::{anyhow, ensure, Result};

fn main() -> Result<()> {
    match args().nth(1).as_deref() {
        None => default(),
        Some("doc") => doc(),
        Some("harvester") => harvester(),
        Some("indexer") => indexer(),
        Some("server") => server(),
        Some(name) => Err(anyhow!("Unknown task {}", name)),
    }
}

fn default() -> Result<()> {
    cargo("Rustfmt", ["fmt"], [])?;

    cargo("Clippy", ["clippy", "--all-targets"], [])?;

    cargo("Tests", ["test"], [])?;

    Ok(())
}

fn doc() -> Result<()> {
    cargo("Rustdoc", ["doc", "--document-private-items"], [])?;

    println!("Documentation built at target/doc/umwelt_info/index.html");

    Ok(())
}

fn harvester() -> Result<()> {
    cargo(
        "Harvester",
        ["run", "--bin", "harvester"],
        [
            ("DATA_PATH", "data"),
            ("RUST_LOG", "info,umwelt_info=debug,harvester=debug"),
        ],
    )?;

    indexer()?;

    Ok(())
}

fn indexer() -> Result<()> {
    let _ = remove_dir_all("data/index");

    cargo(
        "Indexer",
        ["run", "--bin", "indexer"],
        [
            ("DATA_PATH", "data"),
            ("RUST_LOG", "info,umwelt_info=debug,indexer=debug"),
        ],
    )?;

    Ok(())
}

fn server() -> Result<()> {
    cargo(
        "Server",
        ["run", "--bin", "server"],
        [
            ("DATA_PATH", "data"),
            ("BIND_ADDR", "127.0.0.1:8081"),
            ("REQUEST_LIMIT", "32"),
            ("RUST_LOG", "info,umwelt_info=debug,server=debug"),
        ],
    )?;

    Ok(())
}

fn cargo<'a, 'e, A, E>(name: &str, args: A, envs: E) -> Result<()>
where
    A: IntoIterator<Item = &'a str>,
    E: IntoIterator<Item = (&'e str, &'e str)>,
{
    let status = Command::new("cargo").args(args).envs(envs).status()?;

    ensure!(status.success(), "{name} failed with status {status:?}");

    Ok(())
}
