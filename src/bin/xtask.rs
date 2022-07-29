use std::env::args;
use std::fs::remove_dir_all;
use std::process::Command;

use anyhow::{anyhow, ensure, Result};

fn main() -> Result<()> {
    match args().nth(1).as_deref() {
        None => default(),
        Some("harvester") => harvester(),
        Some("indexer") => indexer(),
        Some("server") => server(),
        Some(name) => Err(anyhow!("Unknown task {}", name)),
    }
}

fn default() -> Result<()> {
    let status = Command::new("cargo").arg("fmt").status()?;

    ensure!(status.success(), "Rustfmt failed with status {:?}", status);

    let status = Command::new("cargo")
        .args(["clippy", "--all-targets"])
        .status()?;

    ensure!(status.success(), "Clippy failed with status {:?}", status);

    let status = Command::new("cargo").arg("test").status()?;

    ensure!(status.success(), "Tests failed with status {:?}", status);

    Ok(())
}

fn harvester() -> Result<()> {
    let status = Command::new("cargo")
        .args(["run", "--bin", "harvester"])
        .envs([
            ("DATA_PATH", "data"),
            ("RUST_LOG", "info,umwelt_info=debug,harvester=debug"),
        ])
        .status()?;

    ensure!(
        status.success(),
        "Harvester failed with status {:?}",
        status
    );

    indexer()?;

    Ok(())
}

fn indexer() -> Result<()> {
    let _ = remove_dir_all("data/index");

    let status = Command::new("cargo")
        .args(["run", "--bin", "indexer"])
        .envs([
            ("DATA_PATH", "data"),
            ("RUST_LOG", "info,umwelt_info=debug,indexer=debug"),
        ])
        .status()?;

    ensure!(status.success(), "Indexer failed with status {:?}", status);

    Ok(())
}

fn server() -> Result<()> {
    let status = Command::new("cargo")
        .args(["run", "--bin", "server"])
        .envs([
            ("DATA_PATH", "data"),
            ("BIND_ADDR", "127.0.0.1:8081"),
            ("REQUEST_LIMIT", "32"),
            ("RUST_LOG", "info,umwelt_info=debug,server=debug"),
        ])
        .status()?;

    ensure!(status.success(), "Server failed with status {:?}", status);

    Ok(())
}
