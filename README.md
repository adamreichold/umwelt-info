# umwelt.info metadata index

[![CI](https://github.com/adamreichold/umwelt-info/actions/workflows/ci.yml/badge.svg)](https://github.com/adamreichold/umwelt-info/actions/workflows/ci.yml)

This project is a prototype for a metadata index for the [umwelt.info project](https://umwelt.info). It aims for efficient operation by using the [Rust programming language](https://www.rust-lang.org/) and storing the datasets and a search index directly in the file system to avoid dependencies on additional services like databases or search engines. It does not aim to be abstract, generic, configurable or programmable, especially where that would conflict with efficiency.

The system is implemented as three separate programs that access a common file system directory at `$DATA_PATH`.

* The _harvester_ periodically harvests/crawls/scrapes the sources defined in `$DATA_PATH/harvester.toml` to write all datasets to `$DATA_PATH/datasets` with one directory per source and one file per dataset and to store summary metrics in `$DATA_PATH/metrics`.

* The _indexer_ usually runs after the harvester and reads all datasets to produce a search index over their properties in `$DATA_PATH/index` using the [Tantivy library](https://github.com/quickwit-oss/tantivy).

* The _server_ provides an HTTP-based API to query the search index and retrieve individual datasets. It also collects access statistics about each datasets in `$DATA_PATH/stats_v*`. It is the only continuously running component and can be scaled out by exporting `$DATA_PATH` via a networked file system like NFS or SMB.

## Development and operation

The code is organised as a single library with three entry points for the above mentioned programs. A fourth binary named `xtask` is used automate the development workflow.

The CI pipelines checks formatting via Rustfmt, ensure a warning-free build using Clippy, runs the unit and integration tests and builds and collects optimized binaries.

The system is deployed using a set of sandboxed [systemd units](https://systemd.io/), both for periodically running the harvest and indexer as well as continuously running the server.

### How to get started

To format, lint and test the code, run

```console
> cargo xtask
```

[`deployment/harvester.toml`](deployment/harvester.toml) tracks all relevant sources. Based on that, a configuration like

```toml
[[sources]]
name = "uba-gdi"
type = "csw"
url = "https://gis.uba.de/smartfinder-csw/api/"
source_url = "https://gis.uba.de/smartfinder-client/?lang=de#/datasets/iso/{{id}}"
```

should be created at `data/harvester.toml`, so that the harvester and indexer can be invoked by

```console
> cargo xtask harvester
```

Finally, executing

```console
> cargo xtask server
```

will make the server listen on `127.0.0.1:8081`.

### Replaying responses

Iteratively developing harvesters can be time-consuming and place undue load on the source due to large responses being transmitted over the network. To mitigate this issue, each request must be identified using a key

```rust
let response = client.make_request(&format!("{}-{}", source.name, record_number), |client| ...).await?;
```

under which its response is stored on disk. Once development has reached a state where the set of requests is stable, their responses can be replayed by setting `$REPLAY_RESPONSES`, e.g.

```console
> REPLAY_RESPONSES= cargo xtask harvester
```

### Content negotiation

The HTTP routes `/search` and `/dataset` support content negotiation insofar they yield either rendered HTML pages or the underlying JSON data depending on the `Accept` header transmitted by the HTTP client.
