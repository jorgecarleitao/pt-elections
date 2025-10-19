# PT elections

This repository contains the source code that produces a dataset of the most recent portuguese elections.

You can find its specification and instructions on how to use the dataset here: [specification.md](specification.md).

## How this works

This project uses Rust for downloading and storing the data, and DuckDB/SQL/Python for analysis and agregations.

Data flow:

* `RUST_LOG=INFO cargo run` reads from sources and writes:
    * `s3://pt-elections/raw/autarquicas2025/*/*.json`
    * `s3://pt-elections/raw/legislativas2025/*/*.json`
* `python analysis.py` reads above and writes:
    * `s3://pt-elections/processed/autarquicas2025/v1/all.json`
    * `s3://pt-elections/processed/legislativas2025/v1/all.json`
    * `s3://pt-elections/processed/all/v1/all.csv`

The last item is the dataset described in the [specification.md](specification.md).

## Design decisions

* Use Rust to read sources, that offers excellent performance and control over async environment
* Use DuckDB for analysis
