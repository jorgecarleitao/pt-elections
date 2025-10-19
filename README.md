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

## How to generate the dataset

1. `add .env` with two variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
2. `export $(cat .env | xargs)`
3. Run `RUST_LOG=INFO cargo run` to extract from the source
4. Run `python analysis.py` (install boto3 and duckdb) to produce final analysis

## Design decisions

* Use Rust to read sources, that offers excellent performance and control over async environment
* Use DuckDB for analysis
