# Specification

This repository extracts the results from the Portuguese elections
and stores them in a CSV and JSON format on a blob storage for analysis.

This document describes how this data was collected, where it is stored, and contains the data specification
of each dataset.

## Data specification

### DS-all-results - All results

The raw data is sourced from the following websites:

* `autarquicas2025` for 2021 and 2025 eleicões autárquicas from https://www.autarquicas2025.mai.gov.pt
* `legislativas2025` from 2024 and 2025 eleicões legislativas from https://www.eleicoes.mai.gov.pt/legislativas2025

and stored in JSON and available at s3 endpoint `fsn1.your-objectstorage.com`, path `s3://pt-elections/raw/`.

It is then processed into a single utf-8 encoded CSV file `s3://pt-elections/processed/v1/all.csv` that contains the dataset with the following schema:

```yaml
columns:
  election:
    type: string
    description: Unique identifier of the election
    invariants:
      - one_of:
        - legislativas(2024)
        - legislativas(2025)
        - autarquicas(2021)
        - autarquicas(2025)
  territory_type:
    type: string
    description: |
      The type of the territory the votes are:
        * `país`: from Portugal
        * `distrito`: One of the distrits from Portugal
        * `concelho`: One of the municipalities of a distrit from Portugal
        * `freguesia`: One of the civil parish of a municipality of a distrit from Portugal
        * `zona estrangeira`: foreign set of countries or country (e.g. Europa, Alemanha)
        * `posto consular`: from a consular station outside Portugal
    invariants:
      - one_of:
        - país
        - distrito
        - concelho
        - freguesia
        - zona estrangeira
        - posto consular
    examples:
      - distrito
  territory:
    type: string
    description: The name of the territory
    examples:
      - Fora da Europa
      - Lisboa
      - Golegã
  organ_type:
    type: string
    description: |
      The name of the organ the vote is for:
        * `ar`: Assembleia da República. Only present in eleicões legislativas.
        * `cm`: Câmara Municipal. Only present in eleicões autárquicas.
        * `am`: Assembleia Municipal. Only present in eleicões autárquicas.
        * `af`: Assembleia de Freguesia. Only present in eleicões autárquicas.
    invariants:
      - one_of:
        - ar
        - cm
        - am
        - af
    examples:
      - ar
  list:
    type: string
    description: |
        The name of the list being voted on. There are 4 special names on this column:
            * `inscritos`: the number of elegible voters
            * `votantes`: the number of casted votes
            * `brancos`: the number of blank votes
            * `nulos`: the number of null votes
        Every other name correspond to the number
    examples:
      - B.E.
      - CH
      - inscritos
  count:
    type: integer
    description: The number of votes/voters for the list casted on `election`, `territory_type`, `territory` and `list`.
  mandates:
    type: integer
    description: |
      The number of assigned mandates according to whatever method is used to assign mandates used in the election.
      This is the total number of mandates for list `inscritos`.
  percentage:
    type: float
    description: The effective percentage obtained by each list (i.e. after discounting nulls and blanks).
invariants:
  - type: uniqueness
    columns: [election, territory_type, territory, organ_type, list]
    description: for each election, for each territory_type, for each territory, for each organ_type and for each list, there is exactly one row - the outcomes of casted votes
```

## How to use

You can download the data directly from

> https://fsn1.your-objectstorage.com/pt-elections/processed/v1/all.csv

and use your favourite spreadsheet tool.

Here we describe how to use it via SQL (e.g. [duckdb](https://duckdb.org/)),
which you can adapt for your tool of choice.

### Examples

#### Oficial results for Eleicoes Autarquicas 2025

One of the simplest queries is to analyise the results of one "freguesia" in the Eleicoes Autarquicas de 2025.
The SQL below

```sql
SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

-- same as https://www.autarquicas2025.mai.gov.pt/resultados/territorio-nacional?local=2429&election=AF
SELECT
    *
FROM
    read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
WHERE
election = 'autarquicas(2025)'
AND territory_type = 'freguesia'
AND territory = 'Golegã'
AND organ_type = 'af'
```

represents the voters registered in "freguesia" "Golegã" and how they voted for the Assembleia de Freguesia (af).

#### Oficial results for Eleicoes Legislativas 2025

The SQL below

```sql
SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

-- https://www.eleicoes.mai.gov.pt/legislativas2025/resultados/estrangeiro?local=FOREIGN-800000
SELECT
    *
FROM read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
WHERE election = 'legislativas(2025)'
AND territory = 'Europa'
```

represents the voters registered in Europa and how they voted for their electoral circle (Europa) in legislativas 2025.

#### Other examples

You can find these and other examples at [./examples](./examples/).
