SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

SELECT
    count(*) AS entries,
    count(distinct election) AS distinct_elections,
    count(distinct territory_type) AS distinct_territory_types,
    count(distinct territory) AS distinct_territories
FROM read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
