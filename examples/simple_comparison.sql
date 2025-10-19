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
