SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

-- https://www.eleicoes.mai.gov.pt/legislativas2025/resultados/estrangeiro?local=FOREIGN-800000
SELECT
    *
FROM read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
WHERE election = 'legislativas(2025)'
AND territory = 'Europa'
