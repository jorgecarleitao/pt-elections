SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

WITH legislativas_2025 AS (
    SELECT
        *
    FROM read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
    WHERE election = 'legislativas(2025)'
)

, legislativas_2024 AS (
    SELECT
        *
    FROM read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
    WHERE election = 'legislativas(2024)'
)

, autarquicas_2021 AS (
    SELECT
        *
    FROM read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
    WHERE election = 'autarquicas(2021)'
    AND organ_type = 'af'
)

, autarquicas_2025 AS (
    SELECT
        *
    FROM read_csv_auto('s3://pt-elections/processed/v1/all.csv', header=True)
    WHERE election = 'autarquicas(2025)'
    AND organ_type = 'af'
)

SELECT
    coalesce(legislativas_2025.list, legislativas_2024.list, autarquicas_2025.list, autarquicas_2021.list) AS lista,
    autarquicas_2025.count AS 'autarquicas 2025',
    legislativas_2025.count AS 'legislativas 2025',
    legislativas_2024.count AS 'legislativas 2024',
    autarquicas_2021.count AS 'autarquicas 2021',
    autarquicas_2025.percentage AS 'autarquicas 2025 (%)',
    legislativas_2025.percentage AS 'legislativas 2025 (%)',
    legislativas_2024.percentage AS 'legislativas 2024 (%)',
    autarquicas_2021.percentage AS 'autarquicas 2021 (%)',
FROM legislativas_2025
FULL OUTER JOIN legislativas_2024 USING (territory, territory_type, list)
FULL OUTER JOIN autarquicas_2025 USING (territory, territory_type, list)
FULL OUTER JOIN autarquicas_2021 USING (territory, territory_type, list)
WHERE
    coalesce(autarquicas_2025.territory_type, legislativas_2025.territory_type, legislativas_2024.territory_type, autarquicas_2021.territory_type) = 'freguesia'
    AND coalesce(autarquicas_2025.territory, legislativas_2025.territory, legislativas_2024.territory, autarquicas_2021.territory) = 'Venteira'
    AND coalesce(autarquicas_2025.count, legislativas_2025.count, legislativas_2024.count, autarquicas_2021.count) > 200
ORDER BY coalesce(autarquicas_2025.count, legislativas_2025.count, legislativas_2024.count, autarquicas_2021.count) DESC
