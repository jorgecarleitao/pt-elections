SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

WITH raw_data AS (
    SELECT
    CASE
        WHEN ends_with(filename, '4.json') THEN 'cm'
        WHEN ends_with(filename, '5.json') THEN 'am'
        WHEN ends_with(filename, '6.json') THEN 'af'
    END AS organ_type,
    CASE
        WHEN data.territoryTypeId == 2 THEN 'país'
        WHEN data.territoryTypeId == 5 THEN 'distrito'
        WHEN data.territoryTypeId == 6 THEN 'concelho'
        WHEN data.territoryTypeId == 7 THEN 'freguesia'
    END AS territory_type,
    data.*,
FROM
    read_json("s3://pt-elections/raw/autarquicas2025/*/*.json", union_by_name=True, filename=True)
)
SELECT
    territory_type AS 'tipo_territorio',
    territoryName AS 'territorio',
    organ_type AS 'orgão',
    unnest(
        list_concat(
            list_transform(currentResults.resultsParty, lambda x: {'lista(2025)': x['acronym'], 'votos(2025)': x['votes']}),
            [
                {'lista(2025)': 'inscritos', 'votos(2025)': currentResults.subscribedVoters},
                {'lista(2025)': 'votantes', 'votos(2025)': currentResults.totalVoters},
                {'lista(2025)': 'brancos', 'votos(2025)': currentResults.blankVotes},
                {'lista(2025)': 'nulos', 'votos(2025)': currentResults.nullVotes}
            ]
        ),
        recursive := true
    ),
    unnest(
        list_concat(
            list_transform(previousResults.resultsParty, lambda x: {'lista(2021)': x['acronym'], 'votos(2021)': x['votes']}),
            [
                {'lista(2021)': 'inscritos', 'votos(2021)': previousResults.subscribedVoters},
                {'lista(2021)': 'votantes', 'votos(2021)': previousResults.totalVoters},
                {'lista(2021)': 'brancos', 'votos(2021)': previousResults.blankVotes},
                {'lista(2021)': 'nulos', 'votos(2021)': previousResults.nullVotes}
            ]
        ),
        recursive := true
    ),
FROM raw_data
