SET s3_endpoint='fsn1.your-objectstorage.com';
SET s3_region='fsn1';

-- processed for legislativas 2025
COPY (
    SELECT
        territoryName AS territory,
        CASE
            WHEN ends_with(parse_filename(filename), 'LOCAL-500000.json') THEN 'país'
            WHEN starts_with(parse_filename(filename), 'LOCAL') AND ends_with(parse_filename(filename), '0000.json') THEN 'distrito'
            WHEN starts_with(parse_filename(filename), 'LOCAL') AND ends_with(parse_filename(filename), '00.json') THEN 'concelho'
            WHEN starts_with(parse_filename(filename), 'LOCAL') THEN 'freguesia'
            WHEN ends_with(parse_filename(filename), 'FOREIGN-600000.json') THEN 'zona estrangeira'
            WHEN ends_with(parse_filename(filename), 'FOREIGN-800000.json') THEN 'zona estrangeira'
            WHEN ends_with(parse_filename(filename), 'FOREIGN-900000.json') THEN 'zona estrangeira'
            WHEN starts_with(parse_filename(filename), 'FOREIGN') AND ends_with(parse_filename(filename), '00.json') THEN 'zona estrangeira'
            WHEN starts_with(parse_filename(filename), 'FOREIGN') THEN 'posto consular'
        END AS territory_type,
        currentResults,
        previousResults
    FROM
        read_json("s3://pt-elections/raw/legislativas2025/*.json", union_by_name=True, filename=True)
) TO 's3://pt-elections/processed/legislativas2025/v1/all.json';

-- processed for autarquicas 2025
COPY (
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
        data.territoryName AS territory,
        data.currentResults,
        data.previousResults
    FROM
        read_json("s3://pt-elections/raw/autarquicas2025/*/*.json", union_by_name=True, filename=True)
) TO 's3://pt-elections/processed/autarquicas2025/v1/all.json';

COPY (
WITH autarquicas_2025 AS (
    SELECT
    'autarquicas(2025)' AS election,
    territory_type,
    territory,
    organ_type,
    unnest(
        list_concat(
            list_transform(currentResults.resultsParty, lambda x: {'list': x['acronym'], 'count': x['votes'], 'percentage': x['percentage'], 'mandates': x['mandates']}),
            [
                {'list': 'inscritos', 'count': currentResults.subscribedVoters, 'percentage': 100, 'mandates': currentResults.totalMandates},
                {'list': 'votantes', 'count': currentResults.totalVoters, 'percentage': currentResults.percentageVoters, 'mandates': 0},
                {'list': 'brancos', 'count': currentResults.blankVotes, 'percentage': currentResults.blankVotesPercentage, 'mandates': 0},
                {'list': 'nulos', 'count': currentResults.nullVotes, 'percentage': currentResults.nullVotesPercentage, 'mandates': 0}
            ]
        ),
        recursive := true
    )
    FROM read_json('s3://pt-elections/processed/autarquicas2025/v1/all.json')
)

, autarquicas_2021 AS (
    SELECT
    'autarquicas(2021)' AS election,
    territory_type,
    territory,
    organ_type,
    unnest(
        list_concat(
            list_transform(previousResults.resultsParty, lambda x: {'list': x['acronym'], 'count': x['votes'], 'percentage': x['percentage'], 'mandates': x['mandates']}),
            [
                {'list': 'inscritos', 'count': previousResults.subscribedVoters, 'percentage': 100, 'mandates': previousResults.totalMandates},
                {'list': 'votantes', 'count': previousResults.totalVoters, 'percentage': previousResults.percentageVoters, 'mandates': 0},
                {'list': 'brancos', 'count': previousResults.blankVotes, 'percentage': previousResults.blankVotesPercentage, 'mandates': 0},
                {'list': 'nulos', 'count': previousResults.nullVotes, 'percentage': previousResults.nullVotesPercentage, 'mandates': 0}
            ]
        ),
        recursive := true
    )
    FROM read_json('s3://pt-elections/processed/autarquicas2025/v1/all.json')
)

, legislativas2025 AS (
    SELECT
    'legislativas(2025)' AS election,
    territory_type,
    territory,
    'ar' AS organ_type,
    unnest(
        list_concat(
            list_transform(currentResults.resultsParty, lambda x: {'list': x['acronym'], 'count': x['votes'], 'percentage': x['percentage'], 'mandates': x['mandates']}),
            [
                {'list': 'inscritos', 'count': currentResults.subscribedVoters, 'percentage': 100, 'mandates': currentResults.totalMandates},
                {'list': 'votantes', 'count': currentResults.totalVoters, 'percentage': currentResults.percentageVoters, 'mandates': 0},
                {'list': 'brancos', 'count': currentResults.blankVotes, 'percentage': currentResults.blankVotesPercentage, 'mandates': 0},
                {'list': 'nulos', 'count': currentResults.nullVotes, 'percentage': currentResults.nullVotesPercentage, 'mandates': 0}
            ]
        ),
        recursive := true
    )
    FROM read_json('s3://pt-elections/processed/legislativas2025/v1/all.json')
)

, legislativas2024 AS (
    SELECT
    'legislativas(2024)' AS election,
    territory_type,
    territory,
    'ar' AS organ_type,
    unnest(
        list_concat(
            list_transform(previousResults.resultsParty, lambda x: {'list': x['acronym'], 'count': x['votes'], 'percentage': x['percentage'], 'mandates': x['mandates']}),
            [
                {'list': 'inscritos', 'count': previousResults.subscribedVoters, 'percentage': 100, 'mandates': previousResults.totalMandates},
                {'list': 'votantes', 'count': previousResults.totalVoters, 'percentage': previousResults.percentageVoters, 'mandates': 0},
                {'list': 'brancos', 'count': previousResults.blankVotes, 'percentage': previousResults.blankVotesPercentage, 'mandates': 0},
                {'list': 'nulos', 'count': previousResults.nullVotes, 'percentage': previousResults.nullVotesPercentage, 'mandates': 0}
            ]
        ),
        recursive := true
    )
    FROM read_json('s3://pt-elections/processed/legislativas2025/v1/all.json')
)

SELECT * FROM autarquicas_2025
UNION ALL 
SELECT * FROM autarquicas_2021
UNION ALL
SELECT * FROM legislativas2024
UNION ALL
SELECT * FROM legislativas2025
) TO 's3://pt-elections/processed/v1/all.csv';
