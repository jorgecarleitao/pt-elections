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
