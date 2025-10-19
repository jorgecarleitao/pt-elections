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
