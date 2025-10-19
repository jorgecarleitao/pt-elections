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
