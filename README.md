# elastic-schema-tools
Quick and dirty repository of tools used to work with ECS (Elastic) schema data

### ECS Schema toolkit
- `ecs-url-to-csv.py` is a script to download the latest version of the [all fields csv file](https://github.com/elastic/ecs/blob/master/generated/csv/fields.csv).
- `ecs-csv-to-sql.py` is a config script to generate the DDL (as postgres 17+ syntax) for a lookup table containing the current schema
- `ecs.csv` is the current schema as described in the [all fields csv file](https://github.com/elastic/ecs/blob/master/generated/csv/fields.csv).
