# elastic-schema-tools
Quick and dirty repository of tools used to work with ECS (Elastic) schema data

### ECS Schema toolkit
- `ecs-url-to-csv.py` is a script to download the latest version of the [all fields csv file](https://github.com/elastic/ecs/blob/master/generated/csv/fields.csv).
- `ecs-csv-to-sql.py` is a config script to generate the DDL (as postgres 17+ syntax) for a lookup table containing the current schema
- `ecs.csv` is the current schema as described in the [all fields csv file](https://github.com/elastic/ecs/blob/master/generated/csv/fields.csv).


#### NOTE:
It's not perfect. These are a few scripts I came up with while tinkering with ideas for logging. 
They do generate clean SQL, generally type/size appropriate for a single Postgresql table. Tested only on 17+.

The output of `ecs-csv-to-sql.py` should go to `BASE_DIR/data/Elastic/DDL-ecs.sql`, and will also prefill the entire table with the current known/available values.
When updating, the old csv is rotated to `ecs.MAJOR.MINOR.RELEASE-XYZ.csv` and the new one is placed in `BASE_DIR/data/Elastic/ecs.csv`.
