-- View: ecs.schema_versions
-- DROP MATERIALIZED VIEW IF EXISTS ecs.schema_versions;
CREATE MATERIALIZED VIEW IF NOT EXISTS ecs.schema_versions
AS
 SELECT DISTINCT "ECS_Version" AS version,
    split_part(split_part("ECS_Version"::text, '-'::text, 1), '.'::text, 1) AS major,
    split_part(split_part("ECS_Version"::text, '-'::text, 1), '.'::text, 2) AS minor,
    split_part(split_part("ECS_Version"::text, '-'::text, 1), '.'::text, 3) AS build,
    NULLIF(split_part("ECS_Version"::text, '-'::text, 2), ''::text) AS release
   FROM ecs.elastic_log_schema
WITH NO DATA;

ALTER TABLE IF EXISTS ecs.schema_versions
    OWNER TO postgres;

COMMENT ON MATERIALIZED VIEW ecs.schema_versions
    IS 'View of the ECS schema version(s) currently available';

-- Ensure the views pegged to this table are refreshed and available autmatically
REFRESH MATERIALIZED VIEW ecs.schema_versions;


-- View: ecs.schema_versions_fields
-- DROP MATERIALIZED VIEW IF EXISTS ecs.schema_versions_fields;
CREATE MATERIALIZED VIEW IF NOT EXISTS ecs.schema_versions_fields
AS
 SELECT sv.version,
    sv.major,
    sv.minor,
    sv.build,
    sv.release,
    els."Indexed",
    els."Field_Set",
    els."Field",
    els."Type",
    els."Level",
    els."Normalization"
   FROM ecs.elastic_log_schema els
     JOIN ecs.schema_versions sv ON els."ECS_Version" = sv.version
WITH NO DATA;

ALTER TABLE IF EXISTS ecs.schema_versions_fields
    OWNER TO postgres;

COMMENT ON MATERIALIZED VIEW ecs.schema_versions_fields
    IS 'Essential characteristics of fields available by given version of a loaded ECS schema, excluding descriptions and examples';

-- Ensure the views pegged to this table are refreshed and available autmatically
REFRESH MATERIALIZED VIEW ecs.schema_versions_fields;


-- View: ecs.schema_versions_levels
-- DROP MATERIALIZED VIEW IF EXISTS ecs.schema_versions_levels;
CREATE MATERIALIZED VIEW IF NOT EXISTS ecs.schema_versions_levels
AS
 SELECT sv.version,
    sv.major,
    sv.minor,
    sv.build,
    sv.release,
    els."Field_Set",
    els."Level",
    els."Normalization",
    els."Type",
    count(els."Field") AS "Count",
    count(els_i."Field") AS "Indexed"
   FROM ecs.elastic_log_schema els
     JOIN ecs.schema_versions sv ON els."ECS_Version" = sv.version
     JOIN ecs.elastic_log_schema els_i ON els_i."ECS_Version" = sv.version AND els."Field_Set" = els_i."Field_Set" AND els."Field" = els_i."Field"
  GROUP BY sv.version, sv.major, sv.minor, sv.build, sv.release, els."Level", els."Field_Set", els."Type", els."Normalization"
WITH NO DATA;

ALTER TABLE IF EXISTS ecs.schema_versions_levels
    OWNER TO postgres;

COMMENT ON MATERIALIZED VIEW ecs.schema_versions_levels
    IS 'Quick overview of the various levels, types, and normalizations available by fields set for the loaded ECS schema data';

-- Ensure the views pegged to this table are refreshed and available autmatically
REFRESH MATERIALIZED VIEW ecs.schema_versions_levels;
