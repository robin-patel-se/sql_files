CREATE OR REPLACE TABLE collab.hackday_posa_destination_mapping.country_long_lat
(
    country_code VARCHAR PRIMARY KEY NOT NULL,
    latitude     DECIMAL(13, 6),
    longitude    DECIMAL(13, 6),
    country_name VARCHAR
);


USE SCHEMA collab.hackday_posa_destination_mapping;
PUT 'file:///Users/robin/myrepos/sql_files/travel_distance_hack_day/country_long_lat.csv' @%country_long_lat;

COPY INTO collab.hackday_posa_destination_mapping.country_long_lat
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );


SELECT *
FROM collab.hackday_posa_destination_mapping.country_long_lat;

GRANT SELECT ON TABLE collab.hackday_posa_destination_mapping.country_long_lat TO ROLE personal_role__kirstengrieve;


SELECT
    t.name,
    CASE t.name
        WHEN 'UK' THEN 'GB'
        WHEN 'Conde Nast UK' THEN 'GB'
        WHEN 'TB-BE_FR' THEN 'FR'
        WHEN 'TB-NL' THEN 'NL'
        WHEN 'Guardian - UK' THEN 'GB'
        WHEN 'TB-BE_NL' THEN 'NL'
        END,
    cll.country_name
FROM hygiene_snapshot_vault_mvp.cms_mysql.territory t
    LEFT JOIN collab.hackday_posa_destination_mapping.country_long_lat cll ON t.name = cll.country_code;


SELECT
    ssa.longitude,
    ssa.latitude,
    TRY_TO_GEOGRAPHY('POINT(' || ssa.longitude || ' ' || ssa.latitude || ')')
FROM se.data.se_sale_attributes ssa;


SELECT *
FROM collab.hackday_posa_destination_mapping.sale_distance_haul_type sdht;

USE ROLE tableau;



SELECT DISTINCT
    ssa.posu_country
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
  AND ssa.posa_territory = 'DE'
  AND ssa.posu_cluster_region = 'UK';


SELECT *
FROM latest_vault.iterable.campaign c;


SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time >= CURRENT_DATE - 1
  AND s.warehouse_name LIKE '%DBT%'
  AND s.query_text IS NULL;


SELECT * FROm dbt.information_schema.schemata s
WHERE s.schema_name NOT LIKE '%__INTERMEDIATE';

