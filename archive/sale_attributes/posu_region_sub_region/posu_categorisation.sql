--populate first table
SELECT DISTINCT
       prs.country,
       prs.division,
       prs.city,
       prs.posu_sub_region,
       prs.posu_region

FROM data_vault_mvp.chiasma_sql_server_snapshots.posu_regions_snapshot prs
         LEFT JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss ON prs.posu_region_id = dss.posu_region_id
WHERE prs.country IS NOT NULL
  AND prs.country != '~'
--   AND dss.key_product != 3;

DROP TABLE raw_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation;

dataset_task --include 'cms_mongodb.booking_summary' --operation ProductionIngestOperation --method 'run' --upstream --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'

--extract gsheet to s3
dataset_task \
--include 'fpa_gsheets.posu_categorisations*' \
--operation ExtractOperation \
--method 'run' \
--start '2020-08-20 00:00:00' \
--end '2020-08-20 00:00:00'

--load s3 to transient table
dataset_task \
--include 'fpa_gsheets.posu_categorisations*' \
--operation RawIngestOperation \
--method 'run' \
--start '2020-08-20 00:00:00' \
--end '2020-08-20 00:00:00'

--load transient table to raw_vault table
dataset_task \
--include 'fpa_gsheets.posu_categorisations*' \
--operation ProductionIngestOperation \
--method 'run' \
--start '2020-08-20 00:00:00' \
--end '2020-08-20 00:00:00'


SELECT *
FROM raw_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation


------------------------------------------------------------------------------------------------------------------------

CREATE TABLE fpa_gsheets.posu_categorisation
(
    dataset_name      VARCHAR      NOT NULL,
    dataset_source    VARCHAR      NOT NULL,
    schedule_interval VARCHAR      NOT NULL,
    schedule_tstamp   TIMESTAMPNTZ NOT NULL,
    run_tstamp        TIMESTAMPNTZ NOT NULL,
    loaded_at         TIMESTAMPNTZ NOT NULL,
    filename          VARCHAR      NOT NULL,
    file_row_number   NUMBER       NOT NULL,
    extract_metadata  VARIANT,
    country           VARCHAR,
    division          VARCHAR,
    city              VARCHAR,
    posu_sub_region   VARCHAR,
    posu_region       VARCHAR,
    cluster           VARCHAR,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));

DROP TABLE hygiene_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation;



SELECT *
FROM raw_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc
WHERE pc.country IS NULL;
SELECT *
FROM hygiene_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc
WHERE pc.posu_categorisation_id IS NULL;

self_describing_task --include 'staging/hygiene/fpa_gsheets/pc.py'  --method 'run' --start '2020-08-19 00:00:00' --end '2020-08-19 00:00:00'
/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
staging/
hygiene/
fpa_gsheets/
pc.py

SELECT *
FROM hygiene_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation;

self_describing_task --include 'staging/hygiene_snapshots/fpa_gsheets/pc.py'  --method 'run' --start '2020-08-19 00:00:00' --end '2020-08-19 00:00:00'
SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc;


WITH sale_posu_list AS (
    SELECT ss.se_sale_id,
           ss.posu_country,
           ss.posu_division,
           ss.posu_city
    FROM data_vault_mvp_dev_robin.dwh.se_sale ss
    WHERE ss.class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale' --remove tb sales

    UNION ALL

    SELECT t.se_sale_id,
           t.posu_country,
           t.posu_division,
           t.posu_city
    FROM data_vault_mvp_dev_robin.dwh.tb_offer t
         -- currently have some offers in tb that don't have a se sale id
    WHERE se_sale_id IS NOT NULL
          -- currently have some offers in tb that have the same sale id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY se_sale_id ORDER BY updated_at DESC) = 1
),
     first_country_division AS (
         --return the last posu sub region, region and cluster for country, division
         SELECT DISTINCT
                pc.country,
                pc.division,
                LAST_VALUE(pc.posu_categorisation_id)
                           OVER (PARTITION BY pc.country, pc.division ORDER BY pc.row_file_row_number) AS posu_categorisation_id,
                LAST_VALUE(pc.posu_sub_region)
                           OVER (PARTITION BY pc.country, pc.division ORDER BY pc.row_file_row_number) AS posu_sub_region,
                LAST_VALUE(pc.posu_region)
                           OVER (PARTITION BY pc.country, pc.division ORDER BY pc.row_file_row_number) AS posu_region,
                LAST_VALUE(pc.posu_cluster)
                           OVER (PARTITION BY pc.country, pc.division ORDER BY pc.row_file_row_number) AS posu_cluster
         FROM hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc
     ),

     first_country AS (
         --return the last posu sub region, region and cluster for country
         SELECT DISTINCT
                pc.country,
                LAST_VALUE(pc.posu_categorisation_id)
                           OVER (PARTITION BY pc.country ORDER BY pc.row_file_row_number) AS posu_categorisation_id,
                LAST_VALUE(pc.posu_sub_region)
                           OVER (PARTITION BY pc.country ORDER BY pc.row_file_row_number) AS posu_sub_region,
                LAST_VALUE(pc.posu_region)
                           OVER (PARTITION BY pc.country ORDER BY pc.row_file_row_number) AS posu_region,
                LAST_VALUE(pc.posu_cluster)
                           OVER (PARTITION BY pc.country ORDER BY pc.row_file_row_number) AS posu_cluster
         FROM hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc
     )

SELECT spl.se_sale_id,
       spl.posu_country,
       spl.posu_division,
       spl.posu_city,
       COALESCE(pc1.posu_categorisation_id, pc2.posu_categorisation_id, pc3.posu_categorisation_id) AS posu_categorisation_id,
       COALESCE(pc1.posu_sub_region, pc2.posu_sub_region, pc3.posu_sub_region)                      AS posu_sub_region,
       COALESCE(pc1.posu_region, pc2.posu_region, pc3.posu_region)                                  AS posu_region,
       COALESCE(pc1.posu_cluster, pc2.posu_cluster, pc3.posu_cluster)                               AS posu_cluster
FROM sale_posu_list spl
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc1
                   ON spl.posu_country = pc1.country
                       AND spl.posu_division = pc1.division
                       AND spl.posu_city = pc1.city
         LEFT JOIN first_country_division pc2
                   ON spl.posu_country = pc2.country
                       AND spl.posu_division = pc2.division
         LEFT JOIN first_country pc3
                   ON spl.posu_country = pc3.country;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
WHERE ss.class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale';

self_describing_task --include 'task_catalogue/dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

self_describing_task --include 'se/data/se_sale_attributes.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc
                   ON ss.posu_categorisation_id = pc.posu_categorisation_id
    self_describing_task --include 'task_catalogue/dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_offer t
WHERE t.posu_country IS NOT NULL;

SELECT *
FROM se_dev_robin.data.se_sale_attributes ssa;


self_describing_task --include 'task_catalogue/dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
WHERE ss.product_configuration = 'Catalogue';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_offer t
         self_describing_task --include 'se/data/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

         self_describing_task --include 'se/data/dim_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data.dim_sale ds
WHERE ds.sale_active;

------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT ds.posu_country, ds.posu_division, ds.posu_city, ds.posu_sub_region, ds.posu_region, ds.posu_cluster
FROM se_dev_robin.data.dim_sale ds;


SELECT country, division, count(*)
FROM hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation
GROUP BY 1, 2
HAVING count(*) > 1;

SELECT *
FROM hygiene_vault_mvp.fpa_gsheets.posu_categorisationyou;
SELECT *
FROM hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation;



SELECT *
FROM se_dev_robin.data.se_sale_attributes ssa;

--create a view for FPA to query the posu categorisation deduped version
--lower case on comparison to posu categorisation table
--add sheet for sale posu that don't map to any posu region, sub region, cluster

SELECT DISTINCT
       ssa.posu_country,
       ssa.posu_division,
       ssa.posu_city
FROM se_dev_robin.data.se_sale_attributes ssa
WHERE ssa.posu_sub_region = 'Other'
  AND ssa.posu_region = 'Other'
  AND ssa.posu_cluster = 'Other'
  AND ssa.posu_country IS NOT NULL;

CREATE OR REPLACE VIEW collab.fpa.posu_categorisation_gsheet COPY GRANTS AS
(
--raw extract of the sheet
SELECT pc.row_file_row_number,
       pc.posu_categorisation_id,
       pc.country,
       pc.division,
       pc.city,
       pc.posu_sub_region,
       pc.posu_region,
       pc.posu_cluster,
       pc.failed_some_validation,
       pc.fails_validation__posu_categorisation_id__expected_nonnull,
       pc.fails_validation__country__expected_nonnull,
       pc.fails_validation__posu_sub_region__expected_nonnull,
       pc.fails_validation__posu_region__expected_nonnull,
       pc.fails_validation__posu_cluster__expected_nonnull
FROM hygiene_vault_mvp.fpa_gsheets.posu_categorisation pc
    );

CREATE OR REPLACE VIEW collab.fpa.posu_categorisation_dedupe_gsheet COPY GRANTS AS
(
--deduped version of sheet
SELECT pc.row_file_row_number,
       pc.posu_categorisation_id,
       pc.country,
       pc.division,
       pc.city,
       pc.posu_sub_region,
       pc.posu_region,
       pc.posu_cluster
FROM hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation pc
    );

GRANT SELECT ON VIEW collab.fpa.posu_categorisation_gsheet TO ROLE personal_role__samanthamandeldallal;
GRANT SELECT ON VIEW collab.fpa.posu_categorisation_gsheet TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON VIEW collab.fpa.posu_categorisation_gsheet TO ROLE personal_role__janhitzke;

GRANT SELECT ON VIEW collab.fpa.posu_categorisation_dedupe_gsheet TO ROLE personal_role__samanthamandeldallal;
GRANT SELECT ON VIEW collab.fpa.posu_categorisation_dedupe_gsheet TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON VIEW collab.fpa.posu_categorisation_dedupe_gsheet TO ROLE personal_role__janhitzke;


