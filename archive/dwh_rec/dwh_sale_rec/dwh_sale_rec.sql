USE WAREHOUSE pipe_xlarge;

SELECT data_model,
       COUNT(DISTINCT sale_id)
FROM data_vault_mvp.dwh.se_sale
GROUP BY 1;

SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale;

SELECT MIN(updated_at)
FROM hygiene_vault_mvp.cms_mysql.sale; --2020-02-28 09:44:33.925000000
SELECT MIN(updated_at)
FROM hygiene_vault_mvp.cms_mysql.sale_flight_config; --2020-02-28 09:44:32.448000000
SELECT MIN(updated_at)
FROM hygiene_vault_mvp.cms_mysql.base_sale; --2020-02-28 11:18:05.163000000
SELECT MIN(updated_at)
FROM hygiene_vault_mvp.cms_mysql.hotel; --2020-02-28 09:44:57.488000000

SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale; --2020-02-28 17:44:42.470000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config; --2020-03-10 10:18:41.473000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale; --2020-02-28 16:40:06.933000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.hotel; --2020-03-10 10:15:56.543000000


airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly
airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' dwh__transactional__sale__hourly


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config
WHERE sale_id = 105488;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.base_sale CLONE raw_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.sale CLONE raw_vault_mvp.cms_mysql.sale;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.hotel CLONE raw_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE raw_vault_mvp.cms_mysql.sale_flight_config;


CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;

airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' incoming__cms_mysql__base_sale__hourly
airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' incoming__cms_mysql__sale__hourly
airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' incoming__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' incoming__cms_mysql__sale_flight_config__hourly
airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' incoming__travelbird_mysql__offers_offer__hourly

airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly
airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__hourly
airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly
airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale_flight_config__hourly

airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offer__hourly

airflow backfill --start_date '2020-03-20 00:00:00' --end_date '2020-03-20 00:00:00' --task_regex '.*' dwh__transactional__sale__hourly


DROP TABLE data_vault_mvp_dev_robin.dwh.se_sale;

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale
WHERE sale_id = '105488';

self_describing_task --include 'dv/dwh_rec/transactional/se_sale'  --method 'run' --start '2020-03-10 00:00:00' --end '2020-03-10 00:00:00'
self_describing_task --include 'dv/dwh_rec/transactional/se_sale'  --method 'run' --start '2020-03-10 00:00:00' --end '2020-03-10 00:00:00'

SELECT COUNT(sale_id)
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
SELECT COUNT(id)
FROM hygiene_snapshot_vault_mvp.cms_mysql.hotel;
SELECT COUNT(sale_id)
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale;
SELECT COUNT(DISTINCT id)
FROM raw_vault_mvp.cms_mysql.base_sale;

airflow backfill --start_date '1970-01-01 00:00:00' --end_date '1970-01-01 00:00:00' --task_regex '.*' incoming__cms_mysql__base_sale__hourly
SELECT MIN(loaded_at)
FROM raw_vault_mvp.cms_mysql.base_sale; --2020-02-26 14:13:15.993836000

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' hygiene__cms_mysql__base_sale__hourly
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale; --2020-03-12 13:44:04.709000000

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__base_sale__hourly
--base sale done.

SELECT MIN(loaded_at)
FROM raw_vault_mvp.cms_mysql.hotel; --2019-12-03 14:34:22.228403000

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' hygiene__cms_mysql__hotel__hourly
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.hotel; --2020-03-12 13:44:04.709000000

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__hotel__hourly
--hotel done

SELECT MIN(loaded_at)
FROM raw_vault_mvp.cms_mysql.sale; --2019-12-16 14:42:40.907454000

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' hygiene__cms_mysql__sale__hourly
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale; --2020-03-12 13:44:04.709000000

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__hourly
--sale done

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' dwh__transactional__sale__hourly

SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer; --2020-02-28 09:59:56.568000000


SELECT data_model, count(*)
FROM data_vault_mvp.dwh.se_sale
GROUP BY 1;

SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale;--8481
SELECT COUNT(DISTINCT sale_id)
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale;--8481
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.hotel; --234


SELECT COUNT(sale_id)
FROM data_vault_mvp.dwh.se_sale
WHERE data_model = 'New Model';--6810

SELECT COUNT(sale_id)
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale; --8481

SELECT COUNT(sale_id)
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale
WHERE class != 'com.flashsales.sale.ConnectedWebRedirectSale'; --6810

SELECT *
FROM se.data.dim_sale;

SELECT c.sale_id,
       c.product_type,
       c.product_configuration,
       c.product_line,
       c.data_model_version,
       c.tech_platform,
       dwh.sale_id,
       dwh.sale_product,
       dwh.sale_type,
       dwh.product_type,
       dwh.product_configuration,
       dwh.product_line,
       dwh.data_model,
       dwh.tech_platform
FROM collab.cube.covid_data_20200313 c
         INNER JOIN se.data.dim_sale dwh ON c.sale_id = dwh.sale_id;

SELECT product_configuration,
       product_type,
       count(*)
FROM se.data.dim_sale
GROUP BY 1, 2
;

SELECT COUNT(*), count(DISTINCT sale_id)
FROM se.data.dim_sale;



SELECT d.*
FROM se.data.dim_sale d
         LEFT JOIN raw_vault_mvp.cms_reports.sales s ON s.id = d.sale_id
WHERE s.id IS NULL;



WITH cms_sale_id AS (
    SELECT s.id
    FROM raw_vault_mvp.cms_reports.sales s
        QUALIFY ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY s.loaded_at DESC) = 1
)
SELECT sale_id,
       sale_product,
       sale_type,
       product_type,
       product_configuration,
       product_line,
       data_model,
       tech_platform,
       id
FROM se.data.dim_sale d
         LEFT JOIN cms_sale_id c ON d.sale_id = c.id
WHERE d.tech_platform = 'SECRET_ESCAPES'
  AND c.id IS NULL;


SELECT *
FROM se.data.dim_sale dwh
         LEFT JOIN collab.cube.covid_data_20200313 c ON dwh.sale_id = c.sale_id
WHERE c.sale_id IS NULL;

SELECT *
FROM se.data.dim_sale
WHERE sale_id IN (
    SELECT sale_id
    FROM se.data.dim_sale
    GROUP BY 1
    HAVING COUNT(*) > 1)
ORDER BY sale_id;

--327 dupe sale ids
--vast majority are old model sales that look like we aren't excluding TB sales from old model.


--reconcile the non dupes
SELECT s.sale_id,
       s.product_type,
       c.product_type,
       s.product_configuration,
       c.product_configuration,
       s.product_line,
       c.product_line,
       s.data_model,
       c.data_model_version,
       s.tech_platform,
       c.tech_platform
FROM se.data.dim_sale s
         LEFT JOIN collab.cube.covid_data_20200313 c ON s.sale_id = c.sale_id
WHERE s.sale_id NOT IN (
    SELECT sale_id
    FROM se.data.dim_sale
    GROUP BY 1
    HAVING COUNT(*) > 1);

SELECT s.sale_id,
       s.product_type,
       c.product_type,
       s.product_configuration,
       c.product_configuration,
       s.product_line,
       c.product_line,
       s.data_model,
       c.data_model_version,
       s.tech_platform,
       c.tech_platform
FROM se.data.dim_sale s
         LEFT JOIN collab.cube.covid_data_20200313 c ON s.sale_id = c.sale_id
WHERE s.sale_id NOT IN (--remove temp dupes
    SELECT sale_id
    FROM se.data.dim_sale
    GROUP BY 1
    HAVING COUNT(*) > 1)
  AND s.product_type != c.product_type;

SELECT *
FROM data_vault_mvp.dwh.se_sale
WHERE sale_id = '3259';

SELECT s.se_sale_id,
       e.event_tstamp
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON s.event_hash = e.event_hash
WHERE parse_url(e.page_url, 1)['host'] LIKE '%travelist%'
  AND e.event_tstamp >= '2020-02-28'
LIMIT 100;

airflow backfill --start_date '2020-03-18 00:00:00' --end_date '2020-03-18 00:00:00' --task_regex '.*' dwh__transactional__sale__hourly

SELECT *
FROM collab.cube.covid_data_20200313
WHERE sale_id = '2781';
SELECT *
FROM se.data.dim_sale
WHERE sale_id = '2781';

--all travelist traffic is to their own sale id


SELECT DISTINCT s.se_sale_id
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON s.event_hash = e.event_hash
WHERE parse_url(e.page_url, 1)['host'] LIKE '%travelist%'
  AND e.event_tstamp >= '2020-02-28'
  AND s.se_sale_id NOT IN (SELECT DISTINCT se_sale_id FROM data_vault_mvp.dwh.tb_offer);

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

--cube query
-- SELECT DISTINCT ds.[sale_id]
--               , dp.[product]                                                       AS [product_type]
--               , dst.[sale_type]                                                    AS [product_configuration]
--               , CASE WHEN ds.[key_sale_type] = 9 THEN 'Catalogue' ELSE 'Flash' END AS [product_line]
--               , CASE
--                     WHEN ds.[sale_model_version] = 0
--                         THEN 'Old Data Model'
--                     WHEN ds.[sale_model_version] = 1
--                         THEN 'New Data Model' END                                  AS [data_model_version]
--               , CASE
--                     WHEN ds.[source_id] = 3 THEN 'Travelist'
--                     WHEN ds.[source_id] = 1 THEN 'SE Core' END                     AS [CMS]
--               , CASE
--                     WHEN ds.[source_id] = 3
--                         THEN 'Travelist'
--                     WHEN ds.[source_id] = 1 AND ds.[provider_name] = 'Travelbird'
--                         THEN 'Travelbird'
--                     ELSE 'Secret Escapes Core' END                                 AS [tech_platform]
--               , ds.[provider_name]
-- FROM [dbo].[dim_sales] ds
--          LEFT JOIN [dbo].[dim_products] dp ON ds.[key_product] = dp.[key_product]
--          LEFT JOIN [dbo].[dim_sale_types] dst ON ds.[key_sale_type] = dst.[key_sale_type]

USE SCHEMA collab.dwh_rec;

CREATE OR REPLACE TABLE cube_sales
(
    sale_id               VARCHAR,
    product_type          VARCHAR,
    product_configuration VARCHAR,
    product_line          VARCHAR,
    data_model_version    VARCHAR,
    cms                   VARCHAR,
    tech_plaftorm         VARCHAR,
    provider_name         VARCHAR
);

PUT file:///Users/robin/sqls/dwh_sale_rec/cube_sales.csv @%CUBE_SALES;

COPY INTO collab.dwh_rec.cube_sales
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT CASE
           WHEN cms = 'Travelist' THEN 'TVL' || sale_id
           ELSE sale_id END AS sale_id,
       product_type,
       product_configuration,
       product_line,
       data_model_version,
       cms,
       tech_plaftorm,
       provider_name
FROM cube_sales
WHERE cms = 'Travelist'
;

CREATE OR REPLACE TABLE collab.dwh_rec.sale_dims AS (
    WITH cube_sale AS (
        SELECT CASE
                   WHEN cms = 'Travelist' THEN 'TVL' || sale_id
                   ELSE sale_id END AS sale_id,
               product_type,
               product_configuration,
               product_line,
               data_model_version,
               cms,
               tech_plaftorm,
               provider_name
        FROM cube_sales
    )
    SELECT s.sale_id,
           s.product_type                                         AS dwh_product_type,
           CASE
               WHEN c.product_type = 'Day' THEN 'Day Experience'
               WHEN c.product_type = 'Travel' THEN 'Package'
               ELSE c.product_type
               END                                                AS cube_product_type,
           dwh_product_type = cube_product_type                   AS p_type_match,
           s.product_configuration                                AS dwh_product_configuration,
           CASE
               WHEN c.product_configuration = 'Third Party Package' THEN '3PP'
               WHEN c.product_configuration = 'HotelPlus' THEN 'Hotel Plus'
               WHEN c.product_configuration = 'IHP - Static' THEN 'IHP - static'
               WHEN c.product_configuration = 'Hotel' THEN 'Hotel'
               WHEN c.product_configuration = 'WRD' THEN 'WRD'
               WHEN c.product_configuration = 'Catalogue' THEN 'Package'
               ELSE c.product_configuration
               END
                                                                  AS cube_product_configuration,
           dwh_product_configuration = cube_product_configuration AS p_conf_match,
           s.product_line                                         AS dwh_product_line,
           c.product_line                                         AS cube_product_line,
           dwh_product_line = cube_product_line                   AS p_line_match,
           s.data_model                                           AS dwh_data_model,
           c.data_model_version                                   AS cube_data_model_version,
           s.tech_platform                                        AS dwh_tech_platform,
           c.tech_plaftorm                                        AS cube_tech_platform
    FROM se_dev_robin.data.dim_sale s
             LEFT JOIN cube_sale c ON s.sale_id = c.sale_id
    ORDER BY sale_id
);
GRANT USAGE ON SCHEMA collab.dwh_rec TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON TABLE collab.dwh_rec.sale_dims TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON TABLE collab.dwh_rec.cube_sales TO ROLE personal_role__kirstengrieve;



SELECT *
FROM collab.dwh_rec.sale_dims
WHERE (p_type_match = FALSE
    OR p_conf_match = FALSE
    OR p_line_match = FALSE)
  AND cube_product_configuration IS DISTINCT FROM 'Not Specified';


SELECT cms_s.id,
       cms_s.type                                 AS sale_list_type,
       dwh_product_type,
       cube_product_type,


       CASE
           WHEN cms_s.sale_dimension = 'Third Party Package' THEN '3PP'
           WHEN cms_s.sale_dimension = 'HotelPlus' THEN 'Hotel Plus'
           WHEN cms_s.sale_dimension = 'IHP - Static' THEN 'IHP - static'
           WHEN cms_s.sale_dimension = 'Hotel' THEN 'Hotel'
           WHEN cms_s.sale_dimension = 'WRD' THEN 'WRD'
           WHEN cms_s.sale_dimension = 'Catalogue' THEN 'Package'
           ELSE cms_s.sale_dimension
           END                                    AS sale_list_dimension,
       dwh_product_configuration,
       sale_dimension = dwh_product_configuration AS p_conf_match,
       cube_product_configuration
FROM raw_vault_mvp.cms_reports.sales cms_s
         INNER JOIN (SELECT *
                     FROM collab.dwh_rec.sale_dims
                     WHERE (p_type_match = FALSE
                         OR p_conf_match = FALSE
                         OR p_line_match = FALSE)
                       AND cube_product_configuration IS DISTINCT FROM 'Not Specified') dwh_s
                    ON cms_s.id = dwh_s.sale_id
                        AND cms_s.schedule_tstamp::DATE = '2020-03-22';


GRANT USAGE ON SCHEMA collab.dwh_rec TO ROLE personal_role__kirstengrieve;

--se sales
SELECT ds.*,
       dwh.cube_product_configuration
FROM data_vault_mvp_dev_robin.dwh.se_sale ds
         INNER JOIN (SELECT *
                     FROM collab.dwh_rec.sale_dims
                     WHERE (p_type_match = FALSE
                         OR p_conf_match = FALSE
                         OR p_line_match = FALSE)
                       AND cube_product_configuration IS DISTINCT FROM 'Not Specified') dwh ON ds.sale_id = dwh.sale_id
;

--tb sales
SELECT ds.*,
       dwh.cube_product_configuration
FROM data_vault_mvp_dev_robin.dwh.tb_offer ds
         INNER JOIN (SELECT *
                     FROM collab.dwh_rec.sale_dims
                     WHERE (p_type_match = FALSE
                         OR p_conf_match = FALSE
                         OR p_line_match = FALSE)
                       AND cube_product_configuration IS DISTINCT FROM 'Not Specified') dwh
                    ON ds.se_sale_id = dwh.sale_id
;


SELECT is_team20package
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale;

SELECT sale_id,
       is_able_to_sell_flights AS sale_able_to_sell_flights,
       MAX(updated_at)         AS updated_at
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config
WHERE is_able_to_sell_flights = TRUE;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;
------------------------------------------------------------------------------------------------------------------------

TRUNCATE data_vault_mvp_dev_robin.dwh.tb_offer;


CREATE OR REPLACE TRANSIENT TABLE tb_offer__step01__get_source_batch AS
    (
        SELECT id,
               CASE
                   WHEN product_line = 'catalogue' AND site_id = 46
                       THEN 'TVL' || se_sale_id
                   ELSE se_sale_id END  AS se_sale_id,
               site_id,
               product_line             AS product_line__o,

               CASE
                   WHEN product_line = 'flash'
                       THEN 'PACKAGE'
                   WHEN product_line = 'catalogue' AND site_id != 46
                       THEN 'PACKAGE'
                   WHEN product_line = 'catalogue' AND site_id = 46
                       THEN 'HOTEL'
                   END                  AS sale_product, -- type

               CASE
                   WHEN product_line = 'flash'
                       THEN 'IHP - dynamic'
                   WHEN product_line = 'catalogue' AND site_id != 46
                       THEN 'Catalogue'
                   WHEN product_line = 'catalogue' AND site_id = 46
                       THEN 'Hotel'
                   END                  AS sale_type,    --sale_dimension

               CASE
                   WHEN product_line = 'flash'
                       THEN 'Package'
                   WHEN product_line = 'catalogue' AND site_id != 46
                       THEN 'Package'
                   WHEN product_line = 'catalogue' AND site_id = 46
                       THEN 'Hotel'
                   END                  AS product_type,

               CASE
                   WHEN product_line = 'flash'
                       THEN 'IHP - dynamic'
                   WHEN product_line = 'catalogue' AND site_id != 46
                       THEN 'Package'
                   WHEN product_line = 'catalogue' AND site_id = 46
                       THEN 'Hotel'
                   END                  AS product_configuration,

               CASE
                   WHEN product_line = 'flash'
                       THEN 'Flash'
                   WHEN product_line = 'catalogue' AND site_id != 46
                       THEN 'Catalogue'
                   WHEN product_line = 'catalogue' AND site_id = 46
                       THEN 'Flash'
                   END                  AS product_line,

               CASE
                   WHEN LEFT(se_sale_id, 1) = 'A'
                       THEN 'New Model'
                   ELSE 'Old Model' END AS data_model

        FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer
    );

MERGE INTO data_vault_mvp_dev_robin.dwh.tb_offer AS target
    USING tb_offer__step01__get_source_batch AS batch
    ON target.id = batch.id
    WHEN MATCHED
        THEN UPDATE SET
        target.schedule_tstamp = '2020-03-23',
        target.run_tstamp = '2020-03-23',
        target.operation_id = '2020-03-23',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,
        target.id = batch.id,
        target.se_sale_id = batch.se_sale_id,
        target.site_id = batch.site_id,
        target.product_line__o = batch.product_line__o,
        target.sale_product = batch.sale_product,
        target.sale_type = batch.sale_type,
        target.product_type = batch.product_type,
        target.product_configuration = batch.product_configuration,
        target.product_line = batch.product_line,
        target.data_model = batch.data_model
    WHEN NOT MATCHED
        THEN INSERT VALUES ('2020-03-23',
                            '2020-03-23',
                            '2020-03-23',
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            batch.id,
                            batch.se_sale_id,
                            batch.site_id,
                            batch.product_line__o,
                            batch.sale_product,
                            batch.sale_type,
                            batch.product_type,
                            batch.product_configuration,
                            batch.product_line,
                            batch.data_model);

SELECT DISTINCT product_configuration
FROM se_dev_robin.data.dim_sale;


DROP TABLE data_vault_mvp_dev_robin.dwh.se_sale;
DROP TABLE data_vault_mvp_dev_robin.dwh.tb_offer;
self_describing_task --include 'dv/dwh_rec/transactional/se_sale'  --method 'run' --start '2020-02-28 00:00:00' --end '2020-02-28 00:00:00'
self_describing_task --include 'dv/dwh_rec/transactional/tb_offer'  --method 'run' --start '2020-02-28 00:00:00' --end '2020-02-28 00:00:00'

--need to re-run this after the changes to sale dimensions
airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-25 00:00:00' --task_regex '.*' dwh__transactional__sale__hourly
airflow backfill --start_date '2020-03-25 09:00:00' --end_date '2020-03-25 09:00:00' --task_regex '.*' dwh__transactional__sale__hourly

SELECT *
FROM se.data.bookings_with_flights;

SELECT se_sale_id, COUNT(*)
FROM data_vault_mvp.dwh.tb_offer
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT sale_id, COUNT(*)
FROM se.data.dim_sale
GROUP BY 1
HAVING COUNT(*) > 1;

airflow backfill --start_date '2020-03-25 09:00:00' --end_date '2020-03-25 09:00:00' --task_regex '01_module_touched_spvs.py ' dwh__transactional__sale__hourly

USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------
--manually updating sale ids from travelist spvs

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
    USING (
        --SPVs from page views
        SELECT e.event_hash,
               t.touch_id,
               e.event_tstamp,
               CASE
                   WHEN
                       -- Travelist sales have conflicting sale ids so we prefix the sale id
                           parse_url(e.page_url, 1)['host']::VARCHAR LIKE '%travelist%' AND v_tracker LIKE 'py-%'
                       THEN 'TVL' || e.se_sale_id
                   ELSE e.se_sale_id END AS se_sale_id,
               'page views'              AS event_category,
               'SPV'                     AS event_subcategory
        FROM data_vault_mvp.single_customer_view_stg.module_touchification t
                 INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
        WHERE e.event_name = 'page_view'
          AND e.se_sale_id IS NOT NULL
          AND t.updated_at >= TIMESTAMPADD('day', -1, '2020-02-29 00:00:00'::TIMESTAMP)
          AND (--line in sand between client side and server side tracking
                (--client side tracking, prior implementation/validation
                        e.collector_tstamp < '2020-02-28 00:00:00'
                        AND (
                                e.page_urlpath LIKE '%/sale'
                                OR
                                e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                            -- need to adjust for new definitions of spv e.g. travel bird booking flow
                            )
                        AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                        AND e.is_server_side_event = FALSE -- exclude non validated ss events
                    )
                OR
                (--server side tracking, post implementation/validation
                        e.collector_tstamp >= '2020-02-28 00:00:00'
                        AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                        AND
                        e.device_platform != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                    --TODO need to adjust this for when TB product goes live on native app, we will need to count the wrapped mweb
                    --product
                        AND e.is_server_side_event = TRUE
                    )
            )
    ) AS batch ON target.event_hash = batch.event_hash
    WHEN MATCHED AND target.se_sale_id != batch.se_sale_id
        THEN UPDATE SET
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,
        target.se_sale_id = batch.se_sale_id
;


--check sale ids for travelist
SELECT parse_url(e.page_url, 1)['host']::VARCHAR,
       s.se_sale_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs s
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = s.event_hash
WHERE parse_url(e.page_url, 1)['host']::VARCHAR LIKE '%travelist%'
  AND v_tracker LIKE 'py-%';


USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------
--update TVL sale ids on bookings

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer_bkup CLONE hygiene_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer_bkup CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking_bkup CLONE data_vault_mvp.dwh.tb_booking;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer_update AS (
    SELECT schedule_tstamp,
           run_tstamp,
           operation_id,
           created_at,
           updated_at,
           row_dataset_name,
           row_dataset_source,
           row_loaded_at,
           row_schedule_tstamp,
           row_run_tstamp,
           row_filename,
           row_file_row_number,
           CASE
               WHEN site_id = 46 --travelist sale
                   THEN 'TVL' || external_reference
               ELSE external_reference END AS se_sale_id,
           id,
           voucher_release,
           thumbnail,
           thumbnail_options,
           home_image,
           home_image_options,
           title,
           short_title,
           banner_title,
           seo_title,
           descriptive_title,
           slug,
           internal_name,
           exclude_from_feeds,
           pub_date,
           category_date_start,
           category_date_end,
           end_date,
           active,
           in_use,
           hide_from_search,
           no_adwords,
           price_rounding,
           quantization,
           package_price_amount_of_adults,
           price,
           price_title,
           old_price,
           per_person_price,
           payment_option,
           payment_method_description,
           booking_fee,
           booking_fee_for_person,
           down_payment,
           disable_instalments,
           fixed_down_payment_fee,
           down_payment_for_person,
           offer_unit,
           place_description,
           included,
           excluded,
           excluded_short,
           features,
           details,
           editor_tip,
           editor_tip_picture,
           tags,
           priority,
           label,
           allow_multiple_units,
           participants_fields,
           customer_fields,
           birthdate_required,
           category_id,
           site_id,
           target_group_id,
           transportation_id,
           hero_id,
           hero_options,
           partner_id,
           external_reference,
           concept_id,
           package_price_per_night,
           product_line,
           fails_validation__id__expected_nonnull,
           fails_validation__site_id__expected_nonnull,
           fails_validation__product_line__expected_nonnull,
           failed_some_validation
    FROM hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer
);

DROP TABLE hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer;
ALTER TABLE hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer_update
    RENAME TO hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer;

SELECT *
FROM hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer
WHERE LEFT(se_sale_id, 3) = 'TVL';

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer_update AS (
    SELECT schedule_tstamp,
           run_tstamp,
           operation_id,
           created_at,
           updated_at,
           row_dataset_name,
           row_dataset_source,
           row_loaded_at,
           row_schedule_tstamp,
           row_run_tstamp,
           row_filename,
           row_file_row_number,
           CASE
               WHEN site_id = 46 --travelist sale
                   THEN 'TVL' || external_reference
               ELSE external_reference END AS se_sale_id,
           id,
           voucher_release,
           thumbnail,
           thumbnail_options,
           home_image,
           home_image_options,
           title,
           short_title,
           banner_title,
           seo_title,
           descriptive_title,
           slug,
           internal_name,
           exclude_from_feeds,
           pub_date,
           category_date_start,
           category_date_end,
           end_date,
           active,
           in_use,
           hide_from_search,
           no_adwords,
           price_rounding,
           quantization,
           package_price_amount_of_adults,
           price,
           price_title,
           old_price,
           per_person_price,
           payment_option,
           payment_method_description,
           booking_fee,
           booking_fee_for_person,
           down_payment,
           disable_instalments,
           fixed_down_payment_fee,
           down_payment_for_person,
           offer_unit,
           place_description,
           included,
           excluded,
           excluded_short,
           features,
           details,
           editor_tip,
           editor_tip_picture,
           tags,
           priority,
           label,
           allow_multiple_units,
           participants_fields,
           customer_fields,
           birthdate_required,
           category_id,
           site_id,
           target_group_id,
           transportation_id,
           hero_id,
           hero_options,
           partner_id,
           external_reference,
           concept_id,
           package_price_per_night,
           product_line
    FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer
);

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer;
ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer_update
    RENAME TO hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer
WHERE LEFT(se_sale_id, 3) = 'TVL';

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking_update AS (
    SELECT tbo.schedule_tstamp,
           tbo.run_tstamp,
           tbo.operation_id,
           tbo.created_at,
           tbo.updated_at,
           tbo.id,
           tbo.created_at_dts,
           tbo.updated_at_dts,
           tbo.session_validity,
           tbo.payment_status,
           tbo.order_status,
           tbo.manual_order_status,
           tbo.payment_reference,
           tbo.payment_method_from_adyen,
           tbo.redeem_on,
           tbo.complete_date,
           tbo.reference_id,
           tbo.token,
           tbo.token_expiration,
           tbo.token_type,
           tbo.customer_ip_address,
           tbo.request_country,
           tbo.comments,
           tbo.internal_comments,
           tbo.partner_mail_sent,
           tbo.utm_source,
           tbo.utm_medium,
           tbo.utm_campaign,
           tbo.utm_term,
           tbo.utm_content,
           tbo.platform,
           tbo.user_agent,
           tbo.processed,
           tbo.tracking_pixel_shown,
           tbo.missing_data,
           tbo.warning_count,
           tbo.partner_partial_mail_sent,
           tbo.buyer_id,
           tbo.customer_id,
           tbo.offer_id,
           tbo.offer_date_id,
           tbo.payment_method_id,
           tbo.referral_id,
           tbo.site_id,
           tbo.travel_date,
           tbo.return_date,
           tbo.booking_lead_time_days,
           tbo.sold_price_total_eur,
           tbo.cost_price_total_eur,
           tbo.booking_fee_vat_eur,
           tbo.booking_fee_incl_vat_eur,
           tbo.sold_price_total_gbp,
           tbo.cost_price_total_gbp,
           tbo.booking_fee_vat_gbp,
           tbo.booking_fee_incl_vat_gbp,
           tbo.margin_eur,
           tbo.margin_gbp,
           oof.se_sale_id AS se_sale_id
    FROM data_vault_mvp_dev_robin.dwh.tb_booking tbo
             INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer oof
                        ON tbo.offer_id = oof.id
    );

DROP TABLE data_vault_mvp_dev_robin.dwh.tb_booking;
ALTER TABLE data_vault_mvp_dev_robin.dwh.tb_booking_update RENAME TO data_vault_mvp_dev_robin.dwh.tb_booking;

SELECT * FROM hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer WHERE LEFT(se_sale_id, 3) = 'TVL';
SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer WHERE LEFT(se_sale_id, 3) = 'TVL';
SELECT * FROM data_vault_mvp_dev_robin.dwh.tb_booking WHERE LEFT(se_sale_id, 3) = 'TVL';

CREATE OR REPLACE TABLE hygiene_vault_mvp.travelbird_mysql.offers_offer CLONE hygiene_vault_mvp_dev_robin.travelbird_mysql.offers_offer;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer;
CREATE OR REPLACE TABLE data_vault_mvp.dwh.tb_booking CLONE data_vault_mvp_dev_robin.dwh.tb_booking;

SELECT * FROM hygiene_vault_mvp.travelbird_mysql.offers_offer WHERE LEFT(se_sale_id, 3) = 'TVL';
SELECT * FROM hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer WHERE LEFT(se_sale_id, 3) = 'TVL';
SELECT * FROM data_vault_mvp.dwh.tb_booking WHERE LEFT(se_sale_id, 3) = 'TVL';