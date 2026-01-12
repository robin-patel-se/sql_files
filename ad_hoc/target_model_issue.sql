--original code from transaction model
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.target_model_original AS (
    SELECT COALESCE(targets.target_date, bookings.target_date, bookings_v2.target_date,
                    new_sales.target_date)                                                            AS target_date,
           COALESCE(targets.dimension_1, bookings.dimension_1, bookings_v2.dimension_1, new_sales.dimension_1,
                    'Other')                                                                          AS dimension_1,
           COALESCE(targets.dimension_2, bookings.dimension_2, bookings_v2.dimension_2, 'Other')      AS dimension_2,
           COALESCE(targets.dimension_3, bookings.dimension_3, bookings_v2.dimension_3, 'Other')      AS dimension_3,
           COALESCE(targets.dimension_4, bookings_v2.dimension_4, 'Other')                            AS dimension_4,
           COALESCE(targets.dimension_5, 'Other')                                                     AS dimension_5,
           COALESCE(targets.target_name, bookings.target_name, bookings_v2.target_name, new_sales.target_name,
                    'Other')                                                                          AS target_name,
           COALESCE(targets.target_value, 0)                                                          AS target_value,
           COALESCE(bookings.margin_actual, bookings_v2.margin_actual, new_sales.new_sales_actual, 0) AS target_actual,
           COALESCE(bookings.margin_actual_reported_rate, bookings_v2.margin_actual_reported_rate,
                    0)                                                                                AS target_actual_reported_rate
    FROM hygiene_snapshot_vault_mvp_mvp.fpa_gsheets.generic_targets targets
        FULL OUTER JOIN (
                            SELECT booking_completed_date::DATE                        AS target_date,
                                   'margin'                                            AS target_name,
                                   s.posu_cluster                                      AS dimension_1,
                                   CASE
                                       WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                                           THEN '3PP/WRD'
                                       --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                                       --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                                       WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                                           AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                                           THEN 'Catalogue' --Temp fix for CA
                                       WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                                           THEN 'Hotel'
                                       WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                                           AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                                           THEN 'IHP' -- Temp fix for CA
                                       WHEN s.sale_type IN ('N/A')
                                           THEN NULL
                                       ELSE s.sale_type
                                       END                                             AS dimension_2,
                                   se.data.posa_category_from_territory(fcb.territory) AS dimension_3,
                                   SUM(margin_gross_of_toms_gbp_constant_currency)     AS margin_actual,
                                   SUM(margin_gross_of_toms_gbp)                       AS margin_actual_reported_rate
                            FROM se.data.fact_complete_booking fcb
                                LEFT JOIN se.data.dim_sale s ON fcb.sale_id = s.se_sale_id
                            WHERE booking_completed_date::DATE >= '2019-01-01'
                            GROUP BY 1, 2, 3, 4, 5
                        ) bookings ON bookings.target_date = targets.target_date
        AND bookings.target_name = targets.target_name
        AND bookings.dimension_1 = targets.dimension_1
        AND bookings.dimension_2 = targets.dimension_2
        AND bookings.dimension_3 = targets.dimension_3
        AND bookings.target_name = targets.target_name
        FULL OUTER JOIN (
                            SELECT booking_completed_date::DATE                        AS target_date,
                                   'margin_v2'                                         AS target_name,
                                   s.posu_cluster                                      AS dimension_1,
                                   CASE
                                       WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                                           THEN '3PP/WRD'
                                       --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                                       --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                                       WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                                           AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                                           THEN 'Catalogue' --Temp fix for CA
                                       WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                                           THEN 'Hotel'
                                       WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                                           AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                                           THEN 'IHP' --Temp fix for CA
                                       WHEN s.sale_type IN ('N/A')
                                           THEN NULL
                                       ELSE s.sale_type
                                       END                                             AS dimension_2,
                                   se.data.posa_category_from_territory(fcb.territory) AS dimension_3,
                                   s.cm_region                                         AS dimension_4,
                                   SUM(margin_gross_of_toms_gbp_constant_currency)     AS margin_actual,
                                   SUM(margin_gross_of_toms_gbp_constant_currency)     AS margin_actual_reported_rate
                            FROM se.data.fact_complete_booking fcb
                                LEFT JOIN se.data.dim_sale s ON fcb.sale_id = s.se_sale_id
                            WHERE booking_completed_date::DATE >= '2019-01-01'
                            GROUP BY 1, 2, 3, 4, 5, 6
                        ) bookings_v2 ON bookings_v2.target_date = targets.target_date
        AND bookings_v2.target_name = targets.target_name
        AND bookings_v2.dimension_1 = targets.dimension_1
        AND bookings_v2.dimension_2 = targets.dimension_2
        AND bookings_v2.dimension_3 = targets.dimension_3
        AND bookings_v2.dimension_4 = targets.dimension_4
        AND bookings_v2.target_name = targets.target_name
        FULL OUTER JOIN (
                            SELECT CAST(s.sale_start_date AS DATE) AS target_date,
                                   'new deals'                     AS target_name,
                                   s.posu_cluster                  AS dimension_1,
                                   COUNT(*)                        AS new_sales_actual
                            FROM se.data.dim_sale s
                            WHERE CAST(s.sale_start_date AS DATE) >= '2020-10-01'
                            GROUP BY 1, 2, 3
                        ) new_sales ON new_sales.target_date = targets.target_date
        AND new_sales.target_name = targets.target_name
        AND new_sales.dimension_1 = targets.dimension_1
);


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.target_model AS (
    WITH bookings AS (
        SELECT fcb.booking_completed_date::DATE                    AS target_date,
               'margin'                                            AS target_name,
               s.posu_cluster                                      AS dimension_1,
               CASE
                   WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                       THEN '3PP/WRD'
                   --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                   --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                   WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                       AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                       THEN 'Catalogue' --Temp fix for CA
                   WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                       THEN 'Hotel'
                   WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                       AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                       THEN 'IHP' -- Temp fix for CA
                   WHEN s.sale_type IN ('N/A')
                       THEN NULL
                   ELSE s.sale_type
                   END                                             AS dimension_2,
               se.data.posa_category_from_territory(fcb.territory) AS dimension_3,
               SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_actual,
               SUM(fcb.margin_gross_of_toms_gbp)                   AS margin_actual_reported_rate
        FROM se.data.fact_complete_booking fcb
            LEFT JOIN se.data.dim_sale s ON fcb.sale_id = s.se_sale_id
        WHERE fcb.booking_completed_date::DATE >= '2019-01-01'
        GROUP BY 1, 2, 3, 4, 5
    ),
         bookings_v2 AS (
             SELECT fcb.booking_completed_date::DATE                    AS target_date,
                    'margin_v2'                                         AS target_name,
                    s.posu_cluster                                      AS dimension_1,
                    CASE
                        WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                            THEN '3PP/WRD'
                        --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                        --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                        WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                            AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                            THEN 'Catalogue' --Temp fix for CA
                        WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                            THEN 'Hotel'
                        WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                            AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                            THEN 'IHP' --Temp fix for CA
                        WHEN s.sale_type IN ('N/A')
                            THEN NULL
                        ELSE s.sale_type
                        END                                             AS dimension_2,
                    se.data.posa_category_from_territory(fcb.territory) AS dimension_3,
                    s.cm_region                                         AS dimension_4,
                    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_actual,
                    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_actual_reported_rate
             FROM se.data.fact_complete_booking fcb
                 LEFT JOIN se.data.dim_sale s ON fcb.sale_id = s.se_sale_id
             WHERE fcb.booking_completed_date::DATE >= '2019-01-01'
             GROUP BY 1, 2, 3, 4, 5, 6
         ),
         new_sales AS (
             SELECT CAST(s.sale_start_date AS DATE) AS target_date,
                    'new deals'                     AS target_name,
                    s.posu_cluster                  AS dimension_1,
                    COUNT(*)                        AS new_sales_actual
             FROM se.data.dim_sale s
             WHERE CAST(s.sale_start_date AS DATE) >= '2020-10-01'
             GROUP BY 1, 2, 3
         )

    SELECT COALESCE(targets.target_date, bookings.target_date, bookings_v2.target_date, new_sales.target_date)          AS target_date,
           COALESCE(targets.dimension_1, bookings.dimension_1, bookings_v2.dimension_1, new_sales.dimension_1, 'Other') AS dimension_1,
           COALESCE(targets.dimension_2, bookings.dimension_2, bookings_v2.dimension_2, 'Other')                        AS dimension_2,
           COALESCE(targets.dimension_3, bookings.dimension_3, bookings_v2.dimension_3, 'Other')                        AS dimension_3,
           COALESCE(targets.dimension_4, bookings_v2.dimension_4, 'Other')                                              AS dimension_4,
           COALESCE(targets.dimension_5, 'Other')                                                                       AS dimension_5,
           COALESCE(targets.target_name, bookings.target_name, bookings_v2.target_name, new_sales.target_name, 'Other') AS target_name,
           COALESCE(targets.target_value, 0)                                                                            AS target_value,
           COALESCE(bookings.margin_actual, bookings_v2.margin_actual, new_sales.new_sales_actual, 0)                   AS target_actual,
           COALESCE(bookings.margin_actual_reported_rate, bookings_v2.margin_actual_reported_rate, 0)                   AS target_actual_reported_rate

    FROM hygiene_snapshot_vault_mvp_mvp.fpa_gsheets.generic_targets targets
        FULL OUTER JOIN bookings ON targets.target_date = bookings.target_date
        AND targets.target_name = bookings.target_name
        AND targets.dimension_1 = bookings.dimension_1
        AND targets.dimension_2 = bookings.dimension_2
        AND targets.dimension_3 = bookings.dimension_3
        AND targets.target_name = bookings.target_name
        FULL OUTER JOIN bookings_v2 ON targets.target_date = bookings_v2.target_date
        AND targets.target_name = bookings_v2.target_name
        AND targets.dimension_1 = bookings_v2.dimension_1
        AND targets.dimension_2 = bookings_v2.dimension_2
        AND targets.dimension_3 = bookings_v2.dimension_3
        AND targets.dimension_4 = bookings_v2.dimension_4
        AND targets.target_name = bookings_v2.target_name
        FULL OUTER JOIN new_sales ON new_sales.target_date = targets.target_date
        AND new_sales.target_name = targets.target_name
        AND new_sales.dimension_1 = targets.dimension_1
);

USE WAREHOUSE pipe_2xlarge;

WITH dupes AS (
    SELECT *
    FROM scratch.robinpatel.target_model tm
        EXCEPT
    SELECT *
    FROM scratch.robinpatel.target_model_original tmo
)
   , stack AS (
    SELECT 'refactored' AS source,
           *
    FROM scratch.robinpatel.target_model tm
    UNION ALL
    SELECT 'original' AS source,
           *
    FROM scratch.robinpatel.target_model_original tmo
)
SELECT *
FROM stack
    INNER JOIN dupes ON stack.target_date = dupes.target_date
    AND stack.dimension_1 = dupes.dimension_1
    AND stack.dimension_2 = dupes.dimension_2
    AND stack.dimension_3 = dupes.dimension_3
    AND stack.dimension_4 = dupes.dimension_4
    AND stack.target_name = dupes.target_name
;


SELECT tm.target_date,
       tm.dimension_1,
       tm.dimension_2,
       tm.dimension_3,
       tm.dimension_4,
       tm.dimension_5,
       tm.target_value,
       tmo.target_value,
       tm.target_actual,
       tmo.target_actual,
       tm.target_actual_reported_rate,
       tmo.target_actual_reported_rate
FROM scratch.robinpatel.target_model tm
    LEFT JOIN scratch.robinpatel.target_model_original tmo ON tm.target_date = tmo.target_date
    AND tm.dimension_1 = tmo.dimension_1
    AND tm.dimension_2 = tmo.dimension_2
    AND tm.dimension_3 = tmo.dimension_3
    AND tm.dimension_4 = tmo.dimension_4
    AND tm.dimension_5 = tmo.dimension_5
    AND tm.target_name = tmo.target_name
WHERE tm.target_actual::NUMBER != tmo.target_actual::NUMBER

------------------------------------------------------------------------------------------------------------------------
--territories still misaligned

CREATE OR REPLACE VIEW se_dev_robin.data.dim_sale AS
SELECT *
FROM se.data.dim_sale;
CREATE OR REPLACE VIEW se_dev_robin.bi.fact_sale_metrics AS
SELECT *
FROM se.bi.fact_sale_metrics;
CREATE OR REPLACE VIEW se_dev_robin.data.global_sale_attributes AS
SELECT *
FROM se.data.global_sale_attributes;
CREATE OR REPLACE VIEW se_dev_robin.data.sale_active AS
SELECT *
FROM se.data.sale_active;
CREATE OR REPLACE VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory AS
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.cms_mysql.sale_territory AS
SELECT *
FROM hygiene_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_translation AS
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_translation;
CREATE OR REPLACE VIEW se_dev_robin.data.se_company_attributes AS
SELECT *
FROM se.data.se_company_attributes;
CREATE OR REPLACE VIEW se_dev_robin.data.se_sale_attributes AS
SELECT *
FROM se.data.se_sale_attributes;
CREATE OR REPLACE VIEW se_dev_robin.data.se_sale_tags AS
SELECT *
FROM se.data.se_sale_tags;
CREATE OR REPLACE VIEW se_dev_robin.data.tb_offer AS
SELECT *
FROM se.data.tb_offer;
CREATE OR REPLACE VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory AS
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.territory;

DROP VIEW se_dev_robin.data.dim_sale;
DROP VIEW se_dev_robin.bi.fact_sale_metrics;
DROP VIEW se_dev_robin.data.global_sale_attributes;
DROP VIEW se_dev_robin.data.sale_active;
DROP VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory;
DROP VIEW hygiene_vault_mvp_dev_robin.cms_mysql.sale_territory;
DROP VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_translation;
DROP VIEW se_dev_robin.data.se_company_attributes;
DROP VIEW se_dev_robin.data.se_sale_attributes;
DROP VIEW se_dev_robin.data.se_sale_tags;
DROP VIEW se_dev_robin.data.tb_offer;
DROP VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory;

self_describing_task --include 'se/bi/dim_sale.py'  --method 'run' --start '2021-07-28 00:00:00' --end '2021-07-28 00:00:00'


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_territory st;

SELECT *
FROM hygiene_vault_mvp.cms_mysql.sale_territory st;


SELECT *
FROM se_dev_robin.bi.dim_sale;



CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.athena_email_reporting AS
SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting;
DROP TABLE data_vault_mvp_dev_robin.dwh.sale_active;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.sale_active AS
SELECT *
FROM data_vault_mvp.dwh.sale_active;
CREATE OR REPLACE VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory AS
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory AS
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.territory;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar;

--fact sale metrics
SELECT date,
       posa_territory,
       SUM(fsm.margin_constant_currency) AS margin1
FROM se.bi.fact_sale_metrics fsm
WHERE fsm.date = CURRENT_DATE - 1
GROUP BY 1, 2
HAVING margin1 > 0;

-- fact complete booking
SELECT fcb.booking_completed_date::DATE                    AS date,
       fcb.territory                                       AS posa_territory,
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date::DATE = CURRENT_DATE - 1
GROUP BY 1, 2
    self_describing_task --include 'dv/bi/tableau/deal_model/fact_sale_metrics.py'  --method 'run' --start '2021-07-28 00:00:00' --end '2021-07-28 00:00:00'


SELECT date,
       bm.posa_territory,
       SUM(bm.margin_constant_currency)
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics__step01__booking_metrics bm
WHERE date = CURRENT_DATE - 1
GROUP BY 1, 2;


SELECT *
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics__step03__cpi_metrics
WHERE posa_territory IS NULL;
SELECT *
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics__step04__sale_first_last_dates
WHERE posa_territory IS NULL;
SELECT *
FROM data_vault_mvp_dev_robin.bi.fact_sale_metrics__step05__sale_territory_blowout
WHERE posa_territory IS NULL;
SELECT *
FROM se.data.dim_sale ds
WHERE ds.se_sale_id = '46557';

SELECT DATE_TRUNC(YEAR, fcb.booking_completed_timestamp)   AS year,
       COUNT(DISTINCT booking_id)                          AS trx,
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE fcb.territory IS NULL
GROUP BY 1;


select * from COLLAB.DACH.SFMC_NPS

SELECT get_ddl('table', 'COLLAB.DACH.SFMC_NPS');

SELECT * FROM hygiene_snapshot_vault_mvp.sfmc.net_promoter_score nps