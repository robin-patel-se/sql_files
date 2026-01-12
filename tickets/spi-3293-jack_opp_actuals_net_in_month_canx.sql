SELECT
    f.*,
    ds.salesforce_opportunity_id,
    DATE_TRUNC('month', f.booking_completed_date)                                      AS booking_completed_month,
    DATE_TRUNC('month', f.cancellation_date)                                           AS cancellation_month,
    IFF(f.booking_status_type = 'cancelled' AND booking_completed_month <> cancellation_month,
        margin_gross_of_toms_gbp_constant_currency, 0)                                 AS margin_actual_canx_post_month,
    IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0) AS margin_actual,
    IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp, 0)                   AS margin_actual_reported_rate,
    COALESCE(margin_actual, 0) + COALESCE(margin_actual_canx_post_month, 0)            AS margin_actual_net_in_month_canx
FROM se.data.fact_booking f
    INNER JOIN se.data.dim_sale ds ON f.se_sale_id = ds.se_sale_id;

WITH booking_calc AS (
    SELECT
        f.booking_id,
        f.transaction_id,
        ds.salesforce_opportunity_id,
        DATE_TRUNC('month', f.booking_completed_date)                                      AS booking_completed_month,
        DATE_TRUNC('month', f.cancellation_date)                                           AS cancellation_month,
        IFF(f.booking_status_type = 'cancelled' AND booking_completed_month <> cancellation_month,
            margin_gross_of_toms_gbp_constant_currency, 0)                                 AS margin_actual_canx_post_month,
        IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0) AS margin_actual,
        IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp, 0)                   AS margin_actual_reported_rate,
        COALESCE(margin_actual, 0) + COALESCE(margin_actual_canx_post_month, 0)            AS margin_actual_net_in_month_canx
    FROM se.data.fact_booking f
        INNER JOIN se.data.dim_sale ds ON f.se_sale_id = ds.se_sale_id
    WHERE f.booking_status_type IN ('live', 'cancelled')
)
SELECT
    bc.salesforce_opportunity_id,
    bc.booking_completed_month,
    SUM(bc.margin_actual_canx_post_month)   AS margin_actual_canx_post_month,
    SUM(bc.margin_actual)                   AS margin_actual,
    SUM(bc.margin_actual_reported_rate)     AS margin_actual_reported_rate,
    SUM(bc.margin_actual_net_in_month_canx) AS margin_actual_net_in_month_canx
FROM booking_calc bc
GROUP BY 1, 2
;


SELECT
    DATE_TRUNC(MONTH, t.target_date) AS month,
    t.target_name,
    t.dimension_3                    AS territory,
    SUM(t.target_actual)
FROM data_vault_mvp.bi.targets t
WHERE DATE_TRUNC(MONTH, t.target_date) = '2022-10-01'
  AND t.target_name IN ('cluster_sub_region_target', 'margin_v2')
GROUP BY 1, 2, 3
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.generic_targets CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets;
self_describing_task --include 'biapp/task_catalogue/se/data/udfs/udf_functions.py'  --method 'run' --start '2022-12-04 00:00:00' --end '2022-12-04 00:00:00'

SELECT
    fb.territory
FROM se.data.fact_booking fb;



self_describing_task --include 'biapp/task_catalogue/dv/bi/tableau/target_model/targets.py'  --method 'run' --start '2022-12-04 00:00:00' --end '2022-12-04 00:00:00'
SELECT
    DATE_TRUNC(MONTH, t.target_date) AS month,
    t.target_name,
    t.dimension_3                    AS territory,
    SUM(t.target_actual)
FROM data_vault_mvp_dev_robin.bi.targets t
WHERE DATE_TRUNC(MONTH, t.target_date) = '2022-10-01'
  AND t.target_name IN ('cluster_sub_region_target', 'margin_v2')
GROUP BY 1, 2, 3
;

SELECT
    DATE_TRUNC(MONTH, t.target_date) AS month,
    t.target_name,
    t.dimension_3                    AS territory,
    SUM(t.target_actual)
FROM data_vault_mvp.bi.targets t
WHERE DATE_TRUNC(MONTH, t.target_date) = '2022-10-01'
  AND t.target_name IN ('cluster_sub_region_target', 'margin_v2')
GROUP BY 1, 2, 3
;

SELECT
    DATE_TRUNC(MONTH, t.target_date) AS month,
    t.target_name,
    t.dimension_3                    AS territory,
    SUM(t.target_actual)
FROM data_vault_mvp_dev_robin.bi.targets t
WHERE DATE_TRUNC(MONTH, t.target_date) = '2022-10-01'
  AND t.target_name IN ('cluster_sub_region_target', 'margin_v2')
GROUP BY 1, 2, 3
;



SELECT
    DATE_TRUNC(MONTH, f.booking_completed_timestamp),
    CASE
        WHEN f.territory = 'DE' OR f.territory = 'CH' THEN 'DACH'
        WHEN f.territory = 'UK' THEN f.territory
        ELSE 'ROW'
        END                                AS dimension_3,
    SUM(f.margin_actual)                   AS margin_actual,
    SUM(f.margin_actual_reported_rate)     AS margin_actual_reported_rate,
    SUM(f.margin_actual_canx_post_month)   AS margin_actual_canx_post_month,
    SUM(f.margin_actual_net_in_month_canx) AS margin_actual_net_in_month_canx
FROM data_vault_mvp_dev_robin.bi.targets__step01__bookings_table f
    LEFT JOIN data_vault_mvp.dwh.dim_sale d ON f.se_sale_id = d.se_sale_id
WHERE f.territory = 'UK'
  AND DATE_TRUNC(MONTH, f.booking_completed_timestamp) = '2022-10-01'
GROUP BY 1, 2;

SELECT
    SUM(margin_actual)
FROM data_vault_mvp_dev_robin.bi.targets__step07__cluster_sub_region_grain_actuals
WHERE dimension_3 = 'UK'
  AND DATE_TRUNC(MONTH, target_date) = '2022-10-01';

WITH except_query AS (
    SELECT DISTINCT
        g.booking_completed_date AS target_date,
        g.dimension_1,
        g.dimension_2,
        g.dimension_3,
        g.dimension_4,
        g.dimension_5,
        g.dimension_6
    FROM data_vault_mvp_dev_robin.bi.targets__step05__cluster_sub_region_raw_actuals g
    EXCEPT
    SELECT DISTINCT
        g2.target_date,
        g2.dimension_1,
        g2.dimension_2,
        g2.dimension_3,
        g2.dimension_4,
        g2.dimension_5,
        g2.dimension_6
    FROM data_vault_mvp_dev_robin.bi.targets__step06__cluster_sub_region_grain g2
)
SELECT *
FROM except_query eq
WHERE eq.target_date BETWEEN '2022-01-01' AND CURRENT_DATE;

SELECT *
FROM data_vault_mvp_dev_robin.bi.targets__step05__cluster_sub_region_raw_actuals
WHERE dimension_1 IS NULL;

SELECT *
FROM se.data.fact_booking fb
    LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fb.booking_completed_timestamp) = '2022-10-01'
  AND ds.posu_cluster IS NULL
  AND fb.territory = 'UK';


SELECT *
FROM se.data.tb_booking tb
WHERE tb.order_id = '22053667';
SELECT *
FROM se.data.tb_offer t
WHERE t.tb_offer_id = '118276'


SELECT *
FROM data_vault_mvp_dev_robin.bi.targets__step05__cluster_sub_region_raw_actuals a
WHERE a.dimension_1 IS NULL;


SELECT
    f.booking_id,
    d.se_sale_id,
    f.booking_completed_date,
    d.posu_cluster                    AS dimension_1,
    CASE
        WHEN d.sale_type = 'Hotel' OR d.sale_type = 'Hotel Plus' THEN 'Hotel'
        WHEN UPPER(d.sale_type) LIKE 'IHP%' AND LOWER(d.supplier_name) NOT LIKE 'secret escapes%' THEN 'IHP'
        WHEN UPPER(d.sale_type) LIKE 'WRD%' OR d.sale_type = '3PP' THEN '3PP/WRD'
        WHEN UPPER(d.sale_type) LIKE 'IHP%' OR d.sale_type = 'Catalogue' AND LOWER(d.supplier_name) LIKE 'secret escapes%' THEN 'Catalogue'
        ELSE 'Other'
        END                           AS dimension_2,
    CASE
        WHEN f.territory = 'DE' OR f.territory = 'CH' THEN 'DACH'
        WHEN f.territory = 'UK' THEN f.territory
        ELSE 'ROW'
        END                           AS dimension_3,
    d.cm_region                       AS dimension_4,
    d.posu_cluster_region             AS dimension_5,
    d.posu_cluster_sub_region         AS dimension_6,
    f.margin_actual                   AS margin_actual,
    f.margin_actual_reported_rate     AS margin_actual_reported_rate,
    f.margin_actual_canx_post_month   AS margin_actual_canx_post_month,
    f.margin_actual_net_in_month_canx AS margin_actual_net_in_month_canx
FROM data_vault_mvp_dev_robin.bi.targets__step01__bookings_table f
    LEFT JOIN data_vault_mvp_dev_robin.dwh.dim_sale d ON f.se_sale_id = d.se_sale_id
WHERE d.posu_cluster IS NULL;

SELECT
    f.booking_id,
    f.booking_completed_date,
    f.se_sale_id,
    d.posu_cluster            AS dimension_1,
    CASE
        WHEN d.sale_type = 'Hotel' OR d.sale_type = 'Hotel Plus' THEN 'Hotel'
        WHEN UPPER(d.sale_type) LIKE 'IHP%' AND LOWER(d.supplier_name) NOT LIKE 'secret escapes%' THEN 'IHP'
        WHEN UPPER(d.sale_type) LIKE 'WRD%' OR d.sale_type = '3PP' THEN '3PP/WRD'
        WHEN UPPER(d.sale_type) LIKE 'IHP%' OR d.sale_type = 'Catalogue' AND LOWER(d.supplier_name) LIKE 'secret escapes%' THEN 'Catalogue'
        ELSE 'Other'
        END                   AS dimension_2,
    CASE
        WHEN f.territory = 'DE' OR f.territory = 'CH' THEN 'DACH'
        WHEN f.territory = 'UK' THEN f.territory
        ELSE 'ROW'
        END                   AS dimension_3,
    d.cm_region               AS dimension_4,
    d.posu_cluster_region     AS dimension_5,
    d.posu_cluster_sub_region AS dimension_6,
    f.booking_status_type,
    f.margin_gross_of_toms_gbp_constant_currency
FROM data_vault_mvp.dwh.fact_booking f
    LEFT JOIN data_vault_mvp_dev_robin.dwh.dim_sale d ON f.se_sale_id = d.se_sale_id
WHERE d.se_sale_id IS NULL
  AND f.booking_status_type IN ('live', 'cancelled');


------------------------------------------------------------------------------------------------------------------------
-- compare booking level output for oct 22 margin v2 vs cluster sub region targets

--margin v2
SELECT
    fcb.booking_id,
    fcb.se_sale_id,
    s.se_sale_id,
    booking_completed_date::DATE                                                         AS target_date,
    'margin_v2'                                                                          AS target_name,
    s.posu_cluster                                                                       AS dimension_1,
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
        END                                                                              AS dimension_2,
    IFF(se.data.posa_category_from_territory(fcb.territory) = 'Scandi', fcb.territory,
        se.data.posa_category_from_territory(fcb.territory))                             AS dimension_3,
    s.cm_region                                                                          AS dimension_4,
    DATE_TRUNC('month', fcb.booking_completed_date)                                      AS booking_completed_month,
    DATE_TRUNC('month', fcb.cancellation_date)                                           AS cancellation_month,
    IFF(fcb.booking_status_type = 'cancelled' AND booking_completed_month <> cancellation_month,
        margin_gross_of_toms_gbp_constant_currency, 0)                                   AS margin_actual_canx_post_month,
    IFF(fcb.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0) AS margin_actual,
    IFF(fcb.booking_status_type = 'live', margin_gross_of_toms_gbp, 0)                   AS margin_actual_reported_rate,
    COALESCE(margin_actual, 0) + COALESCE(margin_actual_canx_post_month, 0)              AS margin_actual_net_in_month_canx
FROM se.data.fact_booking fcb
    LEFT JOIN se.data.dim_sale s
              ON fcb.se_sale_id = s.se_sale_id
WHERE booking_completed_date::DATE >= '2018-01-01'
  AND fcb.booking_status_type IN ('live', 'cancelled')
  AND s.se_sale_id IS NULL;


SELECT *
FROM data_vault_mvp.dwh.wrd_booking wb
WHERE wb.wrd_provider = 'JOURNAWAY'

-- found two booking streams that are inflating margin v2 but not cluster sub region (because they don't have a valid sale)
-- Journaway - these are WRD bookings that are valid but the sale id hasn't been correctly attached, Alessandro is managing the communications with Journaway
-- Travelist test bookings on tracy, these aren't valid bookings and are inflating figures because they are still recognised as live


SELECT
    fcb.booking_id,
    fcb.booking_completed_date,
    fcb.margin_gross_of_toms_gbp,
    fcb.se_sale_id,
    fcb.tech_platform,
    tb.order_id,
    tb.offer_id,
    t.concept_name,
    t.se_sale_id,
    tb.site_id,
    t.site_id,
    fcb.territory
FROM se.data.fact_complete_booking fcb
    LEFT JOIN se.data.tb_booking tb ON fcb.booking_id = tb.booking_id
    LEFT JOIN se.data.tb_offer t ON tb.offer_id = t.tb_offer_id
    LEFT JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE ds.se_sale_id IS NULL;

SELECT *
FROM se.data.tb_offer t
WHERE t.tb_offer_id = '113793';


SELECT
    fb.booking_id,
    fb.booking_completed_date,
    fb.se_sale_id  AS data_platform_external_reference,
    tb.order_id,
    tb.offer_id,
    tbo.se_sale_id AS external_reference,
    tbo.site_id
FROM se.data.fact_complete_booking fb
    LEFT JOIN se.data.tb_booking tb ON fb.booking_id = tb.booking_id
    LEFT JOIN data_vault_mvp.dwh.tb_offer tbo ON tb.offer_id = tbo.id
WHERE fb.se_sale_id LIKE 'TVL%'
  AND fb.tech_platform = 'TRAVELBIRD'
  AND fb.booking_completed_date >= CURRENT_DATE - 10;

-- we associate bookings and offers with a site id of 46 as a TVL booking/offer


SELECT *
FROM se.data.tb_offer t
WHERE t.se_sale_id IS NOT NULL
    QUALIFY COUNT(*) OVER (PARTITION BY t.se_sale_id) > 1;

SELECT * FROM se.data.tb_offer t WHERE t.tb_offer_id = 118044;
