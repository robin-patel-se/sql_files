SELECT
    sts.event_tstamp::DATE                            AS date,
    SUM(IFF(sts.page_url LIKE '%sale-offers%', 1, 0)) AS offer_spvs,
    COUNT(*)                                          AS spvs,
    offer_spvs / spvs
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 180
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------

SELECT
    sts.event_tstamp::DATE                            AS date,
    stba.touch_experience,
    ds.product_configuration,
    SUM(IFF(sts.page_url LIKE '%sale-offers%', 1, 0)) AS offer_spvs,
    COUNT(*)                                          AS spvs,
    offer_spvs / spvs
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
WHERE sts.event_tstamp >= CURRENT_DATE - 180
GROUP BY 1, 2, 3;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 180
  AND sts.page_url LIKE '%/sale-offers%';


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

USE WAREHOUSE pipe_xlarge;
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
WHERE page_url LIKE '%/sale-offers%';

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_spv_20220701 CLONE data_vault_mvp.single_customer_view_stg.module_touched_spv;

SELECT
    sts.event_tstamp::DATE                             AS date,
    SUM(IFF(sts.page_url LIKE '%/sale-offers%', 1, 0)) AS offer_spvs,
    COUNT(*)                                           AS spvs,
    offer_spvs / spvs
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 180
GROUP BY 1;

SELECT
    sts.event_tstamp::DATE                             AS date,
    SUM(IFF(sts.page_url LIKE '%/sale-offers%', 1, 0)) AS offer_spvs,
    COUNT(*)                                           AS spvs,
    offer_spvs / spvs
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 180
GROUP BY 1;


SELECT
    mtba.touch_experience,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON mts.touch_id = mtba.touch_id
WHERE mts.page_url LIKE '%/sale-offers%'
  AND mts.event_tstamp >= CURRENT_DATE - 1
GROUP BY 1;


SELECT
    ds.product_configuration,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN data_vault_mvp.dwh.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
WHERE mts.page_url LIKE '%/sale-offers%'
  AND mts.event_tstamp >= CURRENT_DATE - 1
GROUP BY 1;


SELECT
    mtmc.touch_mkt_channel,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mts.touch_id = mtmc.touch_id
WHERE mts.page_url LIKE '%/sale-offers%'
  AND mts.event_tstamp >= CURRENT_DATE - 1
GROUP BY 1;

SELECT
    mtmc.touch_affiliate_territory,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mts.touch_id = mtmc.touch_id
WHERE mts.page_url LIKE '%/sale-offers%'
  AND mts.event_tstamp >= CURRENT_DATE - 1
GROUP BY 1;



SELECT
    sua.member_original_affiliate_classification,
    COUNT(*)
FROM se.data.se_user_attributes sua
GROUP BY 1



SELECT
    TO_DATE(DATE_TRUNC(YEAR, fb.booking_completed_date))                                                           AS booking_date,
    fb.territory                                                                                                   AS territory,
    sua.member_original_affiliate_classification,
    SUM(IFF(fb.booking_status_type IN ('live', 'cancelled'), fb.margin_gross_of_toms_gbp_constant_currency, NULL)) AS margin_pre_canx,
    SUM(IFF(fb.booking_status_type IN ('live'), fb.margin_gross_of_toms_gbp_constant_currency, NULL))              AS margin_post_canx
FROM se.data.fact_booking fb
    LEFT JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.territory IN ('UK', 'DE')
  AND fb.booking_completed_date >= '2018-01-01'
GROUP BY 1, 2, 3;


SELECT
    TO_DATE(DATE_TRUNC(YEAR, fb.booking_completed_date)) AS booking_date,
    fb.territory                                         AS territory,
    sua.member_original_affiliate_classification,
    *
FROM se.data.fact_booking fb
    LEFT JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.territory IN ('UK', 'DE')
  AND fb.booking_completed_date >= '2018-01-01'
  AND sua.member_original_affiliate_classification IS NULL;



SELECT *
FROM latest_vault.survey_sparrow.nps_responses nr;


SELECT *
FROM se.data.user_booking_review ubr;


SELECT *
FROM se.data.dim_sale ds;


-- Hi, does anyone have an existing query for Catalogue 2019 margin and SPVs? Ideally deal level with POSa splits.

SELECT
    ds.posa_territory,
    sts.event_tstamp::DATE AS date,
    COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id AND ds.product_configuration = 'Catalogue'
WHERE YEAR(sts.event_tstamp) = 2019
GROUP BY 1, 2;


SELECT
    ds.posa_territory,
    sts.event_tstamp::DATE AS date,
    COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id AND ds.product_configuration = 'Catalogue'
WHERE YEAR(sts.event_tstamp) = 2019
GROUP BY 1, 2;

SELECT
    fcb.booking_completed_date::DATE                    AS date,
    d.posa_territory,
    COUNT(*)                                            AS bookings,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id AND ds.product_configuration = 'Catalogue'
    INNER JOIN se.data.dim_sale d ON fcb.se_sale_id = d.se_sale_id
WHERE YEAR(fcb.booking_completed_date) = 2019
GROUP BY 1, 2;


SELECT * FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspvs;



