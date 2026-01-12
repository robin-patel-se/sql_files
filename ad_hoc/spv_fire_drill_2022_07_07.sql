USE WAREHOUSE pipe_xlarge;
ALTER SESSION SET QUERY_TAG = 'spv firedrill Jul 2022';
--global spvs by territory
SELECT
    sts.event_tstamp::DATE,
    stmc.touch_affiliate_territory,
    COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2;
------------------------------------------------------------------------------------------------------------------------

--global web metrics
SELECT
    mts.event_tstamp::DATE                                                                                    AS date,
    CASE WHEN ds.data_model = 'New Data Model' THEN ds.posa_territory ELSE mtmc.touch_affiliate_territory END AS posa_territory,
    SUM(IFF(mtba.stitched_identity_type = 'se_user_id', 1, 0))                                                AS member_spvs,
    SUM(IFF(mtba.stitched_identity_type IS DISTINCT FROM 'se_user_id', 1, 0))                                 AS non_member_spvs,
    COUNT(mts.event_hash)                                                                                     AS spvs,
    COUNT(DISTINCT mts.touch_id)                                                                              AS sessions
FROM se.data.scv_touched_spvs mts
    INNER JOIN se.data.scv_touch_marketing_channel mtmc ON mts.touch_id = mtmc.touch_id
    INNER JOIN se.data.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
    INNER JOIN se.data.scv_touch_basic_attributes mtba ON mts.touch_id = mtba.touch_id
WHERE mts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  --To remove SE TECH SPVs from production metrics, these are a tech generated SPV | GR 2021-04-27
  AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'

  --To remove ANOMALOUS flaggged SPVs from production metrics
  AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'ANOMALOUS'
GROUP BY 1, 2, 3;


--spvs by date
SELECT
    mts.event_tstamp::DATE                                                    AS date,
    SUM(IFF(mtba.stitched_identity_type = 'se_user_id', 1, 0))                AS member_spvs,
    SUM(IFF(mtba.stitched_identity_type IS DISTINCT FROM 'se_user_id', 1, 0)) AS non_member_spvs,
    COUNT(mts.event_hash)                                                     AS spvs,
    COUNT(DISTINCT mts.touch_id)                                              AS sessions
FROM se.data.scv_touched_spvs mts
    INNER JOIN se.data.scv_touch_marketing_channel mtmc ON mts.touch_id = mtmc.touch_id
    INNER JOIN se.data.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
    INNER JOIN se.data.scv_touch_basic_attributes mtba ON mts.touch_id = mtba.touch_id
WHERE mts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  --To remove SE TECH SPVs from production metrics, these are a tech generated SPV | GR 2021-04-27
  AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'

  --To remove ANOMALOUS flaggged SPVs from production metrics
  AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'ANOMALOUS'
GROUP BY 1;


--DE SPVs by date
SELECT
    mts.event_tstamp::DATE                                                                                    AS date,
    CASE WHEN ds.data_model = 'New Data Model' THEN ds.posa_territory ELSE mtmc.touch_affiliate_territory END AS posa_territory,
    SUM(IFF(mtba.stitched_identity_type = 'se_user_id', 1, 0))                                                AS member_spvs,
    SUM(IFF(mtba.stitched_identity_type IS DISTINCT FROM 'se_user_id', 1, 0))                                 AS non_member_spvs,
    COUNT(mts.event_hash)                                                                                     AS spvs,
    COUNT(DISTINCT mts.touch_id)                                                                              AS sessions
FROM se.data.scv_touched_spvs mts
    INNER JOIN se.data.scv_touch_marketing_channel mtmc ON mts.touch_id = mtmc.touch_id
    INNER JOIN se.data.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
    INNER JOIN se.data.scv_touch_basic_attributes mtba ON mts.touch_id = mtba.touch_id
WHERE mts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  --To remove SE TECH SPVs from production metrics, these are a tech generated SPV | GR 2021-04-27
  AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'SE TECH'

  --To remove ANOMALOUS flaggged SPVs from production metrics
  AND mtmc.touch_affiliate_territory IS DISTINCT FROM 'ANOMALOUS'
  AND CASE WHEN ds.data_model = 'New Data Model' THEN ds.posa_territory ELSE mtmc.touch_affiliate_territory END = 'DE'
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

--bookings and financials
SELECT
    fb.booking_completed_date,
    COUNT(DISTINCT fb.booking_id)                      AS trx,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
GROUP BY 1;


SELECT
    fb.booking_completed_date,
    COUNT(DISTINCT fb.booking_id)                      AS trx,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    AVG(fb.gross_revenue_gbp_constant_currency)        AS avg_gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
GROUP BY 1;

-- increase in gross revenue, therefore checking avg gross revenue
SELECT
    fb.booking_completed_date,
    COUNT(DISTINCT fb.booking_id)                      AS trx,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    AVG(fb.gross_revenue_gbp_constant_currency)        AS avg_gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
GROUP BY 1;

-- check revenue figures in DE

SELECT
    fb.booking_completed_date,
    COUNT(DISTINCT fb.booking_id)                      AS trx,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    AVG(fb.gross_revenue_gbp_constant_currency)        AS avg_gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND IFF(ds.data_model = 'New Data Model', COALESCE(ds.posa_territory, fb.territory), COALESCE(fb.territory, ds.posa_territory)) = 'DE'
GROUP BY 1;


-- Increase in gross revenue and therefore margin (because take rate is the same)
-- Check which products are driving this change.

SELECT
    fb.booking_completed_date,
    ds.product_configuration,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    AVG(fb.gross_revenue_gbp_constant_currency)        AS avg_gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
GROUP BY 1, 2;

--de product configuration
SELECT
    fb.booking_completed_date,
    ds.product_configuration,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    AVG(fb.gross_revenue_gbp_constant_currency)        AS avg_gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND IFF(ds.data_model = 'New Data Model', COALESCE(ds.posa_territory, fb.territory), COALESCE(fb.territory, ds.posa_territory)) = 'DE'
GROUP BY 1, 2;

--looks like hotel is main driver
-- looking at travel type

SELECT
    fb.booking_completed_date,
    ds.travel_type,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    AVG(fb.gross_revenue_gbp_constant_currency)        AS avg_gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND ds.product_configuration = 'Hotel'
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

SELECT
    fb.booking_completed_date,
    SUM(fb.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp_constant_currency,
    AVG(fb.gross_revenue_gbp_constant_currency)        AS avg_gross_revenue_gbp_constant_currency,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type = 'live'
  AND fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND ds.product_configuration = 'Catalogue'
  AND IFF(ds.data_model = 'New Data Model', COALESCE(ds.posa_territory, fb.territory), COALESCE(fb.territory, ds.posa_territory)) = 'DE'
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------
-- de avg duration and events in session
SELECT
    stba.touch_start_tstamp::DATE                                                AS date,
    AVG(IFF(stba.touch_duration_seconds = 0, NULL, stba.touch_duration_seconds)) AS avg_duration,
    AVG(stba.touch_event_count)                                                  AS avg_events
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
  AND stmc.touch_affiliate_territory = 'DE'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------

-- spvs by session

SELECT
    stba.touch_start_tstamp::DATE  AS date,
    COUNT(DISTINCT stba.touch_id)  AS sessions,
    COUNT(DISTINCT sts.touch_id)   AS sessions_with_spvs,
    COUNT(DISTINCT sts.event_hash) AS spvs,
    spvs / sessions
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN  se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND stmc.touch_affiliate_territory = 'DE'
GROUP BY 1;


-- de spvs by platform
SELECT
    stba.touch_start_tstamp::DATE  AS date,
    stba.touch_experience,
    COUNT(DISTINCT stba.touch_id)  AS sessions,
    COUNT(DISTINCT sts.touch_id)   AS sessions_with_spvs,
    COUNT(DISTINCT sts.event_hash) AS spvs,
    spvs / sessions
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN  se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
  AND stmc.touch_affiliate_territory = 'DE'
GROUP BY 1, 2;


-- spvs by booker segment
SELECT
    stba.touch_start_tstamp::DATE  AS date,
    us.booker_segment,
    COUNT(DISTINCT stba.touch_id)  AS sessions,
    COUNT(DISTINCT sts.touch_id)   AS sessions_with_spvs,
    COUNT(DISTINCT sts.event_hash) AS spvs,
    spvs / sessions
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN  se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    INNER JOIN se.data.user_segmentation us ON stba.touch_start_tstamp::DATE = us.date AND stba.attributed_user_id_hash = SHA2(us.shiro_user_id) AND us.date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
WHERE stba.touch_start_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
  AND stmc.touch_affiliate_territory = 'DE'
GROUP BY 1, 2;


-- travelist spvs
SELECT
    sts.event_tstamp::DATE,
    COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.se_sale_id LIKE 'TVL%'
  AND stmc.touch_affiliate_territory = 'DE'
  AND sts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
-- de spvs channel

SELECT
    mts.event_tstamp::DATE                                                    AS date,
    mtmc.touch_mkt_channel,
    SUM(IFF(mtba.stitched_identity_type = 'se_user_id', 1, 0))                AS member_spvs,
    SUM(IFF(mtba.stitched_identity_type IS DISTINCT FROM 'se_user_id', 1, 0)) AS non_member_spvs,
    COUNT(mts.event_hash)                                                     AS spvs,
    COUNT(DISTINCT mts.touch_id)                                              AS sessions
FROM se.data.scv_touched_spvs mts
    INNER JOIN se.data.scv_touch_marketing_channel mtmc ON mts.touch_id = mtmc.touch_id
    INNER JOIN se.data.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
    INNER JOIN se.data.scv_touch_basic_attributes mtba ON mts.touch_id = mtba.touch_id
WHERE mts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
  AND mtmc.touch_affiliate_territory = 'DE'
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
-- DE active users

USE WAREHOUSE pipe_xlarge;

SELECT
    us.date,
    us.engagement_segment,
    COUNT(*)
FROM se.data.user_segmentation us
    INNER JOIN se.data.se_user_attributes sua ON us.shiro_user_id = sua.shiro_user_id
WHERE us.date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND sua.current_affiliate_territory = 'DE'
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

-- spvs by conversion
WITH daily_transactions AS (
    SELECT
        stt.touch_id,
        stt.event_tstamp::DATE AS date,
        COUNT(*)               AS bookings
    FROM se.data.scv_touched_transactions stt
        INNER JOIN se.data.scv_touch_marketing_channel mtmc ON stt.touch_id = mtmc.touch_id
    WHERE stt.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
      AND mtmc.touch_affiliate_territory = 'DE'
    GROUP BY 1, 2
),
     daily_spvs AS (
         SELECT
             sts.touch_id,
             sts.event_tstamp::DATE AS date,
             COUNT(*)               AS spvs
         FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_marketing_channel mtmc ON sts.touch_id = mtmc.touch_id
         WHERE sts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
           AND mtmc.touch_affiliate_territory = 'DE'
         GROUP BY 1, 2
     )
SELECT
    dt.date,
    COUNT(DISTINCT ds.touch_id) AS sessions,
    SUM(dt.bookings)            AS bookings,
    SUM(ds.spvs)                AS spvs
FROM daily_transactions dt
    INNER JOIN daily_spvs ds ON dt.touch_id = ds.touch_id
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------

--distribution of sales
SELECT
    DATE_TRUNC(WEEK, sts.event_tstamp) AS week,
    COUNT(DISTINCT sts.se_sale_id)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel mtmc ON sts.touch_id = mtmc.touch_id
WHERE sts.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND mtmc.touch_affiliate_territory = 'DE'
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------

USE WAREHOUSE pipe_2xlarge;

--spvs cs vs ss
WITH cs_web_snowplow_spvs AS (
    SELECT
        ses.event_tstamp::DATE AS date,
        COUNT(*)               AS cs_spvs
    FROM se.data_pii.scv_event_stream ses
    WHERE ses.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
      AND ses.event_name = 'page_view'
      AND ses.se_sale_id IS NOT NULL
      AND ses.device_platform NOT IN ('native app ios', 'native app android') --explicitly remove native app (as app offer pages appear like web SPVs)
      AND ses.is_server_side_event = FALSE
      AND ses.page_urlpath LIKE ANY ('%/sale', '%/sale-%')
    GROUP BY 1
),
     ss_web_snowplow_spvs AS (
         SELECT
             ses.event_tstamp::DATE AS date,
             COUNT(*)               AS ss_spvs
         FROM se.data_pii.scv_event_stream ses
         WHERE ses.event_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE - 1
           AND ses.event_name = 'page_view'
           AND ses.device_platform NOT IN ('native app ios', 'native app android')  --explicitly remove native app (as app offer pages appear like web SPVs)
           AND ses.se_sale_id IS NOT NULL
           AND ses.is_server_side_event = TRUE
           AND ses.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
           AND PARSE_URL(ses.page_url, 1)['path']::VARCHAR NOT LIKE '%/sale-offers' -- remove issue where spv events were firing on offer pages
         GROUP BY 1
     )
SELECT
    sc.date_value,
    cwss.cs_spvs,
    swss.ss_spvs
FROM se.data.se_calendar sc
    LEFT JOIN cs_web_snowplow_spvs cwss ON sc.date_value = cwss.date
    LEFT JOIN ss_web_snowplow_spvs swss ON sc.date_value = swss.date
WHERE sc.date_value BETWEEN '2022-01-01' AND CURRENT_DATE;

------------------------------------------------------------------------------------------------------------------------
--2022-07-11

USE WAREHOUSE pipe_xlarge;
ALTER SESSION SET QUERY_TAG = 'spv firedrill Jul 2022';

--DE spvs by product location
WITH spvs_by_location AS (
    SELECT
        stba.touch_start_tstamp::DATE  AS date,
        ds.posu_country,
        COUNT(DISTINCT sts.event_hash) AS spvs
    FROM se.data.scv_touched_spvs sts
        INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
        INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
    WHERE stba.touch_start_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
      AND stmc.touch_affiliate_territory = 'DE'
    GROUP BY 1, 2
),
     total_spvs AS (
         SELECT
             sbl.posu_country,
             SUM(sbl.spvs) AS cumulative_spvs
         FROM spvs_by_location sbl
         GROUP BY 1
     ),
     rank_country AS (
         SELECT
             ts.posu_country,
             ts.cumulative_spvs,
             ROW_NUMBER() OVER (ORDER BY ts.cumulative_spvs DESC) AS rank
         FROM total_spvs ts
     )
SELECT
    sbl.date,
    sbl.posu_country,
    rc.rank,
    sbl.spvs
--     rc.cumulative_spvs,
FROM spvs_by_location sbl
    LEFT JOIN rank_country rc ON sbl.posu_country = rc.posu_country;


------------------------------------------------------------------------------------------------------------------------
--sales active by posu
SELECT
    sa.view_date,
    COALESCE(ds.posu_country, 'NULL') AS posu_country,
    COUNT(DISTINCT ds.se_sale_id)     AS sales
FROM se.data.sale_active sa
    INNER JOIN se.data.dim_sale ds ON sa.se_sale_id = ds.se_sale_id
WHERE sa.view_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND ds.posa_territory = 'DE'
GROUP BY 1, 2;

-- sale active by deal category
SELECT
    sa.view_date,
    ssa.deal_category,
    COUNT(DISTINCT ssa.se_sale_id) AS sales
FROM se.data.sale_active sa
--     INNER JOIN se.data.dim_sale ds ON sa.se_sale_id = ds.se_sale_id
    INNER JOIN se.data.se_sale_attributes ssa ON sa.se_sale_id = ssa.se_sale_id
WHERE sa.view_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND ssa.posa_territory = 'DE'
  AND ssa.deal_category IS NOT NULL
GROUP BY 1, 2;

-- sale active by deal category in germany posu
SELECT
    sa.view_date,
    ssa.deal_category,
    COUNT(DISTINCT ssa.se_sale_id) AS sales
FROM se.data.sale_active sa
    INNER JOIN se.data.se_sale_attributes ssa ON sa.se_sale_id = ssa.se_sale_id
WHERE sa.view_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
  AND ssa.posa_territory = 'DE'
  AND ssa.posu_country = 'Germany'
  AND ssa.deal_category IS NOT NULL
GROUP BY 1, 2;

-- spv by deal category in germany posu
SELECT
    stba.touch_start_tstamp::DATE       AS date,
    COALESCE(ssa.deal_category, 'NULL') AS deal_category,
    COUNT(DISTINCT sts.event_hash)      AS spvs
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
    LEFT JOIN  se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
WHERE stba.touch_start_tstamp BETWEEN '2022-01-01' AND CURRENT_DATE
  AND stmc.touch_affiliate_territory = 'DE'
  AND ds.posu_country = 'Germany'
GROUP BY 1, 2;


SELECT *
FROM data_vault_mvp.data_quality.data_quality_checks dqc
WHERE dqc.should_investigate;



