-- reconciling lobster model data with tableau transaction model
-- https://eu-west-1a.online.tableau.com/#/site/secretescapes/views/Lobsterrec160123SR/Dashboard1


--need to investigate difference between transaction model and lobster dailies for 2018 (65,504,163 trx model vs 64,452,471 lobster daily)
SELECT
    YEAR(fb.booking_completed_timestamp)               AS year,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_status_type = 'live'
GROUP BY 1;

SELECT
    YEAR(dcmlpb.event_month) AS year,
    SUM(dcmlpb.margin_gbp)   AS margin_gbp_constant_currency
FROM dbt.bi_data_platform.dp_cohort_monthly_last_paid_bookings dcmlpb
GROUP BY 1;

-- transaction model
SELECT
    YEAR(fb.booking_completed_timestamp)               AS year,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_status_type = 'live'
  AND YEAR(fb.booking_completed_timestamp) = 2018
GROUP BY 1;

-- lobster daily
SELECT
    YEAR(dcmlpb.event_month) AS year,
    SUM(dcmlpb.margin_gbp)   AS margin_gbp_constant_currency
FROM dbt.bi_data_platform.dp_cohort_monthly_last_paid_bookings dcmlpb
WHERE YEAR(dcmlpb.event_month) = 2018
GROUP BY 1;


-- transaction model
SELECT
    YEAR(fb.booking_completed_timestamp)               AS year,
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_constant_currency
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_status_type = 'live'
  AND YEAR(fb.booking_completed_timestamp) = 2018
GROUP BY 1;

-- lobster daily
SELECT
    YEAR(dcmlpb.event_month) AS year,
    SUM(dcmlpb.margin_gbp)   AS margin_gbp_constant_currency
FROM dbt.bi_data_platform.dp_cohort_monthly_last_paid_bookings dcmlpb
WHERE YEAR(dcmlpb.event_month) = 2018
GROUP BY 1;


-- code that generates lobster daily
/*
SELECT
    COALESCE(sua.original_affiliate_territory, '')
    || COALESCE(sua.original_affiliate_id, 0) ||
    COALESCE(sua.original_affiliate_name, '') ||
    COALESCE(sua.member_original_affiliate_classification, '') ||
    COALESCE(stmc.touch_mkt_channel, '') ||
    COALESCE(fcb.booking_completed_date::DATE, '1970-01-01') ||
    COALESCE(sua.signup_tstamp::DATE, '1970-01-01') ||
    COALESCE(fcb.territory, '') AS id,

    sua.signup_tstamp::DATE AS signup_date,
    sua.original_affiliate_territory,
    sua.original_affiliate_id,
    sua.original_affiliate_name,
    sua.member_original_affiliate_classification,
    stmc.touch_mkt_channel,
    fcb.booking_completed_date::DATE AS event_date,
    fcb.territory AS booking_territory,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT fcb.booking_id) AS bookings
FROM dbt_dev.dbt_robinpatel_staging.base_dwh__fact_booking AS fcb
INNER JOIN
    dbt_dev.dbt_robinpatel_staging.base_dwh__user_attributes AS sua ON
        fcb.shiro_user_id = sua.shiro_user_id
INNER JOIN
    dbt_dev.dbt_robinpatel_staging.base_scv__module_touched_transactions AS stt ON
        fcb.booking_id = stt.booking_id
INNER JOIN
    dbt_dev.dbt_robinpatel_staging.base_scv__module_touch_attribution AS sta ON
        stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
INNER JOIN
    dbt_dev.dbt_robinpatel_staging.base_scv__module_touch_marketing_channel AS stmc ON
        sta.attributed_touch_id = stmc.touch_id
WHERE fcb.booking_completed_date >= '2018-01-01'
    AND fcb.booking_status_type = 'live'

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
*/
-- replicate lobster daily at year level
SELECT
    YEAR(fcb.booking_completed_date)                    AS year,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM dbt.bi_staging.base_dwh__fact_booking AS fcb
    INNER JOIN dbt.bi_staging.base_dwh__user_attributes AS sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN dbt.bi_staging.base_scv__module_touched_transactions AS stt ON fcb.booking_id = stt.booking_id
    INNER JOIN dbt.bi_staging.base_scv__module_touch_attribution AS sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
    INNER JOIN dbt.bi_staging.base_scv__module_touch_marketing_channel AS stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE YEAR(fcb.booking_completed_date) = 2018
  AND fcb.booking_status_type = 'live'
GROUP BY 1;

-- replicate lobster daily at year level removing join to users
SELECT
    YEAR(fcb.booking_completed_date)                    AS year,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM dbt.bi_staging.base_dwh__fact_booking AS fcb
--     INNER JOIN dbt.bi_staging.base_dwh__user_attributes AS sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN dbt.bi_staging.base_scv__module_touched_transactions AS stt ON fcb.booking_id = stt.booking_id
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_attribution AS sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_marketing_channel AS stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE YEAR(fcb.booking_completed_date) = 2018
  AND fcb.booking_status_type = 'live'
  AND fcb.shiro_user_id IS NOT NULL
GROUP BY 1;

-- check transaction model data
SELECT
    SUM(tub.margin_gross_of_toms_gbp_constant_currency)
FROM data_vault_mvp.bi.trx_union_bookings tub
WHERE tub.booking_status_type = 'live'
  AND YEAR(tub.date) = 2018
  AND tub.shiro_user_id IS NOT NULL;

-- cross reference with fact booking
SELECT
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.fact_complete_booking fcb
WHERE YEAR(fcb.booking_completed_timestamp) = 2018
  AND fcb.shiro_user_id IS NOT NULL;

-- fact booking and transaction model match
-- adjusting the lobster daily to remove the single customer view joins brings the sum of margin to match transaction model and fact booking

-- investigate difference of bookings that aren't in single customer view in 2018

SELECT
    fcb.booking_id
FROM se.data.fact_complete_booking fcb
WHERE YEAR(fcb.booking_completed_timestamp) = 2018
  AND fcb.shiro_user_id IS NOT NULL

EXCEPT

SELECT
    stt.booking_id
FROM se.data.scv_touched_transactions stt
WHERE YEAR(stt.event_tstamp) = 2018;


SELECT *
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_id = 'AB-60016586'

-- found a large number of air berlin bookings that were increasing the margin

-- when removing these it makes the number match within tolerance
SELECT
    SUM(tub.margin_gross_of_toms_gbp_constant_currency)
FROM data_vault_mvp.bi.trx_union_bookings tub
WHERE tub.booking_status_type = 'live'
  AND YEAR(tub.date) = 2018
  AND tub.shiro_user_id IS NOT NULL
  AND tub.tech_platform IN ('SECRET_ESCAPES', 'TRAVELBIRD');
--64,638,125

SELECT
    YEAR(fcb.booking_completed_date)                    AS year,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM dbt.bi_staging.base_dwh__fact_booking AS fcb
--     INNER JOIN dbt.bi_staging.base_dwh__user_attributes AS sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN dbt.bi_staging.base_scv__module_touched_transactions AS stt ON fcb.booking_id = stt.booking_id
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_attribution AS sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_marketing_channel AS stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE YEAR(fcb.booking_completed_date) = 2018
  AND fcb.booking_status_type = 'live'
  AND fcb.shiro_user_id IS NOT NULL
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------
-- investigate variance between transaction model and lobster monthly for 2019

-- transaction model
SELECT
    SUM(tub.margin_gross_of_toms_gbp_constant_currency)
FROM data_vault_mvp.bi.trx_union_bookings tub
WHERE tub.booking_status_type = 'live'
  AND YEAR(tub.date) = 2019
  AND tub.shiro_user_id IS NOT NULL
  AND tub.tech_platform IN ('SECRET_ESCAPES', 'TRAVELBIRD');

-- lobster monthly
SELECT
    SUM(dcmlpb.margin_gbp)
FROM dbt.bi_data_platform.dp_cohort_monthly_last_paid_bookings dcmlpb
WHERE YEAR(dcmlpb.event_month) = 2019

-- lobster daily
SELECT
    YEAR(fcb.booking_completed_date)                    AS year,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM dbt.bi_staging.base_dwh__fact_booking AS fcb
--     INNER JOIN dbt.bi_staging.base_dwh__user_attributes AS sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN dbt.bi_staging.base_scv__module_touched_transactions AS stt ON fcb.booking_id = stt.booking_id
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_attribution AS sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_marketing_channel AS stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE YEAR(fcb.booking_completed_date) = 2019
  AND fcb.booking_status_type = 'live'
  AND fcb.shiro_user_id IS NOT NULL
GROUP BY 1;

WITH groupup AS (
    SELECT
        YEAR(fcb.booking_completed_date)                    AS year,
        'daily'                                             AS source,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings
    FROM dbt.bi_staging.base_dwh__fact_booking AS fcb
--     INNER JOIN dbt.bi_staging.base_dwh__user_attributes AS sua ON fcb.shiro_user_id = sua.shiro_user_id
        INNER JOIN dbt.bi_staging.base_scv__module_touched_transactions AS stt ON fcb.booking_id = stt.booking_id
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_attribution AS sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_marketing_channel AS stmc ON sta.attributed_touch_id = stmc.touch_id
    WHERE YEAR(fcb.booking_completed_date) = 2019
      AND fcb.booking_status_type = 'live'
      AND fcb.shiro_user_id IS NOT NULL
    GROUP BY 1, 2

    UNION ALL

    SELECT
        YEAR(fcb.booking_completed_date)                    AS year,
        'prop'                                              AS source,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings
    FROM dbt.bi_staging.base_dwh__fact_booking fcb
        INNER JOIN dbt.bi_staging.base_dwh__user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    WHERE fcb.territory NOT IN ('TL', 'PL')
      AND fcb.booking_status_type = 'live'
      AND (
                fcb.booking_completed_date < '2018-01-01'
        )
      AND YEAR(fcb.booking_completed_timestamp) = 2019
    GROUP BY 1
)
SELECT
    year,
    g.source,
    SUM(g.margin_gbp)
FROM groupup g
GROUP BY 1, 2
;

-- check difference in booking ids

WITH groupup AS (
    SELECT
        fcb.booking_id
    FROM dbt.bi_staging.base_dwh__fact_booking AS fcb
--     INNER JOIN dbt.bi_staging.base_dwh__user_attributes AS sua ON fcb.shiro_user_id = sua.shiro_user_id
        INNER JOIN dbt.bi_staging.base_scv__module_touched_transactions AS stt ON fcb.booking_id = stt.booking_id
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_attribution AS sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
--     INNER JOIN dbt.bi_staging.base_scv__module_touch_marketing_channel AS stmc ON sta.attributed_touch_id = stmc.touch_id
    WHERE YEAR(fcb.booking_completed_date) = 2019
      AND fcb.booking_status_type = 'live'
      AND fcb.shiro_user_id IS NOT NULL

    UNION ALL

    SELECT
        fcb.booking_id
    FROM dbt.bi_staging.base_dwh__fact_booking fcb
        INNER JOIN dbt.bi_staging.base_dwh__user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    WHERE fcb.territory NOT IN ('TL', 'PL')
      AND fcb.booking_status_type = 'live'
      AND (
                fcb.booking_completed_date < '2018-01-01'
            OR
                (fcb.tech_platform = 'TRAVELBIRD' AND fcb.booking_completed_date < '2020-03-01') -- to account for travelbird bookings before tracking
        )
      AND YEAR(fcb.booking_completed_timestamp) = 2019
)

SELECT *
FROM groupup
    QUALIFY COUNT(*) OVER (PARTITION BY groupup.booking_id) > 1

-- removed travelbird input from code, single customer view was rerun to backfill history to 2018, the new artificial insemination code will insert tracy bookings
-- into the event stream too therefore no longer need to insert them this way.
-- https://github.com/secretescapes/dbt/pull/217

-- after removal and rerunning of the dbt model the numbers now reconcile in the workbook
-- https://eu-west-1a.online.tableau.com/#/site/secretescapes/views/Lobsterrec160123SR/Dashboard1
