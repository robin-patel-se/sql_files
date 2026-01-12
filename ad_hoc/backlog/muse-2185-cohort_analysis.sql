--2022 spvs
/*
--originally provided
-- SELECT
--     DATE_TRUNC(MONTH, sts.event_tstamp)  AS event_month,
--     DATE_TRUNC(MONTH, sua.signup_tstamp) AS sign_up_month,
--     COUNT(*)                             AS spvs
-- FROM se.data.scv_touched_spvs sts
--     INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
--     INNER JOIN se.data_pii.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
-- WHERE sts.event_tstamp >= '2022-01-01'
-- GROUP BY 1, 2;
*/

-- 2019 spvs
/*
SELECT
    DATE_TRUNC(MONTH, sts.event_tstamp)  AS event_month,
    DATE_TRUNC(MONTH, sua.signup_tstamp) AS sign_up_month,
    COUNT(*)                             AS spvs
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    INNER JOIN se.data_pii.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE sts.event_tstamp BETWEEN '2019-01-01' AND '2019-04-27'
  AND sua.signup_tstamp <= '2019-04-27'
GROUP BY 1, 2;
*/

-- 2022 spvs
SELECT
    DATE_TRUNC(MONTH, stba.touch_start_tstamp)   AS event_month,
    DATE_TRUNC(MONTH, sua.signup_tstamp)         AS sign_up_month,
    COUNT(DISTINCT sts.event_hash)               AS spvs,
    COUNT(DISTINCT stba.attributed_user_id_hash) AS mau
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id_hash = SHA2(sua.shiro_user_id)
    LEFT JOIN  se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
WHERE stba.touch_start_tstamp >= '2022-01-01'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2;


-- 2019 spvs
SELECT
    DATE_TRUNC(MONTH, stba.touch_start_tstamp)   AS event_month,
    DATE_TRUNC(MONTH, sua.signup_tstamp)         AS sign_up_month,
    COUNT(DISTINCT sts.event_hash)               AS spvs,
    COUNT(DISTINCT stba.attributed_user_id_hash) AS mau
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id_hash = SHA2(sua.shiro_user_id)
    LEFT JOIN  se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
WHERE stba.touch_start_tstamp BETWEEN '2019-01-01' AND '2019-05-09'
  AND sua.signup_tstamp <= '2019-05-09'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2;

-- 2022 margin
SELECT
    DATE_TRUNC(MONTH, fb.booking_completed_timestamp)                                                              AS booking_month,
    DATE_TRUNC(MONTH, sua.signup_tstamp)                                                                           AS sign_up_month,
    SUM(IFF(fb.booking_status_type IN ('live', 'cancelled'), fb.margin_gross_of_toms_gbp_constant_currency, NULL)) AS gross_margin,
    SUM(IFF(fb.booking_status_type IN ('live'), fb.margin_gross_of_toms_gbp_constant_currency, NULL))              AS net_margin
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_completed_date BETWEEN '2022-01-01' AND CURRENT_DATE - 1
GROUP BY 1, 2;


-- 2019 margin
SELECT
    DATE_TRUNC(MONTH, fb.booking_completed_timestamp)                                                              AS booking_month,
    DATE_TRUNC(MONTH, sua.signup_tstamp)                                                                           AS sign_up_month,
    SUM(IFF(fb.booking_status_type IN ('live', 'cancelled'), fb.margin_gross_of_toms_gbp_constant_currency, NULL)) AS gross_margin,
    SUM(IFF(fb.booking_status_type IN ('live'), fb.margin_gross_of_toms_gbp_constant_currency, NULL))              AS net_margin
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_completed_date BETWEEN '2019-01-01' AND '2019-04-27'
GROUP BY 1, 2;


-- 2022 email opens
SELECT
    DATE_TRUNC(MONTH, ceo.event_tstamp)  AS event_month,
    DATE_TRUNC(MONTH, sua.signup_tstamp) AS sign_up_month,
    COUNT(*)                             AS email_opens,
    COUNT(DISTINCT ceo.shiro_user_id)    AS email_mau
FROM se.data.crm_events_opens ceo
    INNER JOIN se.data.se_user_attributes sua ON ceo.shiro_user_id = sua.shiro_user_id
WHERE ceo.event_tstamp >= '2022-01-01'
GROUP BY 1, 2;

-- 2019 email opens
SELECT
    DATE_TRUNC(MONTH, ceo.event_tstamp)  AS event_month,
    DATE_TRUNC(MONTH, sua.signup_tstamp) AS sign_up_month,
    COUNT(*)                             AS email_opens,
    COUNT(DISTINCT ceo.shiro_user_id)    AS email_mau
FROM se.data.crm_events_opens ceo
    INNER JOIN se.data.se_user_attributes sua ON ceo.shiro_user_id = sua.shiro_user_id
WHERE ceo.event_tstamp BETWEEN '2019-01-01' AND '2019-05-09'
  AND sua.signup_tstamp <= '2019-05-09'
GROUP BY 1, 2;

