SELECT *
FROM se.data.se_credit_model scm
WHERE scm.credit_reason ILIKE '%COVID_19_CANCELLATION%';

SELECT DISTINCT
    scm.credit_status
FROM se.data.se_credit_model scm;
--
-- CREDIT_STATUS
-- USED_TB
-- LOCKED
-- USED
-- ACTIVE
-- DELETED
-- REFUNDED_CASH

SELECT *
FROM se.data.se_credit_model scm
WHERE scm.credit_reason ILIKE '%COVID_19_CANCELLATION%'
  AND scm.credit_date_created >= '2020-11-01';

--bookings with credit awarded
SELECT
    COUNT(DISTINCT scm.original_se_booking_id) AS bookings_with_canx_credit
FROM se.data.se_credit_model scm
    INNER JOIN se.data.fact_booking fb ON scm.original_se_booking_id = fb.booking_id
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE scm.credit_reason ILIKE '%COVID_19_CANCELLATION%'
  AND ds.product_configuration = 'Hotel'
  AND scm.credit_date_created >= '2020-11-01';

------------------------------------------------------------------------------------------------------------------------
--users with credit awarded
SELECT
    COUNT(DISTINCT scm.user_id) AS users_with_canx_credit
FROM se.data.se_credit_model scm
    INNER JOIN se.data.fact_booking fb ON scm.original_se_booking_id = fb.booking_id
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE scm.credit_reason ILIKE '%COVID_19_CANCELLATION%'
  AND ds.product_configuration = 'Hotel'
  AND scm.credit_date_created >= '2020-11-01';
------------------------------------------------------------------------------------------------------------------------
--users that have had any activity since credit creation
SELECT
    COUNT(DISTINCT ua.shiro_user_id) AS credit_users_whove_interacted_with_the_site
FROM se.data.user_activity ua
    INNER JOIN (
                   SELECT DISTINCT
                       scm.user_id,
                       MIN(scm.credit_date_created) AS credit_date
                   FROM se.data.se_credit_model scm
                       INNER JOIN se.data.fact_booking fb ON scm.original_se_booking_id = fb.booking_id
                       INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
                   WHERE scm.credit_reason ILIKE '%COVID_19_CANCELLATION%'
                     AND ds.product_configuration = 'Hotel'
                     AND scm.credit_date_created >= '2020-11-01'
                   GROUP BY 1
               ) cu ON ua.shiro_user_id = cu.user_id AND ua.date >= cu.credit_date
WHERE (ua.web_sessions_1d > 0 OR ua.app_sessions_1d > 0)
  AND ua.date >= '2020-11-01';
------------------------------------------------------------------------------------------------------------------------
--users that have redeemed the credit
SELECT
    COUNT(DISTINCT scm.user_id) AS users_with_redeemed_credit
FROM se.data.se_credit_model scm
    INNER JOIN se.data.fact_booking fb ON scm.original_se_booking_id = fb.booking_id
    INNER JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE scm.credit_reason ILIKE '%COVID_19_CANCELLATION%'
  AND ds.product_configuration = 'Hotel'
  AND scm.credit_date_created >= '2020-11-01'
  AND scm.redeemed_se_booking_id IS NOT NULL;


SELECT DATEADD(DAY, -1, DATE_TRUNC(MONTH, CURRENT_DATE))