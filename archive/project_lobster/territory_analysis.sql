SELECT
    DATE_TRUNC(MONTH, fcb.booking_completed_timestamp)  AS booking_month,
    sua.original_affiliate_territory,
    fcb.territory,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.fact_complete_booking fcb
    LEFT JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
GROUP BY 1, 2, 3;

--bookings without completed date
SELECT
    COUNT(*)
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_timestamp IS NULL;
-- 12295

SELECT
    fcb.tech_platform,
    COUNT(*)
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_timestamp IS NULL
GROUP BY 1;
-- appear to be almost all chiasma tech platform bookings (1 secret escapes)

--bookings without shiro user
SELECT
    COUNT(*)
FROM se.data.fact_complete_booking fcb
WHERE fcb.shiro_user_id IS NULL;
--1,253,656

SELECT
    fcb.tech_platform,
    COUNT(*)
FROM se.data.fact_complete_booking fcb
WHERE fcb.shiro_user_id IS NULL
GROUP BY 1;
--majority are travelist bookings


SELECT
    DATE_TRUNC(MONTH, fcb.booking_completed_date) AS month,
    COUNT(*)
FROM se.data.fact_complete_booking fcb
WHERE fcb.shiro_user_id IS NULL
GROUP BY 1;
--still occurring today

------------------------------------------------------------------------------------------------------------------------


SELECT
    DATE_TRUNC(MONTH, fcb.booking_completed_timestamp)  AS booking_month,
    sua.original_affiliate_territory = fcb.territory    AS do_territories_match,
    fcb.shiro_user_id IS NOT NULL                       AS has_shiro_user_id,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.fact_complete_booking fcb
    LEFT JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
GROUP BY 1, 2, 3;



SELECT
    DATE_TRUNC(MONTH, fcb.booking_completed_timestamp) AS booking_month,
    sua.original_affiliate_territory,
    fcb.shiro_user_id,
    fcb.territory,
    fcb.margin_gross_of_toms_gbp_constant_currency
FROM se.data.fact_complete_booking fcb
    LEFT JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE sua.original_affiliate_territory IS DISTINCT FROM fcb.territory;



SELECT
    fcb.shiro_user_id IS NULL,
    COUNT(*)
FROM se.data.fact_complete_booking fcb
    LEFT JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE sua.original_affiliate_territory IS DISTINCT FROM fcb.territory
GROUP BY 1;

-- Most non matches are due to no user id being on the booking

------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_vault.stripe.charges c;