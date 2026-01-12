-- query one
SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.user_attributes sua
;

-- query two
SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.user_attributes sua
LIMIT 1
;


-- query three
SELECT
	DATE_TRUNC(MONTH, sua.signup_tstamp) AS month,
	sua.main_affiliate_brand,
	COUNT(*)
FROM data_vault_mvp.dwh.user_attributes sua
GROUP BY 1, 2
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	fcb.booking_completed_date,
	COUNT(DISTINCT fcb.booking_id)                      AS bookings,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS sum_of_margin_gross_of_toms_gbp_constant_currency,
	SUM(fcb.gross_revenue_gbp_constant_currency)        AS sum_of_gross_revenue_gbp_constant_currency
FROM se.data.fact_complete_booking fcb
GROUP BY 1
;



SELECT sts.travel_types,
       array_to_string(sts.travel_types, ', ')
FROM se.data.scv_touched_searches sts
WHERE sts.travel_types IS NOT NULL