WITH
	calculating_data AS (
		SELECT
			fcb.booking_id,
			fcb.territory,
			ds.product_configuration,
			fcb.gross_revenue_gbp_constant_currency,
			fcb.margin_gross_of_toms_gbp_constant_currency,
			fcb.margin_gross_of_toms_gbp_constant_currency / fcb.gross_revenue_gbp_constant_currency AS take_rate,
			fcb.no_nights,
			ROUND(fcb.gross_revenue_gbp_constant_currency / fcb.no_nights)                           AS price_per_night_gbp_constant_currency,
			CASE
				WHEN RIGHT(price_per_night_gbp_constant_currency, 1) IN (5, 9)
					THEN price_per_night_gbp_constant_currency
				WHEN RIGHT(price_per_night_gbp_constant_currency, 1) <= 5 THEN
					(5 - RIGHT(price_per_night_gbp_constant_currency, 1)) + price_per_night_gbp_constant_currency
				WHEN RIGHT(price_per_night_gbp_constant_currency, 1) <= 9 THEN
					(9 - RIGHT(price_per_night_gbp_constant_currency, 1)) + price_per_night_gbp_constant_currency
			END                                                                                      AS rounded_up_price_per_night,
			rounded_up_price_per_night * fcb.no_nights                                               AS rounded_up_gross_revenue_gbp_constant_currency,
			rounded_up_gross_revenue_gbp_constant_currency * take_rate                               AS rounded_up_margin_gross_of_toms_gbp_constant_currency

		FROM se.data.fact_complete_booking fcb
			INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
		WHERE fcb.se_brand = 'SE Brand' AND fcb.booking_completed_date >= '2024-01-01'
	)
SELECT
	SUM(cd.gross_revenue_gbp_constant_currency)                AS gross_revenue_gbp_constant_currency,
	SUM(rounded_up_gross_revenue_gbp_constant_currency)        AS rounded_up_gross_revenue_gbp_constant_currency,
	SUM(margin_gross_of_toms_gbp_constant_currency)            AS margin_gross_of_toms_gbp_constant_currency,
	SUM(rounded_up_margin_gross_of_toms_gbp_constant_currency) AS rounded_up_margin_gross_of_toms_gbp_constant_currency
FROM calculating_data cd;



