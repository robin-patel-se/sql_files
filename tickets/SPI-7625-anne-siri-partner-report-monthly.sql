--first day of last month

-- SELECT
-- 	fact_booking.booking_id,
-- 	-- CONTRACTOR
-- 	dim_sale.se_sale_id,
-- 	dim_sale.sale_name,
-- 	se_booking.offer_name,
-- 	fact_booking.shiro_user_id,
-- 	se_booking.affiliate,
-- 	user_attributes.original_affiliate_name,
-- 	fact_booking.booking_completed_date,
-- 	fact_booking.check_in_date,
-- 	fact_booking.check_out_date,
-- 	fact_booking.no_nights,
-- 	fact_booking.adult_guests,
-- 	fact_booking.child_guests,
-- 	fact_booking.infant_guests,
-- 	fact_booking.currency,
-- 	fact_booking.territory,
-- 	fact_booking.gross_revenue_cc,
-- 	ROUND(fact_booking.gross_revenue_gbp / fact_booking.gross_revenue_cc, 2)               AS rate_to_gbp,
-- 	fact_booking.gross_revenue_gbp,
--
--
-- 	COALESCE(se_booking.credits_used_gbp, tb_booking.se_credit_redeemed_amount_gbp,
-- 			 0)                                                                            AS credits_redeeemd_gbp,
-- -- 	fact_booking.margin_gross_of_toms_gbp,
-- 	IFF(credits_redeeemd_gbp > (fact_booking.gross_revenue_gbp - fact_booking.margin_gross_of_toms_gbp),
-- 		credits_redeeemd_gbp - (fact_booking.gross_revenue_gbp - fact_booking.margin_gross_of_toms_gbp),
-- 		0)                                                                                 AS credits_deducable_from_commission,
-- 	fact_booking.margin_gross_of_toms_gbp - COALESCE(credits_deducable_from_commission, 0) AS commission_ex_vat,
-- 	dim_sale.sale_type,
-- 	fact_booking.transaction_id,
-- 	dim_sale.posu_city,
-- 	dim_sale.posu_division,
-- 	dim_sale.posu_country,
-- 	user_attributes.signup_tstamp,
-- FROM se.data.fact_booking
-- INNER JOIN se.data.dim_sale
-- 	ON fact_booking.se_sale_id = dim_sale.se_sale_id
-- INNER JOIN se.data.se_user_attributes user_attributes
-- 	ON fact_booking.shiro_user_id = user_attributes.shiro_user_id
-- LEFT JOIN se.data.se_booking
-- 	ON fact_booking.booking_id = se_booking.booking_id
-- LEFT JOIN se.data.tb_booking
-- 	ON fact_booking.booking_id = tb_booking.booking_id
-- WHERE fact_booking.booking_status_type IN ('live', 'cancelled')
-- --   AND user_attributes.original_affiliate_name LIKE ANY ('PS_DE_RS_PA_Urlaubsguru_Open Site SE') -- TODO ADJUST
--   AND fact_booking.booking_id IN (
-- 								  'A24654231',
-- 								  'A24654892',
-- 								  'A24655385',
-- 								  'A24655551'
-- 	)
--   AND fact_booking.margin_gross_of_toms_gbp > credits_deducable_from_commission
--   -- remove bookings to tracy packages
--   AND dim_sale.product_configuration IS DISTINCT FROM 'Catalogue'
--


------------------------------------------------------------------------------------------------------------------------
-- revised
USE WAREHOUSE marketing_pipe_medium
;

SET reporting_month_start = DATE_TRUNC('month', DATEADD('month', -2, CURRENT_DATE))
; --first day of last month
SET reporting_month_end = LAST_DAY($reporting_month_start)
;

SET reporting_month_end_plus_day = DATEADD(DAY, 1, $reporting_month_end)
;
-----

WITH
	whitelabel_bookings AS (
		SELECT
			$reporting_month_start                                                             AS reporting_month,
			-- we are required to pay fees on all bookings for customers with original affiliate
			user_attributes.original_affiliate_name                                            AS affiliate_group,
			dim_sale.sale_name,
			fact_booking.transaction_id,
			1                                                                                  AS bookings,
			fact_booking.adult_guests + fact_booking.child_guests + fact_booking.infant_guests AS passengers,
			fact_booking.booking_completed_date,
			fact_booking.check_in_date,
			fact_booking.check_out_date,
			fact_booking.gross_revenue_gbp,
			fact_booking.gross_revenue_gbp                                                     AS customer_total_price,
			COALESCE(se_booking.credits_used_gbp, tb_booking.se_credit_redeemed_amount_gbp,
					 0)                                                                        AS credits_redeeemd_gbp,
--         fact_booking.margin_gross_of_toms_gbp,
			IFF(credits_redeeemd_gbp > (fact_booking.gross_revenue_gbp - fact_booking.margin_gross_of_toms_gbp),
				credits_redeeemd_gbp - (fact_booking.gross_revenue_gbp - fact_booking.margin_gross_of_toms_gbp),
				0)                                                                             AS credits_deducable_from_commission,
			fact_booking.margin_gross_of_toms_gbp -
			COALESCE(credits_deducable_from_commission, 0)                                     AS commission_ex_vat,

			-- calculate amount to be removed from commission if credits redeemed outweigh the non margin amount
			0.025                                                                              AS tx_fee_pc,
			fact_booking.gross_revenue_gbp * tx_fee_pc                                         AS tx_fee,
			0.5                                                                                AS partner_commission_pc,
			(commission_ex_vat - tx_fee) * partner_commission_pc                               AS partner_commission_ex_vat,
			fact_booking.booking_status_type,
			fact_booking.cancellation_date,
			-- in the absense of being able to deduce if a refund on a tracy booking is fully or partially refunded
			-- using a proxy of if a refund amount is greater or equal to 90% of the sold price then consider it fully
			-- refunded
			COALESCE(
					se_booking.refund_type,
					CASE
						WHEN fact_booking.booking_status_type = 'cancelled'
							AND tb_booking.refund_actual_amount_gbp / NULLIF(tb_booking.sold_price_total_gbp, 0) < 0.9
							THEN 'PARTIAL'
						WHEN fact_booking.booking_status_type = 'cancelled'
							AND tb_booking.refund_actual_amount_gbp / NULLIF(tb_booking.sold_price_total_gbp, 0) >= 0.9
							THEN 'FULL'
					END
			)                                                                                  AS refund_type
		-- need to work out refund type COALESCE(se_booking.refund_type,
		FROM se.data.fact_booking
		INNER JOIN se.data.dim_sale
			ON fact_booking.se_sale_id = dim_sale.se_sale_id

		LEFT JOIN se.data.se_booking
			ON fact_booking.booking_id = se_booking.booking_id
		LEFT JOIN se.data.tb_booking
			ON fact_booking.booking_id = tb_booking.booking_id
		INNER JOIN se.data.harmonised_user_attributes user_attributes
			ON IFF(fact_booking.shiro_user_id IS NOT NULL, 'SE-' || fact_booking.shiro_user_id,
				   'AF-' || se_booking.affiliate_user_id) = user_attributes.user_identifier
		WHERE fact_booking.booking_status_type IN ('live', 'cancelled')
		  AND fact_booking.margin_gross_of_toms_gbp > credits_deducable_from_commission
		  AND user_attributes.original_affiliate_name IN
			  (
			   'First Class & More',
			   'First Class & More TPT',
			   'First Class & More €20',
			   'First Class and More 24h',
			   'PS_DE_RS_SE_First Class & More_100EUR',
			   'PS_DE_RS_SE_First Class & More_50EUR',
			   'PS_DE_RS_SE_First Class & More_Banner',
			   'PS_DE_RS_SE_First Class & More_Gewinnspiel',
			   'PS_CH_RS_PA_Holidayguru CH_Open Site Site SE',
			   'PS_CH_RS_SE_Holidayguru CH_CHF 80',
			   'PS_CH_RS_SE_Holidayguru CH_Closed Site SE',
			   'Holiday Pirates',
			   'Holiday Pirates DE Affiliate',
			   'Holiday Pirates IT',
			   'PS_AT_RS_PA_Holiday Pirates',
			   'PS_AT_RS_PA_Holiday Pirates_Open Site',
			   'PS_AT_RS_PA_Holiday Pirates_Open Site SE',
			   'PS_AT_RS_PA_Holiday Pirates_Open Site SE - 59 share',
			   'PS_AT_RS_PA_Holiday Pirates_Open Site SE - 62 share',
			   'PS_AT_RS_PA_Holiday Pirates_Open Site SE - 65 share',
			   'PS_AT_RS_SE_Holiday Pirates_Closed',
			   'PS_CH_RS_PA_Holiday Pirates',
			   'PS_CH_RS_PA_Holiday Pirates_Open Site',
			   'PS_CH_RS_PA_Holiday Pirates_Open Site SE',
			   'PS_CH_RS_PA_Holiday Pirates_Open Site SE - 59 share',
			   'PS_CH_RS_SE_Holiday Pirates_Closed Site SE',
			   'PS_DE_CPL_SE_Holiday Pirates_Closed Site 20',
			   'PS_DE_CPL_SE_Holiday Pirates_Closed Site Paid Post',
			   'PS_DE_CPL_SE_Holiday Pirates_Open Site',
			   'PS_DE_RS_PA_Holiday Pirates_Open Site SE',
			   'PS_DE_RS_PA_Holiday Pirates_Open Site SE - 59 share',
			   'PS_DE_RS_PA_Holiday Pirates_Open Site SE - 62 share',
			   'PS_DE_RS_PA_Holiday Pirates_Open Site SE - 65 share',
			   'PS_DE_RS_SE_Holiday Pirates_100EUR',
			   'PS_DE_RS_SE_Holiday Pirates_50EUR',
			   'PS_DE_RS_SE_Holiday Pirates_80EUR',
			   'PS_DE_RS_SE_Holiday Pirates_Closed',
			   'PS_IT_RS_PA_Holiday Pirates_Closed Site SE',
			   'PS_IT_RS_PA_Holiday Pirates_Open Site SE',
			   'PS_IT_RS_PA_Holiday Pirates_Open Site SE',
			   'PS_IT_RS_PA_Holiday Pirates_Open Site SE_59share',
			   'PS_IT_RS_SE_Holiday Pirates_Closed Site SE',
			   'Mitarbeitervorteile',
			   'PS_AT_RS_SE_Mitarbeitervorteile',
			   'PS_CH_RS_SE_Mitarbeitervorteile',
			   'PS_CH_RS_SE_Mitarbeitervorteile_20 CHF',
			   'PS_DE_RS_SE_Mitarbeitereisevorteile_100EUR',
			   'PS_DE_RS_SE_Mitarbeitereisevorteile_25EUR',
			   'PS_DE_RS_SE_Traveldealz EU',
			   'Applitools Whitelabel Travelbook',
			   'PS_DE_RS_SE_Travelbook_24h',
			   'PS_DE_RS_SE_Travelbook_50EUR',
			   'PS_DE_RS_SE_Travelbook_50EUR2',
			   'PS_DE_RS_SE_Travelbook_Open Site',
			   'Travelbook Bild.de',
			   'Travelbook Bild.de + €15',
			   'Travelbook Escapes',
			   'Travelbook Stylebook',
			   'Travelbook Stylebook + €15',
			   'PS_AT_RS_PA_Urlaubsguru_Open Site',
			   'PS_AT_RS_PA_Urlaubsguru_Open Site SE',
			   'PS_AT_RS_SE_Urlaubsguru_50EUR',
			   'PS_AT_RS_SE_Urlaubsguru_Closed',
			   'PS_CH_CPL_SE_Urlaubsguru_Calendar',
			   'PS_CH_RS_SE_Urlaubsguru_100CHF',
			   'PS_DE_CPL_SE_Urlaubsguru_Calendar',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE App',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE Blog',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE FB',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE FB Messenger',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE Insta',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE NL',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site SE WA',
			   'PS_DE_RS_PA_Urlaubsguru_Open Site_v2',
			   'PS_DE_RS_SE_Urlaubsguru_100EUR',
			   'PS_DE_RS_SE_Urlaubsguru_40EUR',
			   'PS_DE_RS_SE_Urlaubsguru_Cashback15',
			   'PS_DE_RS_SE_Urlaubsguru_Closed_15',
			   'PS_DE_RS_SE_Urlaubsguru_b',
			   'PS_DE_RS_Urlaubsguru_Gewinnspiel_closed',
			   'Urlaubsguru AT',
			   'Urlaubsguru AT new - Austria',
			   'Urlaubsguru CH new - Switzerland',
			   'Urlaubsguru DE',
			   'Urlaubsguru DE 15',
			   'Urlaubsguru DE new - Germany',
			   'PS_DE_RS_SE_Urlaubshamster_Closed',
			   'PS_DE_RS_SE_Urlaubshamster_Open Site',
			   'PS_AT_RS_PA_UrlaubstrackerAT_Open Site SE',
			   'PS_AT_RS_SE_Urlaubstracker_Closed',
			   'PS_DE_RS_PA_Urlaubstracker_Open Site SE',
			   'PS_DE_RS_PA_Urlaubstracker_Open Site SE_54share',
			   'PS_DE_RS_PA_Urlaubstracker_Open Site_Header',
			   'PS_DE_RS_SE_Urlaubstracker_40EUR',
			   'PS_DE_RS_SE_Urlaubstracker_Cashback15',
			   'PS_DE_RS_SE_Urlaubstracker_Closed'
				  )

	),
-----------------------------------------------------------------------------------------------------------
--partner bookings based on date_booked (last month)
	partner_bookings AS (
		SELECT
			gross_bookings.* EXCLUDE (booking_status_type, cancellation_date, refund_type),
			NULL AS cancellation_date,
			NULL AS refund_type,
			0 AS valid_cancellation, 'partner booking' AS dataset
		FROM whitelabel_bookings gross_bookings
		WHERE booking_completed_date BETWEEN $reporting_month_start AND $reporting_month_end_plus_day
	),
--partner cancellations based on check_in_date (last month)
	partner_cancellations AS (
		SELECT
			cancelled_bookings.* EXCLUDE (booking_status_type) RENAME bookings AS cancellations,
			--cancellation business logic
			CASE
				WHEN DATE_TRUNC(MONTH, cancelled_bookings.cancellation_date) =
					 DATE_TRUNC(MONTH, cancelled_bookings.booking_completed_date)
					THEN 0 --INVALID - booked and cancelled in the same month
				WHEN refund_type = 'PARTIAL' THEN 0 --INVALID - incomplete refund
				ELSE 1
			END AS valid_cancellation, 'partner cancellation' AS dataset
		FROM whitelabel_bookings cancelled_bookings
		WHERE cancelled_bookings.booking_status_type = 'cancelled'
		  AND cancelled_bookings.check_in_date BETWEEN $reporting_month_start AND $reporting_month_end_plus_day
	)
SELECT *
FROM partner_bookings
WHERE booking_completed_date BETWEEN $reporting_month_start AND $reporting_month_end
UNION ALL
SELECT *
FROM partner_cancellations
WHERE check_in_date BETWEEN $reporting_month_start AND $reporting_month_end
;

