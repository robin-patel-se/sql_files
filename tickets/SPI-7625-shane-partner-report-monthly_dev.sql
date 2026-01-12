USE WAREHOUSE pipe_medium
;

SET reporting_month_start = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE))
; --first day of last month
SET reporting_month_end = LAST_DAY($reporting_month_start)
;

SET reporting_month_end_plus_day = DATEADD(DAY, 1, $reporting_month_end)
;
-----------------------------------------------------------------------------------------------------------
WITH
	cms_report AS (
		SELECT
			$reporting_month_start                                                   AS reporting_month,
			CASE
				WHEN affiliate ILIKE 'telegraph%' THEN 'Telegraph Media Group'
				WHEN affiliate ILIKE 'guardian%' THEN 'Guardian News & Media'
				WHEN affiliate ILIKE 'time out%' THEN 'Time Out'
			END                                                                      AS affiliate_group,
			salename,
			transactionid,
			'1'                                                                      AS bookings,
			adults + children + infants                                              AS pass,
			datebooked,
			checkin,
			checkout,
			totalsellrate,
			customertotalprice,
			creditsused,
			(commissionexvat - creditamountdeductiblefromcommission)                 AS commission_ex_vat,
			bookingfeenetrate,
			0.025                                                                    AS tx_fee_pc,
			customertotalprice * tx_fee_pc                                           AS tx_fee,
			0.5                                                                      AS partner_commission_pc,
			(commission_ex_vat + bookingfeenetrate - tx_fee) * partner_commission_pc AS partner_commission_ex_vat,
		FROM latest_vault.cms_reports.booking_summary
		WHERE datebooked >= DATEADD('year', -2, $reporting_month_start)
		  AND (affiliate ILIKE 'telegraph%' OR affiliate ILIKE 'guardian%' OR affiliate ILIKE 'time out%')
		  AND (commissionexvat - creditamountdeductiblefromcommission) > 0
		ORDER BY affiliate, datebooked
	),
-----------------------------------------------------------------------------------------------------------
--partner bookings based on date_booked (last month)
	partner_bookings AS (
		SELECT
			cms.*,
			NULL              AS refund_type,
			NULL              AS cancellation_date,
			0                 AS valid_cancellation,
			'partner booking' AS dataset
		FROM cms_report cms
		WHERE datebooked BETWEEN $reporting_month_start AND $reporting_month_end_plus_day
	),
--partner cancellations based on check_in_date (last month)
	partner_cancellations AS (
		SELECT
			cms.* RENAME bookings AS cancellations, seb.refund_type, seb.cancellation_date,
			--cancellation business logic
			CASE
				WHEN DATE_TRUNC(MONTH, seb.cancellation_date) = DATE_TRUNC(MONTH, cms.datebooked)
					THEN 0 --INVALID - booked and cancelled in the same month
				WHEN refund_type = 'PARTIAL' THEN 0 --INVALID - incomplete refund
				ELSE 1
			END AS valid_cancellation, 'partner cancellation' AS dataset
		FROM cms_report cms
		INNER JOIN (
					   SELECT
						   transaction_id,
						   refund_type,
						   cancellation_date
					   FROM se.data.se_booking
					   WHERE cancellation_date IS NOT NULL
				   ) seb
					   ON seb.transaction_id = cms.transactionid
		WHERE checkin BETWEEN $reporting_month_start AND $reporting_month_end_plus_day
	)
SELECT *
FROM partner_bookings
WHERE datebooked BETWEEN $reporting_month_start AND $reporting_month_end
UNION ALL
SELECT *
FROM partner_cancellations
WHERE checkin BETWEEN $reporting_month_start AND $reporting_month_end
;


SELECT *
FROM latest_vault.cms_reports.booking_summary
;


SELECT *
FROM latest_vault.cms_mysql.external_booking eb
;


SELECT *
FROM se.data.se_user_attributes sua
WHERE LOWER(sua.original_affiliate_name) LIKE ANY ('telegraph%', 'guardian%', 'time out%')
;



------------------------------------------------------------------------------------------------------------------------
-- revised
USE WAREHOUSE pipe_medium
;

SET reporting_month_start = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE))
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
			CASE
				WHEN LOWER(user_attributes.original_affiliate_name) LIKE 'telegraph%' THEN 'Telegraph Media Group'
				WHEN LOWER(user_attributes.original_affiliate_name) LIKE 'guardian%' THEN 'Guardian News & Media'
				WHEN LOWER(user_attributes.original_affiliate_name) LIKE 'time out%' THEN 'Time Out'
			END                                                                                AS affiliate_group,
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
-- 	fact_booking.margin_gross_of_toms_gbp,
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
			fact_booking.cancellation_date,
			fact_booking.booking_status_type,
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
		INNER JOIN se.data.se_user_attributes user_attributes
					   ON fact_booking.shiro_user_id = user_attributes.shiro_user_id
		LEFT JOIN  se.data.se_booking
					   ON fact_booking.booking_id = se_booking.booking_id
		LEFT JOIN  se.data.tb_booking
					   ON fact_booking.booking_id = tb_booking.booking_id
		WHERE fact_booking.booking_status_type IN ('live', 'cancelled')
		  AND LOWER(user_attributes.original_affiliate_name) LIKE ANY ('telegraph%', 'guardian%', 'time out%')
		  AND fact_booking.margin_gross_of_toms_gbp > credits_deducable_from_commission
	),
-----------------------------------------------------------------------------------------------------------
--partner bookings based on date_booked (last month)
	partner_bookings AS (
		SELECT
			gross_bookings.*
			EXCLUDE (booking_status_type, refund_type, cancellation_date), NULL AS refund_type, NULL AS cancellation_date, 0 AS valid_cancellation, 'partner booking' AS dataset
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


SELECT * FROM se.data.tb_booking tb