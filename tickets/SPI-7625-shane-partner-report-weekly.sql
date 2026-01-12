-- revised
USE WAREHOUSE pipe_medium
;

SET week_commencing = '2025-08-04'
;

-----

SELECT
	$week_commencing                                                             AS week_commencing,
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
	fact_booking.booking_status_type,
-- need to work out refund type COALESCE(se_booking.refund_type,
FROM se.data.fact_booking
INNER JOIN se.data.dim_sale
	ON fact_booking.se_sale_id = dim_sale.se_sale_id

INNER JOIN se.data.se_user_attributes user_attributes
	ON fact_booking.shiro_user_id = user_attributes.shiro_user_id
LEFT JOIN se.data.se_booking
	ON fact_booking.booking_id = se_booking.booking_id
LEFT JOIN se.data.tb_booking
	ON fact_booking.booking_id = tb_booking.booking_id
WHERE fact_booking.booking_status_type IN ('live', 'cancelled')
  AND LOWER(user_attributes.original_affiliate_name) LIKE ANY ('telegraph%', 'guardian%', 'time out%')
  AND fact_booking.margin_gross_of_toms_gbp > credits_deducable_from_commission
  -- remove bookings to tracy packages
  AND dim_sale.product_configuration IS DISTINCT FROM 'Catalogue'
  AND DATE_TRUNC('week', fact_booking.booking_completed_date) = $week_commencing
