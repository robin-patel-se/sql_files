SELECT
	fact_booking.booking_id,
	fact_booking.check_in_date,
	DAYNAME(fact_booking.check_in_date)                                                AS check_in_day_name,
	fact_booking.check_out_date,
	fact_booking.booking_completed_date,
	calendar.is_year_to_date,
	calendar.se_week,
	fact_booking.adult_guests,
	fact_booking.child_guests,
	fact_booking.infant_guests,
	fact_booking.adult_guests + fact_booking.child_guests + fact_booking.infant_guests AS total_guests,
	fact_booking.no_nights,
	fact_booking.margin_gross_of_toms_gbp_constant_currency,
	dim_sale.posu_country,
	dim_sale.posa_territory,
FROM se.data.fact_booking
INNER JOIN se.data.dim_sale
	ON fact_booking.se_sale_id = dim_sale.se_sale_id
INNER JOIN se.data.se_calendar calendar
	ON fact_booking.booking_completed_date = calendar.date_value
WHERE fact_booking.booking_status_type IN ('live', 'cancelled')
  AND fact_booking.se_brand = 'SE Brand'
  AND fact_booking.booking_completed_date >= '2023-01-01'
