SELECT *
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.fact_booking fb ON stt.booking_id = fb.booking_id
WHERE stt.event_tstamp >= CURRENT_DATE - 1


SELECT DISTINCT
	ssa.posu_country
FROM se.data.se_sale_attributes ssa
WHERE ssa.posa_territory = 'IE'


SELECT
	*
FROM se.data.dim_sale ds