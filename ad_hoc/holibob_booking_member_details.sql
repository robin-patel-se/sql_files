-- camilla
SELECT
	fcb.booking_id,
	r.passenger_first_name AS first_name,
	r.passenger_last_name AS last_name,
	bs.record__o['customerEmail']::VARCHAR AS email,
	r.passenger_phone_number AS phone
FROM se.data.fact_complete_booking fcb
	INNER JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON fcb.booking_id = bs.booking_id
	INNER JOIN latest_vault.cms_mysql.reservation r ON fcb.booking_id = r.booking_id
WHERE fcb.booking_completed_date >= CURRENT_DATE - 1
;

-- tracy
SELECT
	fcb.booking_id,
	op.first_name,
	op.last_name,
	op.email,
	op.phone
FROM se.data.fact_complete_booking fcb
	INNER JOIN latest_vault.travelbird_mysql.orders_order oo ON 'TB-' || oo.id = fcb.booking_id
	INNER JOIN latest_vault.travelbird_mysql.orders_person op ON oo.customer_id = op.id
WHERE fcb.booking_completed_date >= CURRENT_DATE - 1
;


SELECT * FROm dbt.bi_product_analytics__intermediate.pda_session_metrics psm