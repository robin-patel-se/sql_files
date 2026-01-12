SELECT *
FROM se.data.tb_order_item toi
WHERE toi.booking_id = 'TB-22286633'


-- salesforce opp id: 0061r00001FKbLn;


SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_id = 'A18007525'
;

SELECT *
FROM se.data.se_offer_attributes soa
WHERE soa.se_offer_id = 'A12846'
;

------------------------------------------------------------------------------------------------------------------------
-- working out camilla bookings first

-- find offer ids for the hotel
SELECT
	soa.offer_name_object['en_GB']::VARCHAR AS en_offer_name,
	*
FROM se.data.se_offer_attributes soa
WHERE soa.hotel_code = '0011r00002SEi5n'
;

-- two offer ids with the suite affected: Junior suite with balcony, four nights or more (all-inclusive)
-- SE_OFFER_ID
-- A12846
-- A12847


-- bookings to the HOTEL with check in date in the future
SELECT *
FROM se.data.se_booking sb
	INNER JOIN se.data.se_sale_attributes ssa ON sb.se_sale_id = ssa.se_sale_id
WHERE ssa.salesforce_opportunity_id = '0061r00001FKbLn'
  AND sb.booking_status = 'COMPLETE'
  AND sb.check_in_date >= CURRENT_DATE
;

-- bookings to the HOTEL with check in date in the future for the relevant offer ids
SELECT *
FROM se.data.se_booking sb
	INNER JOIN se.data.se_sale_attributes ssa ON sb.se_sale_id = ssa.se_sale_id
WHERE ssa.salesforce_opportunity_id = '0061r00001FKbLn'
  AND sb.offer_id IN (
					  'A12846',
					  'A12847'
	)
  AND sb.booking_status = 'COMPLETE'
  AND sb.check_in_date >= CURRENT_DATE
;

------------------------------------------------------------------------------------------------------------------------
-- tracy
SELECT *
FROM se.data.tb_offer t
WHERE t.salesforce_opportunity_id = '0061r00001FKbLn'
-- 10 sales on tracy for that opp id

SELECT *
FROM se.data.tb_order_item toi
	INNER JOIN se.data.tb_booking tb ON toi.booking_id = tb.booking_id
	INNER JOIN se.data.tb_offer t ON tb.se_sale_id = t.se_sale_id
WHERE t.salesforce_opportunity_id = '0061r00001FKbLn'
  AND COALESCE(tb.accommodation_start_date, tb.travel_date) >= CURRENT_DATE
  AND toi.order_item_type = 'ACCOMMODATION'
  AND toi.allocation_unit_name = 'Junior Suite Balcony'



SHIRO_USER_ID
29676776
65427701
68052665
54803122
28292488
26179524
65847541
17816
17447410
25076332
25142239
43379722
72141789
80950172

