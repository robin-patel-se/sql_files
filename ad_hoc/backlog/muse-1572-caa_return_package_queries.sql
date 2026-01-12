-- Live (i.e. not cancelled) UK Package bookings booked in a specific month (TTV + PAX) -- aggregated and list

SELECT DATE_TRUNC(MONTH, fcb.booking_completed_date) AS booking_month,
       ds.product_configuration,
       SUM(fcb.gross_revenue_gbp)                    AS ttv,
       SUM(fcb.adult_guests + fcb.child_guests)      AS pax
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fcb.booking_completed_date) = DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND fcb.territory = 'UK'
  AND fcb.booking_includes_flight
GROUP BY 1, 2
;

SELECT fcb.booking_id,
       fcb.booking_status,
       fcb.booking_status_type,
       fcb.booking_completed_date,
       fcb.check_in_date,
       ds.product_configuration,
       fcb.gross_revenue_gbp,
       fcb.adult_guests,
       fcb.child_guests,
       fcb.territory,
       fcb.booking_includes_flight
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fcb.booking_completed_date) = DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND fcb.territory = 'UK'
  AND fcb.booking_includes_flight
;

-- Live UK Package bookings departing in a specified month (TTV + PAX)
SELECT DATE_TRUNC(MONTH, fcb.check_in_date)     AS check_in_month,
       ds.product_configuration,
       SUM(fcb.gross_revenue_gbp)               AS ttv,
       SUM(fcb.adult_guests + fcb.child_guests) AS pax
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fcb.check_in_date) = DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND DATE_TRUNC(MONTH, fcb.booking_completed_timestamp) <= DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND fcb.territory = 'UK'
  AND fcb.booking_includes_flight
GROUP BY 1, 2
;

SELECT fcb.booking_id,
       fcb.booking_status,
       fcb.booking_status_type,
       fcb.booking_completed_date,
       fcb.check_in_date,
       ds.product_configuration,
       fcb.gross_revenue_gbp,
       fcb.adult_guests,
       fcb.child_guests,
       fcb.territory,
       fcb.booking_includes_flight
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fcb.check_in_date) = DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND DATE_TRUNC(MONTH, fcb.booking_completed_timestamp) <= DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND fcb.territory = 'UK'
  AND fcb.booking_includes_flight
;


-- Live total UK Package bookings due to travel in the future (TTV + PAX)
SELECT DATE_TRUNC(MONTH, fcb.check_in_date)     AS check_in_month,
       ds.product_configuration,
       SUM(fcb.gross_revenue_gbp)               AS ttv,
       SUM(fcb.adult_guests + fcb.child_guests) AS pax
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fcb.check_in_date) >= DATE_TRUNC(MONTH, CURRENT_DATE)
  AND DATE_TRUNC(MONTH, fcb.booking_completed_timestamp) <= DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND fcb.territory = 'UK'
  AND fcb.booking_includes_flight
GROUP BY 1, 2
ORDER BY 1
;

SELECT fcb.booking_id,
       fcb.booking_status,
       fcb.booking_status_type,
       fcb.booking_completed_date,
       fcb.check_in_date,
       ds.product_configuration,
       fcb.gross_revenue_gbp,
       fcb.adult_guests,
       fcb.child_guests,
       fcb.territory,
       fcb.booking_includes_flight
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fcb.check_in_date) >= DATE_TRUNC(MONTH, CURRENT_DATE)
  AND DATE_TRUNC(MONTH, fcb.booking_completed_timestamp) <= DATE_TRUNC(MONTH, DATEADD(MONTH, -1, CURRENT_DATE))
  AND fcb.territory = 'UK'
  AND fcb.booking_includes_flight
;

SELECT GET_DDL('table', 'se.data.partner_affiliate_param');

DESCRIBE FUNCTION se.data.partner_affiliate_param(VARCHAR);

SELECT ces.event_tstamp::DATE AS date,
       COUNT(*)               AS sends
FROM se.data.crm_events_sends ces
WHERE ces.event_tstamp >= '2021-12-05'
GROUP BY 1


SELECT * FROM hygiene_snapshot_vault_mvp.cms_mysql.product_reservation pr;