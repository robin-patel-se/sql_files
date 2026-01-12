SELECT
	PARSE_URL(ses.page_url, 1)['path']::VARCHAR AS url_path,
	*
FROM se.data_pii.scv_event_stream ses
WHERE DATE_TRUNC(MONTH, ses.event_tstamp) = '2025-07-01'
  AND PARSE_URL(ses.page_url, 1)['path']::VARCHAR LIKE 'flights%'
  AND ses.event_name = 'page_view'
--   AND ses.is_server_side_event = FALSE


SELECT
	se.data.page_url_categorisation(ses.page_url) AS page_category,
	COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE DATE_TRUNC(MONTH, ses.event_tstamp) = '2025-07-01'
--   AND PARSE_URL(ses.page_url, 1)['path']::VARCHAR LIKE 'flights%'
  AND ses.se_action = 'select_flights'
GROUP BY ALL
;


WITH
	clicks_to_flights AS (
		SELECT
			ses.event_name,
			ses.event_tstamp,
			ses.se_category,
			ses.se_action,
			ses.se_label,
			ses.se_property,
			ses.page_url,
			se.data.page_url_categorisation(ses.page_url) AS page_category,
			ses.page_referrer
		FROM se.data_pii.scv_event_stream ses
		WHERE DATE_TRUNC(MONTH, ses.event_tstamp) = '2025-07-01'
--   AND PARSE_URL(ses.page_url, 1)['path']::VARCHAR LIKE 'flights%'
		  AND ses.se_action = 'select_flights'
		  AND ses.se_brand = 'SE Brand'
	),
	agg_clicks AS (
		SELECT
			clicks_to_flights.event_tstamp::DATE AS date,
			COUNT(*)                             AS clicks_to_flights
		FROM clicks_to_flights
		GROUP BY ALL
	)
		,
	spvs AS (
		SELECT
			fsm.date,
			fsm.se_sale_id,
			fsm.member_spvs
		FROM se.bi.fact_sale_metrics fsm
		INNER JOIN se.bi.dim_sale_territory dst
			ON fsm.se_sale_id = dst.se_sale_id AND fsm.posa_territory = dst.posa_territory
		WHERE DATE_TRUNC(MONTH, fsm.date) = '2025-07-01'
		  AND dst.posa_territory IS DISTINCT FROM 'PL'
		  AND dst.product_configuration = 'Hotel Plus'
	),
	agg_spvs AS (
		SELECT
			spvs.date,
			SUM(spvs.member_spvs) AS member_spvs
		FROM spvs
		GROUP BY 1
	)

SELECT
	spvs.date,
	spvs.member_spvs,
	clicks_to_flights
FROM agg_spvs spvs
LEFT JOIN agg_clicks clicks
	ON spvs.date = clicks.date

-- https://docs.google.com/spreadsheets/d/1xR4D9ae1AOUZPqpOk4FrR9WBDvd-qDe5KFarZe5iUTQ/edit?gid=0#gid=0