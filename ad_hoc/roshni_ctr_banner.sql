WITH
	total_email_sends AS (
		-- aggregating sends up to a campaign level
		SELECT
			ces.campaign_id,
			COUNT(*) AS sends
		FROM se.data.crm_events_sends ces
		GROUP BY 1
	),
	model_clicks AS (
		-- modelling click data up to campaign level
		SELECT
			cec.campaign_id,
--             cjl.email_name,
			COUNT(cec.shiro_user_id)                                                         AS clicks,        -- all clicks (not just banner)
			COUNT(DISTINCT cec.shiro_user_id)                                                AS unique_clicks, -- all unique clicks (not just banner)
			SUM(IFF(cec.url LIKE 'https://www.secretescapes.de/eucitytrips/filter?%', 1, 0)) AS banner_clicks,
			COUNT(DISTINCT IFF(cec.url LIKE 'https://www.secretescapes.de/eucitytrips/filter?%', cec.shiro_user_id,
							   NULL))                                                        AS banner_unique_clicks
		FROM se.data.crm_events_clicks cec
--             INNER JOIN se.data.crm_jobs_list cjl ON cec.campaign_id = cjl.campaign_id
		WHERE
			-- filter to select a certain date the click occurred on
			event_date BETWEEN '2024-04-18' AND '2024-04-18' AND

			-- to add more campaigns simply place a comma at the end of the line
			-- and add a new campaign below it
			cec.campaign_id IN (
								'9527535',
								'9527543',
								'9401701',
								'7180487'
				)
		GROUP BY 1
	)
-- modelling campaign sends and click data
SELECT
	mc.campaign_id,
--     mc.email_name,
--     tes.sends,
	mc.clicks,
	mc.unique_clicks,
	mc.banner_clicks,
	mc.banner_unique_clicks,
--     mc.banner_clicks / tes.sends AS banner_ctr
FROM model_clicks mc
--     INNER JOIN total_email_sends tes ON mc.campaign_id = tes.campaign_id


SELECT *
FROM se.data.iterable_crm_reporting icr



WITH
	banner_metrics AS (
		SELECT
			SHA2(cec.message_id || cec.email_address) AS message_id_email_hash,

			SUM(IFF(SPLIT_PART(PARSE_URL(cec.url, 1)['path']::VARCHAR, '/', -1) IN ('filter', 'filtra'), 1,
					0))                               AS clicks_to_filter,
			SUM(IFF(SPLIT_PART(PARSE_URL(cec.url, 1)['path']::VARCHAR, '/', -1) IN ('search'), 1,
					0))                               AS clicks_to_search,
			SUM(IFF(SPLIT_PART(PARSE_URL(cec.url, 1)['path']::VARCHAR, '/', -1) IN ('competition'), 1,
					0))                               AS clicks_to_competition,
			SUM(IFF(SPLIT_PART(PARSE_URL(cec.url, 1)['path']::VARCHAR, '/', -1) IN
					('current-sales', 'aktuelle-angebote', 'offerte-in-corso', 'aanbiedingen', 'aktuella-kampanjer'), 1,
					0))                               AS clicks_to_homepage,
			SUM(IFF(SPLIT_PART(PARSE_URL(cec.url, 1)['path']::VARCHAR, '/', -1) IN
					('sale-hotel', 'sale-wrd', 'sale-ncwrd'), 1,
					0))                               AS clicks_to_spv
		FROM se.data_pii.crm_events_clicks cec
		GROUP BY 1
	)
SELECT
	icr.campaign_id,
	icr.campaign_name,
	SUM(icr.email_sends)     AS sends,
	SUM(icr.email_clicks)    AS clicks,
	SUM(bm.clicks_to_filter) AS filter_clicks,
	filter_clicks / sends AS filter_click_through_rate,
	SUM(bm.clicks_to_search) AS search_clicks,
	search_clicks / sends AS search_click_through_rate
FROM se.data.iterable_crm_reporting icr
	LEFT JOIN banner_metrics bm ON icr.message_id_email_hash = bm.message_id_email_hash
WHERE icr.campaign_id IN (
						  '9527535',
						  '9527543',
						  '9401701',
						  '7180487'
	)
  AND icr.send_start_date BETWEEN '2024-03-26' AND '2024-04-26'
GROUP BY 1, 2
;


SELECT
	PARSE_URL(cec.url, 1)['path'],
	COUNT(*)
FROM se.data.crm_events_clicks cec
GROUP BY 1
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	SPLIT_PART(PARSE_URL(cec.url, 1)['path']::VARCHAR, '/', -1) AS click_url_end_of_path,
	COUNT(*)
FROM se.data.crm_events_clicks cec
WHERE cec.event_tstamp >= CURRENT_DATE - 15
GROUP BY 1
ORDER BY 2 DESC