WITH
	search_high_level_sessions AS (
		SELECT
			stba.touch_start_tstamp::date AS event_date,
			stba.touch_hostname_territory AS territory,
			stba.touch_experience         AS device,
			stmc.channel_category         AS channel,
			COUNT(stba.touch_id)          AS sessions
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
		WHERE event_date >= CURRENT_DATE - 30
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_bookings AS (
		SELECT
			stba.touch_start_tstamp::date AS event_date,
			stba.touch_hostname_territory AS territory,
			stba.touch_experience         AS device,
			stmc.channel_category         AS channel,
			COUNT(stt.booking_id)         AS bookings,
			COUNT(DISTINCT stt.touch_id)  AS booking_sessions
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stba.touch_id = stt.touch_id
		WHERE event_date >= CURRENT_DATE - 30
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_spvs AS (
		SELECT
			stba.touch_start_tstamp::date AS event_date,
			stba.touch_hostname_territory AS territory,
			stba.touch_experience         AS device,
			stmc.channel_category         AS channel,
			COUNT(spvs.event_hash)        AS spvs,
			COUNT(DISTINCT spvs.touch_id) AS spv_sessions
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_spvs spvs
					   ON stba.touch_id = spvs.touch_id
		WHERE event_date >= CURRENT_DATE - 30
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_searches AS (
		SELECT
			stba.touch_start_tstamp::date                                        AS event_date,
			stba.touch_hostname_territory                                        AS territory,
			stba.touch_experience                                                AS device,
			stmc.channel_category                                                AS channel,
			COUNT(DISTINCT stse.touch_id)                                        AS search_sessions,
			COUNT(stse.event_hash)                                               AS searches,
			COUNT(DISTINCT IFF(stse.triggered_by = 'user', stse.touch_id, NULL)) AS user_search_sessions,
			COUNT(IFF(stse.triggered_by = 'user', stse.event_hash, NULL))        AS user_searches
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_searches stse
					   ON stba.touch_id = stse.touch_id
		WHERE event_date >= CURRENT_DATE - 30
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		  AND stse.page_url LIKE '%search%'
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_searches_and_booking AS (
		SELECT
			stba.touch_start_tstamp::date                                        AS event_date,
			stba.touch_hostname_territory                                        AS territory,
			stba.touch_experience                                                AS device,
			stmc.channel_category                                                AS channel,
			COUNT(DISTINCT stse.touch_id)                                        AS search_sessions,
			COUNT(stse.event_hash)                                               AS searches,
			COUNT(DISTINCT IFF(stse.triggered_by = 'user', stse.touch_id, NULL)) AS user_search_sessions,
			COUNT(IFF(stse.triggered_by = 'user', stse.event_hash, NULL))        AS user_searches
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_searches stse
					   ON stba.touch_id = stse.touch_id
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stse.touch_id = stt.touch_id
		WHERE event_date >= CURRENT_DATE - 30
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		  AND stse.page_url LIKE '%search%'
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_output AS (
		SELECT
			ses.event_date,
			ses.territory,
			ses.device,
			ses.channel,
			COALESCE(ses.sessions, 0)                AS sessions,
			COALESCE(spv.spv_sessions, 0)            AS spv_sessions,
			COALESCE(spv.spvs, 0)                    AS spvs,
			COALESCE(search.search_sessions, 0)      AS search_sessions,
			COALESCE(search.searches, 0)             AS searches,
			COALESCE(search.user_search_sessions, 0) AS user_search_sessions,
			COALESCE(search.user_searches, 0)        AS user_searches,
			COALESCE(bk.booking_sessions, 0)         AS booking_sessions,
			COALESCE(bk.bookings, 0)                 AS bookings,
			COALESCE(sbk.search_sessions, 0)         AS book_search_sessions,
			COALESCE(sbk.user_search_sessions, 0)    AS book_user_search_sessions
		FROM search_high_level_sessions ses
			LEFT JOIN search_high_level_spvs spv
					  ON spv.event_date = ses.event_date
						  AND spv.territory = ses.territory
						  AND spv.device = ses.device
						  AND spv.channel = ses.channel
			LEFT JOIN search_high_level_searches search
					  ON search.event_date = ses.event_date
						  AND search.territory = ses.territory
						  AND search.device = ses.device
						  AND search.channel = ses.channel
			LEFT JOIN search_high_level_bookings bk
					  ON bk.event_date = ses.event_date
						  AND bk.territory = ses.territory
						  AND bk.device = ses.device
						  AND bk.channel = ses.channel
			LEFT JOIN search_high_level_searches_and_booking sbk
					  ON sbk.event_date = ses.event_date
						  AND sbk.territory = ses.territory
						  AND sbk.device = ses.device
						  AND sbk.channel = ses.channel
	)
SELECT
	event_date,
	territory,
	device,
	channel,
	SUM(sessions)                  AS sessions,
	SUM(spv_sessions)              AS spv_sessions,
	SUM(spvs)                      AS spvs,
	SUM(searches)                  AS searches,
	SUM(user_searches)             AS user_searches,
	SUM(search_sessions)           AS search_sessions,
	SUM(user_search_sessions)      AS user_search_sessions,
	SUM(booking_sessions)          AS booking_sessions,
	SUM(book_search_sessions)      AS book_search_sessions,
	SUM(book_user_search_sessions) AS book_user_search_sessions,
	SUM(bookings)                  AS bookings
FROM search_high_level_output
GROUP BY 1, 2, 3, 4
;

------------------------------------------------------------------------------------------------------------------------

-- What I really need on there is click through rate from search results page which will give a better idea of how the
-- search feature is doing in moving people to the next step of the funnel. Booking conversion happens so far away from
-- a search that its not that helpful

USE WAREHOUSE pipe_xlarge
;
-- search page results
SELECT
	ses.event_tstamp::DATE         AS event_date,
	stba.touch_experience          AS platform,
	stmc.touch_affiliate_territory AS territory,
	stmc.channel_category          AS channel,
	COUNT(1)                       AS search_pages
FROM se.data_pii.scv_event_stream ses
	INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
	INNER JOIN se.data.scv_touch_basic_attributes stba ON ssel.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON ssel.touch_id = stmc.touch_id
WHERE ses.event_tstamp::DATE BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
  AND ses.event_name = 'page_view'
  AND stba.stitched_identity_type = 'se_user_id'
  AND ses.page_urlpath LIKE ANY ('%search/search%', '%mbSearch/mbSearch')
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC
;

-- search click through
SELECT
	ses.event_tstamp::DATE         AS event_date,
	stba.touch_experience          AS platform,
	stmc.touch_affiliate_territory AS territory,
	stmc.channel_category          AS channel,
	COUNT(1)                       AS search_click_through_pages
FROM se.data_pii.scv_event_stream ses
	INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
	INNER JOIN se.data.scv_touch_basic_attributes stba ON ssel.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON ssel.touch_id = stmc.touch_id
WHERE ses.event_tstamp::DATE BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
  AND ses.event_name = 'page_view'
  AND stba.stitched_identity_type = 'se_user_id'
  AND ses.refr_urlpath LIKE ANY ('%search/search%', '%mbSearch/mbSearch')
  AND ses.is_server_side_event -- remove duplication of server side and client side spvs
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC
;



SELECT
	fb.booking_completed_date                                             AS event_date,
	fb.se_sale_id,

	COUNT(fb.booking_id)                                                  AS bookings_total,
	COUNT(IFF(fb.booking_status_type = 'live', fb.booking_id, NULL))      AS bookings_live,
	COUNT(IFF(fb.booking_status_type = 'cancelled', fb.booking_id, NULL)) AS bookings_cancelled,

	COUNT(IFF(fb.booking_status_type = 'live' AND fb.booking_includes_flight = TRUE, fb.booking_id,
			  NULL))                                                      AS live_bookings_with_flights,
	COUNT(IFF(fb.booking_status_type = 'live' AND fb.booking_includes_flight = FALSE, fb.booking_id,
			  NULL))                                                      AS live_bookings_without_flights,
	COUNT(IFF(fb.booking_status_type = 'cancelled' AND fb.booking_includes_flight = TRUE, fb.booking_id,
			  NULL))                                                      AS cancelled_bookings_with_flights,
	COUNT(IFF(fb.booking_status_type = 'cancelled' AND fb.booking_includes_flight = FALSE, fb.booking_id,
			  NULL))                                                      AS cancelled_bookings_without_flights,

	SUM(CASE
			WHEN fb.booking_status_type = 'live'
				THEN fb.margin_gross_of_toms_gbp_constant_currency
		END)                                                              AS live_margin,
	SUM(CASE
			WHEN fb.booking_status_type = 'cancelled'
				THEN fb.margin_gross_of_toms_gbp_constant_currency
		END)                                                              AS cancelled_margin,
	live_margin + cancelled_margin                                        AS total_margin,

	SUM(CASE
			WHEN fb.booking_status_type = 'live' AND fb.booking_includes_flight = TRUE
				THEN fb.margin_gross_of_toms_gbp_constant_currency
		END)                                                              AS
																			 live_margin_with_flights,
	SUM(CASE
			WHEN fb.booking_status_type = 'live' AND fb.booking_includes_flight = FALSE
				THEN fb.margin_gross_of_toms_gbp_constant_currency
		END)                                                              AS
																			 live_margin_without_flights,
	SUM(CASE
			WHEN fb.booking_status_type = 'cancelled' AND fb.booking_includes_flight = TRUE
				THEN fb.margin_gross_of_toms_gbp_constant_currency
		END)                                                              AS
																			 cancelled_margin_with_flights,
	SUM(CASE
			WHEN fb.booking_status_type = 'cancelled' AND fb.booking_includes_flight = FALSE
				THEN fb.margin_gross_of_toms_gbp_constant_currency
		END)                                                              AS
																			 cancelled_margin_without_flights,

	SUM(CASE
			WHEN fb.booking_status_type = 'live'
				THEN fb.gross_revenue_gbp_constant_currency
		END)                                                              AS live_gross_margin,
	SUM(CASE
			WHEN fb.booking_status_type = 'cancelled'
				THEN fb.gross_revenue_gbp_constant_currency
		END)                                                              AS cancelled_gross_margin,
	live_gross_margin + cancelled_gross_margin                            AS total_gross_margin,
	SUM(CASE
			WHEN fb.booking_status_type = 'live'
				THEN fb.commission_ex_vat_gbp
		END)                                                              AS live_commission_ex_vat_gbp,
	live_commission_ex_vat_gbp / NULLIF(live_gross_margin, 0)             AS take_rate


FROM dbt_dev.dbt_robinpatel_staging.base_dwh__fact_booking fb
	INNER JOIN dbt.bi_commercial_insights_planning__intermediate.cip_hotel_plus_01_sale_list sf
			   ON sf.se_sale_id = fb.se_sale_id
WHERE fb.booking_status_type IN ('live', 'cancelled')
GROUP BY 1, 2


------------------------------------------------------------------------------------------------------------------------


WITH
	search_high_level_sessions AS (
		SELECT
			stba.touch_start_tstamp::date AS event_date,
			stba.touch_hostname_territory AS territory,
			stba.touch_experience         AS device,
			stmc.channel_category         AS channel,
			COUNT(stba.touch_id)          AS sessions
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
		WHERE event_date >= '2023-01-01'
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_bookings AS (
		SELECT
			stba.touch_start_tstamp::date AS event_date,
			stba.touch_hostname_territory AS territory,
			stba.touch_experience         AS device,
			stmc.channel_category         AS channel,
			COUNT(stt.booking_id)         AS bookings,
			COUNT(DISTINCT stt.touch_id)  AS booking_sessions
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stba.touch_id = stt.touch_id
		WHERE event_date >= '2023-01-01'
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_spvs AS (
		SELECT
			stba.touch_start_tstamp::date AS event_date,
			stba.touch_hostname_territory AS territory,
			stba.touch_experience         AS device,
			stmc.channel_category         AS channel,
			COUNT(spvs.event_hash)        AS spvs,
			COUNT(DISTINCT spvs.touch_id) AS spv_sessions
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_spvs spvs
					   ON stba.touch_id = spvs.touch_id
		WHERE event_date >= '2023-01-01'
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_searches AS (
		SELECT
			stba.touch_start_tstamp::date                                        AS event_date,
			stba.touch_hostname_territory                                        AS territory,
			stba.touch_experience                                                AS device,
			stmc.channel_category                                                AS channel,
			COUNT(DISTINCT stse.touch_id)                                        AS search_sessions,
			COUNT(stse.event_hash)                                               AS searches,
			COUNT(DISTINCT IFF(stse.triggered_by = 'user', stse.touch_id, NULL)) AS user_search_sessions,
			COUNT(IFF(stse.triggered_by = 'user', stse.event_hash, NULL))        AS user_searches
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_searches stse
					   ON stba.touch_id = stse.touch_id
		WHERE event_date >= '2023-01-01'
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		  AND stse.page_url LIKE '%search%'
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_searches_and_booking AS (
		SELECT
			stba.touch_start_tstamp::date                                        AS event_date,
			stba.touch_hostname_territory                                        AS territory,
			stba.touch_experience                                                AS device,
			stmc.channel_category                                                AS channel,
			COUNT(DISTINCT stse.touch_id)                                        AS search_sessions,
			COUNT(stse.event_hash)                                               AS searches,
			COUNT(DISTINCT IFF(stse.triggered_by = 'user', stse.touch_id, NULL)) AS user_search_sessions,
			COUNT(IFF(stse.triggered_by = 'user', stse.event_hash, NULL))        AS user_searches
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.scv_touched_searches stse
					   ON stba.touch_id = stse.touch_id
			INNER JOIN se.data.scv_touched_transactions stt
					   ON stse.touch_id = stt.touch_id
		WHERE event_date >= '2023-01-01'
		  AND stba.touch_experience IN ('web', 'mobile web', 'tablet web')
		  AND stse.page_url LIKE '%search%'
		GROUP BY 1, 2, 3, 4
	),
	search_high_level_output AS (
		SELECT
			ses.event_date,
			ses.territory,
			ses.device,
			ses.channel,
			COALESCE(ses.sessions, 0)                AS sessions,
			COALESCE(spv.spv_sessions, 0)            AS spv_sessions,
			COALESCE(spv.spvs, 0)                    AS spvs,
			COALESCE(search.search_sessions, 0)      AS search_sessions,
			COALESCE(search.searches, 0)             AS searches,
			COALESCE(search.user_search_sessions, 0) AS user_search_sessions,
			COALESCE(search.user_searches, 0)        AS user_searches,
			COALESCE(bk.booking_sessions, 0)         AS booking_sessions,
			COALESCE(bk.bookings, 0)                 AS bookings,
			COALESCE(sbk.search_sessions, 0)         AS book_search_sessions,
			COALESCE(sbk.user_search_sessions, 0)    AS book_user_search_sessions
		FROM search_high_level_sessions ses
			LEFT JOIN search_high_level_spvs spv
					  ON spv.event_date = ses.event_date
						  AND spv.territory = ses.territory
						  AND spv.device = ses.device
						  AND spv.channel = ses.channel
			LEFT JOIN search_high_level_searches search
					  ON search.event_date = ses.event_date
						  AND search.territory = ses.territory
						  AND search.device = ses.device
						  AND search.channel = ses.channel
			LEFT JOIN search_high_level_bookings bk
					  ON bk.event_date = ses.event_date
						  AND bk.territory = ses.territory
						  AND bk.device = ses.device
						  AND bk.channel = ses.channel
			LEFT JOIN search_high_level_searches_and_booking sbk
					  ON sbk.event_date = ses.event_date
						  AND sbk.territory = ses.territory
						  AND sbk.device = ses.device
						  AND sbk.channel = ses.channel
	)
SELECT
	event_date,
	territory,
	device,
	channel,
	SUM(sessions)                  AS sessions,
	SUM(spv_sessions)              AS spv_sessions,
	SUM(spvs)                      AS spvs,
	SUM(searches)                  AS searches,
	SUM(user_searches)             AS user_searches,
	SUM(search_sessions)           AS search_sessions,
	SUM(user_search_sessions)      AS user_search_sessions,
	SUM(booking_sessions)          AS booking_sessions,
	SUM(book_search_sessions)      AS book_search_sessions,
	SUM(book_user_search_sessions) AS book_user_search_sessions,
	SUM(bookings)                  AS bookings
FROM search_high_level_output
GROUP BY 1, 2, 3, 4