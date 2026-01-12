SELECT
	sp.event_tstamp::DATE                   AS event_date,
	sp.device_platform,
	COUNT(DISTINCT
		  IFF(sp.page_urlpath IN ('/contact',
								  '/kontakt-und-impressum'),
			  tc.attributed_user_id,
			  NULL
		  ))                                AS unique_contact_page_user_visits,
	COUNT(DISTINCT (tc.attributed_user_id)) AS unique_total_user_visits,
	COUNT(IFF(sp.page_urlpath IN ('/contact',
								  '/kontakt-und-impressum'),
			  tc.attributed_user_id,
			  NULL
		  ))                                AS all_contact_page_user_visits,
	COUNT(tc.attributed_user_id)            AS all_total_user_visits,
	COUNT(DISTINCT IFF(sp.page_urlpath IN ('/faq',
										   '/faq',
										   '/faq',
										   '/faq'),
					   tc.attributed_user_id,
					   NULL
				   ))                       AS unique_contact_page_user_visits_faq,
	COUNT(IFF(sp.page_urlpath IN ('/faq',
								  '/faq',
								  '/faq',
								  '/faq'),
			  tc.attributed_user_id,
			  NULL
		  ))                                AS all_contact_page_user_visits_faq
FROM se.data_pii.scv_event_stream sp
	INNER JOIN se.data_pii.scv_session_events_link tc ON tc.event_hash = sp.event_hash
	AND tc.event_tstamp::DATE >= CURRENT_DATE - 30
WHERE sp.event_tstamp::DATE >= CURRENT_DATE - 30
GROUP BY 1, 2
;

