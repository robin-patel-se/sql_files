SELECT
	stmc.touch_affiliate_territory,
	stmc.touch_mkt_channel,
	DATE_TRUNC(WEEK, stba.touch_start_tstamp)::DATE                                                           AS week,
	ca.se_week,
	ca.se_year,
	CASE
		WHEN stba.stitched_identity_type LIKE 'se_user_id' THEN 'Member'
		ELSE 'Non-member'
	END                                                                                                       AS identity_type,
	a.attribution_model,
	SUM(CASE WHEN touch_duration_seconds = 0 THEN 1 ELSE 0 END)                                               AS zero_duration_sessions,
	COUNT(stba.touch_id)                                                                                      AS sessions,
	SUM(CASE WHEN is_engaged_session = 'TRUE' THEN 1 ELSE 0 END)                                              AS engaged_sessions,
	SUM(CASE WHEN is_engaged_session_events_of_interest = 'TRUE' THEN 1 ELSE 0 END)                           AS engaged_session_events_of_interest,
	SUM(num_web_page_views)                                                                                   AS web_page_views,
	SUM(num_app_screen_views)                                                                                 AS app_screen_views,
	SUM(num_page_views)                                                                                       AS page_views,
	SUM(num_spvs)                                                                                             AS spvs,
	SUM(num_searches)                                                                                         AS searches,
	SUM(num_bfvs)                                                                                             AS bfvs,
	SUM(num_pay_button_clicks)                                                                                AS pay_button_clicks,
	SUM(num_trxs)                                                                                             AS bookings,
	SUM(num_app_installs)                                                                                     AS app_installs,
	SUM(num_app_notification_events_in_app_opens)                                                             AS app_notification_events_in_app_opens,
	SUM(num_app_notification_events_in_app_clicks)                                                            AS in_app_clicks,
	COALESCE(SUM(b.margin), 0)                                                                                AS gross_margin_gross_of_toms_gbp_constant_currency,
	COALESCE(SUM(CASE
					 WHEN DATE_TRUNC(WEEK, sua.signup_tstamp)::DATE = DATE_TRUNC(WEEK, stba.touch_start_tstamp)::DATE
						 THEN b.margin
				 END),
			 0)                                                                                               AS w1_gross_margin_gross_of_toms_gbp_constant_currency,
	COUNT(DISTINCT stba.attributed_user_id)                                                                   AS wau,
	COUNT(DISTINCT CASE
					   WHEN DATE_TRUNC(WEEK, sua.signup_tstamp)::DATE = DATE_TRUNC(WEEK, stba.touch_start_tstamp)::DATE
						   THEN stba.attributed_user_id
				   END)                                                                                       AS w1_wau,
	COUNT(DISTINCT CASE
					   WHEN is_engaged_session = 'TRUE' THEN stba.attributed_user_id
				   END)                                                                                       AS engaged_wau,
	COUNT(DISTINCT CASE
					   WHEN DATE_TRUNC(WEEK, sua.signup_tstamp)::DATE =
							DATE_TRUNC(WEEK, stba.touch_start_tstamp)::DATE AND is_engaged_session = 'TRUE'
						   THEN stba.attributed_user_id
				   END)                                                                                       AS engaged_w1_wau,
	COUNT(DISTINCT CASE
					   WHEN is_engaged_session_events_of_interest = 'TRUE' THEN stba.attributed_user_id
				   END)                                                                                       AS engaged_of_interest_wau,
	COUNT(DISTINCT CASE
					   WHEN DATE_TRUNC(WEEK, sua.signup_tstamp)::DATE =
							DATE_TRUNC(WEEK, stba.touch_start_tstamp)::DATE AND
							is_engaged_session_events_of_interest = 'TRUE' THEN stba.attributed_user_id
				   END)                                                                                       AS w1_engaged_of_interest_wau,
	COUNT(DISTINCT CASE WHEN num_spvs >= 1 THEN stba.attributed_user_id END)                                  AS unique_spv_users,
	COUNT(DISTINCT CASE WHEN num_trxs >= 1 THEN stba.attributed_user_id END)                                  AS unique_bookers,
	COUNT(DISTINCT CASE
					   WHEN DATE_TRUNC(WEEK, sua.signup_tstamp)::DATE =
							DATE_TRUNC(WEEK, stba.touch_start_tstamp)::DATE AND num_trxs >= 1
						   THEN stba.attributed_user_id
				   END)                                                                                       AS w1_unique_bookers

FROM se.data_pii.scv_touch_basic_attributes stba
	LEFT JOIN  se.data.se_user_attributes sua ON sua.shiro_user_id::VARCHAR = stba.attributed_user_id
	-- RP: use join to session level territory (not via attribution) to allow correct filtering
	INNER JOIN se.data.scv_touch_marketing_channel tstmc ON stba.touch_id = tstmc.touch_id
	INNER JOIN se.data.scv_touch_attribution a ON a.touch_id = stba.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON a.attributed_touch_id = stmc.touch_id
	LEFT JOIN  sess_bookings b ON stba.touch_id = b.touch_id
	INNER JOIN se.data.se_calendar ca ON ca.date_value = stba.touch_start_tstamp::DATE
WHERE stba.touch_start_tstamp::DATE >= $from_date
  AND stba.touch_start_tstamp::DATE <= $to_date
  -- RP: adjusted
  AND tstmc.touch_affiliate_territory IN ('DE')
GROUP BY 1, 2, 3, 4, 5, 6, 7