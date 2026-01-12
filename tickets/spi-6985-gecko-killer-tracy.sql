-- spvs

SELECT
	se.data.posa_category_from_territory(COALESCE(
			se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
			REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
			REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
										 )) AS territory,
	DATE_TRUNC('hour', event_tstamp)        AS hour,
	event_tstamp::DATE = CURRENT_DATE       AS today,
	ds.posu_cluster,
	ds.posu_cluster_sub_region,
	ds.posu_cluster_region,
	e.event_tstamp_yesterday,
	e.event_tstamp_today_last_week,
	e.event_tstamp_today_ly,
	e.event_tstamp_today_lly,
	e.event_tstamp_today_2019,
	COUNT(*)                                AS spvs
FROM data_vault_mvp.dwh.trimmed_event_stream e
	LEFT JOIN se.data.dim_sale ds ON ds.se_sale_id = e.se_sale_id
WHERE DATE_TRUNC('hour', event_tstamp) < DATE_TRUNC('hour', CURRENT_TIMESTAMP) AND
	  (
		  (--app spvs
			  ( -- old world native app event data
				  e.collector_tstamp < '2020-02-28 00:00:00'
					  AND
				  e.contexts_com_secretescapes_sale_page_context_1 IS NOT NULL
				  )
				  OR
			  ( -- new world native app event data
				  e.collector_tstamp >= '2020-02-28 00:00:00'
					  AND
				  (
					  e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR =
					  'sale'
						  OR
					  e.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR =
					  'sale page'
					  )
				  )
			  )
			  OR (--web spvs
			  (--client side tracking, prior implementation/validation
				  e.collector_tstamp < '2020-02-28 00:00:00'
					  AND (
					  e.page_urlpath LIKE '%/sale'
						  OR
					  e.page_urlpath LIKE
					  '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
					  )
					  AND e.is_server_side_event = FALSE -- exclude non validated ss events
				  )
				  OR
			  (--server side tracking, post implementation/validation
				  e.collector_tstamp >= '2020-02-28 00:00:00'
					  AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
					  AND PARSE_URL(e.page_url, 1)['path']::VARCHAR NOT LIKE
						  '%/sale-offers' -- remove issue where spv events were firing on offer pages
					  AND e.is_server_side_event = TRUE
				  )
			  )
			  OR --wrd spvs
		  e.se_category = 'web redirect click'
		  )

GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------
-- bookings and margin

WITH
	model_hours AS (
		SELECT
			DATEADD('hour', h.hour, sc.date_value) AS hour,
			sc.date_value                          AS date,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019
		FROM se.data.se_calendar sc
			LEFT JOIN data_vault_mvp.dwh.hour h
		WHERE (
				  sc.today
					  OR sc.yesterday
					  OR sc.today_last_week
					  OR sc.today_ly
					  OR sc.today_lly
					  OR sc.today_2019
				  )
	),
	daily_target AS (
		SELECT
			CASE gt.dimension_3
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                  AS territory,
			gt.dimension_6       AS posu_cluster_sub_region,
			gt.dimension_1       AS posu_cluster,
			SUM(gt.target_value) AS target
		FROM se.data.generic_targets gt
		WHERE gt.target_date = CURRENT_DATE
		  AND gt.target_name = 'cluster_sub_region_target'
		  AND gt.dimension_2 IS DISTINCT FROM 'Catalogue'
		GROUP BY 1, 2, 3
	),

	model_bookings AS (
		SELECT
			CASE se.data.posa_category_from_territory(bs.territory)
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END
																		AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019,
			ROUND(SUM(IFF(bs.currency = 'GBP',
						  bs.margin_gross_of_toms_cc,
						  margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
			ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
			COUNT(DISTINCT bs.booking_id)                               AS bookings
		FROM latest_vault.cms_mongodb.booking_summary bs
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
			INNER JOIN data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
			INNER JOIN se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			bs.currency = cc.base_currency
		WHERE (
				  sc.today
					  OR sc.yesterday
					  OR sc.today_last_week
					  OR sc.today_ly
					  OR sc.today_lly
					  OR sc.today_2019
				  )
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	),
	distinct_territories AS (
		SELECT DISTINCT
			mb.territory,
			mb.posu_cluster,
			mb.posu_cluster_sub_region
		FROM daily_target mb
	),
	target_grain AS (
		SELECT
			dt.territory,
			dt.posu_cluster,
			dt.posu_cluster_sub_region,
			dt.target,
			mh.hour
		FROM daily_target dt
			LEFT JOIN model_hours mh
		WHERE mh.today
	),
	model_target AS (
		-- model target phased on today LW run rate
		SELECT
			tg.territory,
			tg.posu_cluster,
			tg.posu_cluster_sub_region,
			tg.hour,
			tg.target,
			mb.margin_gbp_constant_currency,
			SUM(COALESCE(mb.margin_gbp_constant_currency, 0))
				OVER (PARTITION BY tg.territory, tg.posu_cluster, tg.posu_cluster_sub_region) AS total_margin_gbp_constant_currency,
			IFF(total_margin_gbp_constant_currency = 0,
				tg.target / 24,
				(COALESCE(mb.margin_gbp_constant_currency, 0) / total_margin_gbp_constant_currency) * tg.target
			)                                                                                 AS hourly_target_gbp_constant_currency
		FROM target_grain tg
			LEFT JOIN model_bookings mb ON tg.hour = DATEADD('day', 7, mb.hour)
			AND tg.territory = mb.territory
			AND tg.posu_cluster = mb.posu_cluster
			AND tg.posu_cluster_sub_region = mb.posu_cluster_sub_region
			AND mb.today_last_week
	),
	grain AS (
		SELECT
			mh.hour,
			mh.date,
			dt.territory,
			dt.posu_cluster,
			dt.posu_cluster_sub_region,
			mh.today,
			mh.yesterday,
			mh.today_last_week,
			mh.today_ly,
			mh.today_lly,
			mh.today_2019
		FROM model_hours mh
			LEFT JOIN distinct_territories dt
	)
SELECT
	g.hour,
	g.date,
	g.territory,
	g.posu_cluster,
	g.posu_cluster_sub_region,
	g.today,
	g.yesterday,
	g.today_last_week,
	g.today_ly,
	g.today_lly,
	g.today_2019,
--        mt.target,
--        mt.total_margin_gbp_constant_currency,
	mt.hourly_target_gbp_constant_currency,
	mb.margin_gbp_constant_currency,
	mb.margin_gbp,
	mb.bookings
FROM grain g
	LEFT JOIN model_target mt ON g.hour = mt.hour
	AND g.territory = mt.territory
	AND g.posu_cluster = mt.posu_cluster
	AND g.posu_cluster_sub_region = mt.posu_cluster_sub_region
	LEFT JOIN model_bookings mb ON g.hour = mb.hour
	AND g.territory = mb.territory
	AND g.posu_cluster = mb.posu_cluster
	AND g.posu_cluster_sub_region = mb.posu_cluster_sub_region


------------------------------------------------------------------------------------------------------------------------
SELECT
	CASE se.data.posa_category_from_territory(ds.posa_territory)
		WHEN 'UK' THEN 'UK'
		WHEN 'DACH' THEN 'DACH'
		ELSE 'Other'
	END                                   AS territory,
	ds.posu_cluster,
	ds.posu_cluster_sub_region,
	DATE_TRUNC('hour', oo.created_at_dts) AS hour,
	sc.today,
	sc.yesterday,
	sc.today_last_week,
	sc.today_ly,
	sc.today_lly,
	sc.today_2019,
	ROUND(SUM(oo.margin * r.fx_rate))     AS margin_gbp,
	ROUND(SUM(oo.margin * cc.multiplier)) AS margin_gbp_constant_currency,
	COUNT(DISTINCT oo.id)                 AS bookings
FROM latest_vault.travelbird_mysql.orders_order oo
	INNER JOIN data_vault_mvp.dwh.se_calendar sc ON oo.created_at_dts::DATE = sc.date_value
	INNER JOIN data_vault_mvp.dwh.tb_offer tbo ON oo.offer_id = tbo.id
	INNER JOIN data_vault_mvp.dwh.dim_sale ds ON tbo.se_sale_id = ds.se_sale_id
-- orders_order.site_id > common_sitesettings.site_id > common_sitesettings.site_currency_id > currency_currency.id > currency_currency.code
	INNER JOIN latest_vault.travelbird_mysql.common_sitesettings cs ON oo.site_id = cs.site_id
	INNER JOIN latest_vault.travelbird_mysql.currency_currency cur ON cs.site_currency_id = cur.id
	INNER JOIN data_vault_mvp.fx.rates r
			   ON oo.created_at_dts::DATE = r.fx_date
				   AND r.source_currency = cur.code
				   AND r.target_currency = 'GBP'
	INNER JOIN se.data.constant_currency cc ON
	(CURRENT_DATE) >= cc.start_date AND
	(CURRENT_DATE) <= cc.end_date AND
	cc.currency = 'GBP' AND
	cc.category = 'Primary' AND
	cur.code = cc.base_currency
WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
  AND ds.se_brand = 'SE Brand'
  AND (
	sc.today
		OR sc.yesterday
		OR sc.today_last_week
		OR sc.today_ly
		OR sc.today_lly
		OR sc.today_2019
	)
GROUP BY ALL
;

SELECT
FROM data_vault_mvp.fx.rates


WITH
	camilla AS (

		SELECT
			CASE se.data.posa_category_from_territory(bs.territory)
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                                                         AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019,
			ROUND(SUM(IFF(bs.currency = 'GBP',
						  bs.margin_gross_of_toms_cc,
						  margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
			ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
			COUNT(DISTINCT bs.booking_id)                               AS bookings
		FROM latest_vault.cms_mongodb.booking_summary bs
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
			LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
			LEFT JOIN  se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			bs.currency = cc.base_currency
		WHERE (
				  sc.today
					  OR sc.yesterday
					  OR sc.today_last_week
					  OR sc.today_ly
					  OR sc.today_lly
					  OR sc.today_2019
				  )
		GROUP BY ALL
	)
		,

	tracy AS (
		SELECT
			CASE se.data.posa_category_from_territory(ds.posa_territory)
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                                   AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', oo.created_at_dts) AS hour,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019,
			ROUND(SUM(oo.margin * cc.multiplier)) AS margin_gbp_constant_currency,
			ROUND(SUM(oo.margin * r.fx_rate))     AS margin_gbp,
			COUNT(DISTINCT oo.id)                 AS bookings
		FROM latest_vault.travelbird_mysql.orders_order oo
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON oo.created_at_dts::DATE = sc.date_value
			INNER JOIN data_vault_mvp.dwh.tb_offer tbo ON oo.offer_id = tbo.id
			INNER JOIN data_vault_mvp.dwh.dim_sale ds ON tbo.se_sale_id = ds.se_sale_id
-- orders_order.site_id > common_sitesettings.site_id > common_sitesettings.site_currency_id > currency_currency.id > currency_currency.code
			INNER JOIN latest_vault.travelbird_mysql.common_sitesettings cs ON oo.site_id = cs.site_id
			INNER JOIN latest_vault.travelbird_mysql.currency_currency cur ON cs.site_currency_id = cur.id
			INNER JOIN data_vault_mvp.fx.rates r
					   ON oo.created_at_dts::DATE = r.fx_date
						   AND r.source_currency = cur.code
						   AND r.target_currency = 'GBP'
			INNER JOIN se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			cur.code = cc.base_currency
		WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
		  AND ds.se_brand = 'SE Brand'
		  AND (
			sc.today
				OR sc.yesterday
				OR sc.today_last_week
				OR sc.today_ly
				OR sc.today_lly
				OR sc.today_2019
			)
		GROUP BY ALL
	),
	stack AS (
		SELECT *
		FROM camilla c
		UNION ALL
		SELECT *
		FROM tracy
	)
SELECT
	territory,
	posu_cluster,
	posu_cluster_sub_region,
	hour,
	today,
	yesterday,
	today_last_week,
	today_ly,
	today_lly,
	today_2019,
	SUM(margin_gbp_constant_currency) AS margin_gbp_constant_currency,
	SUM(margin_gbp)                   AS margin_gbp,
	SUM(bookings)                     AS bookings
FROM stack s
GROUP BY ALL
;

------------------------------------------------------------------------------------------------------------------------

-- bookings new version

WITH
	model_hours AS (
		SELECT
			DATEADD('hour', h.hour, sc.date_value) AS hour,
			sc.date_value                          AS date,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019
		FROM se.data.se_calendar sc
			LEFT JOIN data_vault_mvp.dwh.hour h
		WHERE (
				  sc.today
					  OR sc.yesterday
					  OR sc.today_last_week
					  OR sc.today_ly
					  OR sc.today_lly
					  OR sc.today_2019
				  )
	),
	daily_target AS (
		SELECT
			CASE gt.dimension_3
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                  AS territory,
			gt.dimension_6       AS posu_cluster_sub_region,
			gt.dimension_1       AS posu_cluster,
			SUM(gt.target_value) AS target
		FROM se.data.generic_targets gt
		WHERE gt.target_date = CURRENT_DATE
		  AND gt.target_name = 'cluster_sub_region_target'
		GROUP BY 1, 2, 3
	),
	camilla AS (
-- model booking information from camilla
		SELECT
			CASE se.data.posa_category_from_territory(bs.territory)
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                                                         AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019,
			ROUND(SUM(IFF(bs.currency = 'GBP',
						  bs.margin_gross_of_toms_cc,
						  margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
			ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
			COUNT(DISTINCT bs.booking_id)                               AS bookings
		FROM latest_vault.cms_mongodb.booking_summary bs
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
			LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
			LEFT JOIN  se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			bs.currency = cc.base_currency
		WHERE (
				  sc.today
					  OR sc.yesterday
					  OR sc.today_last_week
					  OR sc.today_ly
					  OR sc.today_lly
					  OR sc.today_2019
				  )
		GROUP BY ALL
	),
	tracy AS (
		-- model booking information from tracy
		SELECT
			CASE se.data.posa_category_from_territory(ds.posa_territory)
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                                              AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', oo.created_at_dts)            AS hour,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019,
			ROUND(SUM(oo.margin * IFNULL(cc.multiplier, 1))) AS margin_gbp_constant_currency,
			ROUND(SUM(oo.margin * IFNULL(r.fx_rate, 1)))     AS margin_gbp,
			COUNT(DISTINCT oo.id)                            AS bookings
		FROM latest_vault.travelbird_mysql.orders_order oo
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON oo.created_at_dts::DATE = sc.date_value
			INNER JOIN data_vault_mvp.dwh.tb_offer tbo ON oo.offer_id = tbo.id
			INNER JOIN data_vault_mvp.dwh.dim_sale ds ON tbo.se_sale_id = ds.se_sale_id
-- orders_order.site_id > common_sitesettings.site_id > common_sitesettings.site_currency_id > currency_currency.id > currency_currency.code
			INNER JOIN latest_vault.travelbird_mysql.common_sitesettings cs ON oo.site_id = cs.site_id
			INNER JOIN latest_vault.travelbird_mysql.currency_currency cur ON cs.site_currency_id = cur.id
			INNER JOIN data_vault_mvp.fx.rates r
					   ON oo.created_at_dts::DATE = r.fx_date
						   AND r.source_currency = cur.code
						   AND r.target_currency = 'GBP'
			INNER JOIN se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			cur.code = cc.base_currency
		WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
		  AND ds.se_brand = 'SE Brand'
		  AND (
			sc.today
				OR sc.yesterday
				OR sc.today_last_week
				OR sc.today_ly
				OR sc.today_lly
				OR sc.today_2019
			)
		GROUP BY ALL
	),
	stack AS (
		SELECT *
		FROM camilla c
		UNION ALL
		SELECT *
		FROM tracy
	),
	model_bookings AS (
		SELECT
			territory,
			posu_cluster,
			posu_cluster_sub_region,
			hour,
			today,
			yesterday,
			today_last_week,
			today_ly,
			today_lly,
			today_2019,
			SUM(margin_gbp_constant_currency) AS margin_gbp_constant_currency,
			SUM(margin_gbp)                   AS margin_gbp,
			SUM(bookings)                     AS bookings
		FROM stack s
		GROUP BY ALL
	),
	distinct_territories AS (
		SELECT DISTINCT
			mb.territory,
			mb.posu_cluster,
			mb.posu_cluster_sub_region
		FROM daily_target mb
	),
	target_grain AS (
		SELECT
			dt.territory,
			dt.posu_cluster,
			dt.posu_cluster_sub_region,
			dt.target,
			mh.hour
		FROM daily_target dt
			LEFT JOIN model_hours mh
		WHERE mh.today
	),
	model_target AS (
		-- model target phased on today LW run rate
		SELECT
			tg.territory,
			tg.posu_cluster,
			tg.posu_cluster_sub_region,
			tg.hour,
			tg.target,
			mb.margin_gbp_constant_currency,
			SUM(COALESCE(mb.margin_gbp_constant_currency, 0))
				OVER (PARTITION BY tg.territory, tg.posu_cluster, tg.posu_cluster_sub_region) AS total_margin_gbp_constant_currency,
			IFF(total_margin_gbp_constant_currency = 0,
				tg.target / 24,
				(COALESCE(mb.margin_gbp_constant_currency, 0) / total_margin_gbp_constant_currency) * tg.target
			)                                                                                 AS hourly_target_gbp_constant_currency
		FROM target_grain tg
			LEFT JOIN model_bookings mb ON tg.hour = DATEADD('day', 7, mb.hour)
			AND tg.territory = mb.territory
			AND tg.posu_cluster = mb.posu_cluster
			AND tg.posu_cluster_sub_region = mb.posu_cluster_sub_region
			AND mb.today_last_week
	),
	grain AS (
		SELECT
			mh.hour,
			mh.date,
			dt.territory,
			dt.posu_cluster,
			dt.posu_cluster_sub_region,
			mh.today,
			mh.yesterday,
			mh.today_last_week,
			mh.today_ly,
			mh.today_lly,
			mh.today_2019
		FROM model_hours mh
			LEFT JOIN distinct_territories dt
	)
SELECT
	g.hour,
	g.date,
	g.territory,
	g.posu_cluster,
	g.posu_cluster_sub_region,
	g.today,
	g.yesterday,
	g.today_last_week,
	g.today_ly,
	g.today_lly,
	g.today_2019,
--        mt.target,
--        mt.total_margin_gbp_constant_currency,
	mt.hourly_target_gbp_constant_currency,
	mb.margin_gbp_constant_currency,
	mb.margin_gbp,
	mb.bookings
FROM grain g
	LEFT JOIN model_target mt ON g.hour = mt.hour
	AND g.territory = mt.territory
	AND g.posu_cluster = mt.posu_cluster
	AND g.posu_cluster_sub_region = mt.posu_cluster_sub_region
	LEFT JOIN model_bookings mb ON g.hour = mb.hour
	AND g.territory = mb.territory
	AND g.posu_cluster = mb.posu_cluster
	AND g.posu_cluster_sub_region = mb.posu_cluster_sub_region
;

------------------------------------------------------------------------------------------------------------------------
-- sense checking

WITH
	camilla AS (
-- model booking information from camilla
		SELECT
			CASE se.data.posa_category_from_territory(bs.territory)
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                                                         AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019,
			ROUND(SUM(IFF(bs.currency = 'GBP',
						  bs.margin_gross_of_toms_cc,
						  margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
			ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
			COUNT(DISTINCT bs.booking_id)                               AS bookings
		FROM latest_vault.cms_mongodb.booking_summary bs
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
			LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
			LEFT JOIN  se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			bs.currency = cc.base_currency
		WHERE (
				  sc.today
				  )
		GROUP BY ALL
	),
	tracy AS (
		-- model booking information from tracy
		SELECT
			CASE se.data.posa_category_from_territory(ds.posa_territory)
				WHEN 'UK' THEN 'UK'
				WHEN 'DACH' THEN 'DACH'
				ELSE 'Other'
			END                                              AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', oo.created_at_dts)            AS hour,
			sc.today,
			sc.yesterday,
			sc.today_last_week,
			sc.today_ly,
			sc.today_lly,
			sc.today_2019,
			ROUND(SUM(oo.margin * IFNULL(cc.multiplier, 1))) AS margin_gbp_constant_currency,
			ROUND(SUM(oo.margin * IFNULL(r.fx_rate, 1)))     AS margin_gbp,
			COUNT(DISTINCT oo.id)                            AS bookings
		FROM latest_vault.travelbird_mysql.orders_order oo
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON oo.created_at_dts::DATE = sc.date_value
			INNER JOIN data_vault_mvp.dwh.tb_offer tbo ON oo.offer_id = tbo.id
			INNER JOIN data_vault_mvp.dwh.dim_sale ds ON tbo.se_sale_id = ds.se_sale_id
-- orders_order.site_id > common_sitesettings.site_id > common_sitesettings.site_currency_id > currency_currency.id > currency_currency.code
			INNER JOIN latest_vault.travelbird_mysql.common_sitesettings cs ON oo.site_id = cs.site_id
			INNER JOIN latest_vault.travelbird_mysql.currency_currency cur ON cs.site_currency_id = cur.id
			INNER JOIN data_vault_mvp.fx.rates r
					   ON oo.created_at_dts::DATE = r.fx_date
						   AND r.source_currency = cur.code
						   AND r.target_currency = 'GBP'
			INNER JOIN se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			cur.code = cc.base_currency
		WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
		  AND ds.se_brand = 'SE Brand'
		  AND (
			sc.today
			)
		GROUP BY ALL
	),
	stack AS (
		SELECT *,
			   'camilla' AS tech_platform
		FROM camilla c
		UNION ALL
		SELECT *,
			   'tracy' AS tech_platform
		FROM tracy
	)
SELECT
	s.tech_platform,
	SUM(margin_gbp_constant_currency) AS margin_gbp_constant_currency,
	SUM(margin_gbp)                   AS margin_gbp,
	SUM(bookings)                     AS bookings
FROM stack s
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------
-- margin BY product configuration

WITH
	camilla AS (
		SELECT
			se.data.posa_category_from_territory(bs.territory)          AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
			ds.product_configuration,
			ROUND(SUM(IFF(bs.currency = 'GBP',
						  bs.margin_gross_of_toms_cc,
						  margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency
		FROM latest_vault.cms_mongodb.booking_summary bs
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
			LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
			LEFT JOIN  se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			bs.currency = cc.base_currency
		WHERE sc.today
		GROUP BY ALL
	)
		,
	tracy AS (
		SELECT
			se.data.posa_category_from_territory(ds.posa_territory) AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			DATE_TRUNC('hour', oo.created_at_dts)                   AS hour,
			ds.product_configuration,
			ROUND(SUM(oo.margin * IFNULL(cc.multiplier, 1)))        AS margin_gbp_constant_currency
		FROM latest_vault.travelbird_mysql.orders_order oo
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON oo.created_at_dts::DATE = sc.date_value
			INNER JOIN data_vault_mvp.dwh.tb_offer tbo ON oo.offer_id = tbo.id
			INNER JOIN data_vault_mvp.dwh.dim_sale ds ON tbo.se_sale_id = ds.se_sale_id
-- orders_order.site_id > common_sitesettings.site_id > common_sitesettings.site_currency_id > currency_currency.id > currency_currency.code
			INNER JOIN latest_vault.travelbird_mysql.common_sitesettings cs ON oo.site_id = cs.site_id
			INNER JOIN latest_vault.travelbird_mysql.currency_currency cur ON cs.site_currency_id = cur.id
			INNER JOIN se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			cur.code = cc.base_currency
		WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
		  AND ds.se_brand = 'SE Brand'
		  AND sc.today
		GROUP BY ALL
	),
	stack AS (
		SELECT *
		FROM camilla
		UNION ALL
		SELECT *
		FROM tracy
	)
SELECT
	territory,
	posu_cluster,
	posu_cluster_sub_region,
	hour,
	product_configuration,
	SUM(margin_gbp_constant_currency) AS margin_gbp_constant_currency
FROM stack
GROUP BY ALL

------------------------------------------------------------------------------------------------------------------------
-- top 10 margin

WITH
	camilla AS (
		SELECT
			bs.record__o:saleName::VARCHAR                     AS sale_name,
			bs.sale_id                                         AS se_sale_id,
			se.data.posa_category_from_territory(bs.territory) AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			ROUND(SUM(IFF(bs.currency = 'GBP', bs.margin_gross_of_toms_cc, bs.margin_gross_of_toms_cc * cc.multiplier)),
				  0)                                           AS margin_gbp_constant_currency,
			ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)         AS margin_gbp,
			COUNT(DISTINCT bs.booking_id)                      AS bookings
		FROM latest_vault.cms_mongodb.booking_summary bs
			LEFT JOIN se.data.dim_sale ds ON ds.se_sale_id = bs.sale_id
			LEFT JOIN se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			bs.currency = cc.base_currency
		WHERE bs.date_time_booked::DATE = CURRENT_DATE
		GROUP BY 1, 2, 3, 4, 5
	),
	tracy AS (
		SELECT
			ds.sale_name,
			ds.se_sale_id,
			se.data.posa_category_from_territory(ds.posa_territory) AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			ROUND(SUM(oo.margin * IFNULL(cc.multiplier, 1)))        AS margin_gbp_constant_currency,
			ROUND(SUM(oo.margin * IFNULL(r.fx_rate, 1)))            AS margin_gbp,
			COUNT(DISTINCT oo.id)                                   AS bookings
		FROM latest_vault.travelbird_mysql.orders_order oo
			INNER JOIN data_vault_mvp.dwh.se_calendar sc ON oo.created_at_dts::DATE = sc.date_value
			INNER JOIN data_vault_mvp.dwh.tb_offer tbo ON oo.offer_id = tbo.id
			INNER JOIN data_vault_mvp.dwh.dim_sale ds ON tbo.se_sale_id = ds.se_sale_id
-- orders_order.site_id > common_sitesettings.site_id > common_sitesettings.site_currency_id > currency_currency.id > currency_currency.code
			INNER JOIN latest_vault.travelbird_mysql.common_sitesettings cs ON oo.site_id = cs.site_id
			INNER JOIN latest_vault.travelbird_mysql.currency_currency cur ON cs.site_currency_id = cur.id
			INNER JOIN data_vault_mvp.fx.rates r
					   ON oo.created_at_dts::DATE = r.fx_date
						   AND r.source_currency = cur.code
						   AND r.target_currency = 'GBP'
			INNER JOIN se.data.constant_currency cc ON
			(CURRENT_DATE) >= cc.start_date AND
			(CURRENT_DATE) <= cc.end_date AND
			cc.currency = 'GBP' AND
			cc.category = 'Primary' AND
			cur.code = cc.base_currency
		WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
		  AND ds.se_brand = 'SE Brand'
		  AND sc.today
		GROUP BY ALL
	),
	stack AS (
		SELECT *
		FROM camilla
		UNION ALL
		SELECT *
		FROM tracy
	),
	margin_sums AS (
		SELECT
			sale_name,
			se_sale_id,
			territory,
			posu_cluster,
			posu_cluster_sub_region,
			SUM(margin_gbp_constant_currency) AS margin_gbp_constant_currency,
			SUM(margin_gbp)                   AS margin_gbp,
			SUM(bookings)                     AS bookings
		FROM stack s
		GROUP BY ALL
	),
	top_ten_sales_by_territory AS (
		SELECT
			sc.sale_name,
			sc.se_sale_id,
			sc.territory,
			sc.posu_cluster,
			sc.posu_cluster_sub_region,
			sc.margin_gbp_constant_currency,
			sc.margin_gbp,
			sc.bookings
		FROM margin_sums sc
		QUALIFY ROW_NUMBER(
				) OVER (
					PARTITION BY sc.territory ORDER BY sc.margin_gbp_constant_currency DESC) <= 10
	),
	lifetime_margin AS (
		SELECT
			fcb.se_sale_id,
			se.data.posa_category_from_territory(
					fcb.territory)                                  AS territory,
			tts.posu_cluster,
			tts.posu_cluster_sub_region,
			SUM(
					fcb.margin_gross_of_toms_gbp_constant_currency) AS lifetime_margin_constant_currency,
			SUM(
					fcb.margin_gross_of_toms_gbp)                   AS lifetime_margin,
			COUNT(
					DISTINCT fcb.booking_id)                        AS lifetime_bookings
		FROM se.data.fact_complete_booking fcb
			INNER JOIN top_ten_sales_by_territory tts ON fcb.se_sale_id = tts.se_sale_id AND
														 se.data.posa_category_from_territory(
																 fcb.territory) =
														 tts.territory
		GROUP BY 1, 2, 3, 4
	)

SELECT
	tts.sale_name,
	tts.se_sale_id,
	tts.territory,
	tts.posu_cluster,
	tts.posu_cluster_sub_region,
	ds.sale_start_date,
	ssa.company_name,
	tts.margin_gbp_constant_currency,
	tts.margin_gbp,
	tts.bookings,
	ls.lifetime_margin_constant_currency,
	ls.lifetime_margin,
	ls.lifetime_bookings
FROM top_ten_sales_by_territory tts
	LEFT JOIN lifetime_margin ls ON tts.se_sale_id = ls.se_sale_id AND tts.territory = ls.territory
	LEFT JOIN se.data.se_sale_attributes ssa ON tts.se_sale_id = ssa.se_sale_id
	LEFT JOIN se.data.dim_sale ds ON tts.se_sale_id = ds.se_sale_id
;



SELECT
	bs.record__o:saleName::VARCHAR                     AS sale_name,
	bs.sale_id                                         AS se_sale_id,
	se.data.posa_category_from_territory(bs.territory) AS territory,
	ds.posu_cluster,
	ds.posu_cluster_sub_region,
	ROUND(SUM(IFF(bs.currency = 'GBP', bs.margin_gross_of_toms_cc, bs.margin_gross_of_toms_cc * cc.multiplier)),
		  0)                                           AS margin_gbp_constant_currency,
	ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)         AS margin_gbp,
	COUNT(DISTINCT bs.booking_id)                      AS bookings
FROM latest_vault.cms_mongodb.booking_summary bs
	LEFT JOIN se.data.dim_sale ds ON ds.se_sale_id = bs.sale_id
	LEFT JOIN se.data.constant_currency cc ON
	(CURRENT_DATE) >= cc.start_date AND
	(CURRENT_DATE) <= cc.end_date AND
	cc.currency = 'GBP' AND
	cc.category = 'Primary' AND
	bs.currency = cc.base_currency
WHERE bs.date_time_booked::DATE = CURRENT_DATE
GROUP BY ALL


SELECT
	ds.sale_name,
	ds.se_sale_id,
	se.data.posa_category_from_territory(ds.posa_territory) AS territory,
	ds.posu_cluster,
	ds.posu_cluster_sub_region,
	DATE_TRUNC('hour', oo.created_at_dts)                   AS hour,
	ds.product_configuration,
	ROUND(SUM(oo.margin * IFNULL(cc.multiplier, 1)))        AS margin_gbp_constant_currency,
	ROUND(SUM(oo.margin * IFNULL(r.fx_rate, 1)))            AS margin_gbp,
	COUNT(DISTINCT oo.id)                                   AS bookings
FROM latest_vault.travelbird_mysql.orders_order oo
	INNER JOIN data_vault_mvp.dwh.se_calendar sc ON oo.created_at_dts::DATE = sc.date_value
	INNER JOIN data_vault_mvp.dwh.tb_offer tbo ON oo.offer_id = tbo.id
	INNER JOIN data_vault_mvp.dwh.dim_sale ds ON tbo.se_sale_id = ds.se_sale_id
-- orders_order.site_id > common_sitesettings.site_id > common_sitesettings.site_currency_id > currency_currency.id > currency_currency.code
	INNER JOIN latest_vault.travelbird_mysql.common_sitesettings cs ON oo.site_id = cs.site_id
	INNER JOIN latest_vault.travelbird_mysql.currency_currency cur ON cs.site_currency_id = cur.id
	INNER JOIN data_vault_mvp.fx.rates r
			   ON oo.created_at_dts::DATE = r.fx_date
				   AND r.source_currency = cur.code
				   AND r.target_currency = 'GBP'
	INNER JOIN se.data.constant_currency cc ON
	(CURRENT_DATE) >= cc.start_date AND
	(CURRENT_DATE) <= cc.end_date AND
	cc.currency = 'GBP' AND
	cc.category = 'Primary' AND
	cur.code = cc.base_currency
WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
  AND ds.se_brand = 'SE Brand'
  AND sc.today
GROUP BY ALL

------------------------------------------------------------------------------------------------------------------------
-- top 10 spvs

WITH
	spv_counts AS (
		SELECT
			e.contexts_com_secretescapes_content_context_1[0]['name']::VARCHAR           AS sale_name,
			e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR AS se_sale_id,
			se.data.posa_category_from_territory(COALESCE(
					se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
					REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
							'UK'),
					REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
												 ))                                      AS territory,
			ds.posu_cluster,
			ds.posu_cluster_sub_region,
			COUNT(*)                                                                     AS spvs
		FROM data_vault_mvp.dwh.trimmed_event_stream e
			LEFT JOIN se.data.dim_sale ds ON ds.se_sale_id = e.se_sale_id
		WHERE e.event_tstamp::DATE = CURRENT_DATE
		  AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
		GROUP BY 1, 2, 3, 4, 5
	),
	top_ten_sales_by_territory AS (
		SELECT
			sc.sale_name,
			sc.se_sale_id,
			sc.territory,
			sc.posu_cluster,
			sc.posu_cluster_sub_region,
			sc.spvs
		FROM spv_counts sc
		QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.spvs DESC) <= 10
	),
	lifetime_spvs AS (
		SELECT
			sts.se_sale_id,
			se.data.posa_category_from_territory(stmc.touch_affiliate_territory) AS territory,
			COUNT(*)                                                             AS lifetime_spvs
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
			INNER JOIN top_ten_sales_by_territory tts ON sts.se_sale_id = tts.se_sale_id AND
														 se.data.posa_category_from_territory(stmc.touch_affiliate_territory) =
														 tts.territory
		GROUP BY 1, 2
	)
SELECT
	tts.sale_name,
	tts.se_sale_id,
	ds.sale_start_date,
	ssa.company_name,
	tts.territory,
	tts.posu_cluster,
	tts.posu_cluster_sub_region,
	tts.spvs,
	ls.lifetime_spvs
FROM top_ten_sales_by_territory tts
	LEFT JOIN lifetime_spvs ls ON tts.se_sale_id = ls.se_sale_id AND tts.territory = ls.territory
	LEFT JOIN se.data.se_sale_attributes ssa ON tts.se_sale_id = ssa.se_sale_id
	LEFT JOIN se.data.dim_sale ds ON tts.se_sale_id = ds.se_sale_id
;



SELECT
	ds.tech_platform,
	e.page_url,
	e.contexts_com_secretescapes_content_context_1[0]['name']::VARCHAR           AS sale_name,
	e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR AS se_sale_id,
	se.data.posa_category_from_territory(COALESCE(
			se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
			REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
					'UK'),
			REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
										 ))                                      AS territory,
	ds.posu_cluster,
	ds.posu_cluster_sub_region,
FROM data_vault_mvp.dwh.trimmed_event_stream e
	LEFT JOIN se.data.dim_sale ds ON ds.se_sale_id = e.se_sale_id
WHERE e.event_tstamp::DATE = CURRENT_DATE
  AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
;

SELECT *
FROM data_vault_mvp.dwh.se_sale ssa
WHERE ssa.se_sale_id = 'A65330'



SELECT *
FROM se.data.search_location_translation slt
QUALIFY ROW_NUMBER() OVER (PARTITION BY slt.location ORDER BY slt.territory_translation_locale) = 1