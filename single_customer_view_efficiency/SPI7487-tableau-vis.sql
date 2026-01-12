-- event stream

WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_event_stream esh
		WHERE esh.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.event_stream_2025_07_14.event_stream es
		WHERE es.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_tstamp::DATE AS date,
	COUNT(*)                 AS events
FROM stack
GROUP BY ALL
;

-- attribution
WITH
	stack AS (
		SELECT
			'historical' AS source,
			mta.* EXCLUDE archive_source,
			mtmc.touch_mkt_channel,
		FROM single_customer_view_historical.unioned_data.historical_module_touch_attribution mta
			INNER JOIN single_customer_view_historical.unioned_data.historical_module_touch_marketing_channel mtmc
					   ON mta.attributed_touch_id = mtmc.touch_id
						   AND mtmc.touch_start_tstamp >= '2018-01-01'
		WHERE mta.touch_start_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			a.*,
			mtmc.touch_mkt_channel
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touch_attribution a
			INNER JOIN single_customer_view_historical.single_customer_view_2025_07_14.module_touch_marketing_channel mtmc
					   ON a.attributed_touch_id = mtmc.touch_id
						   AND mtmc.touch_start_tstamp >= '2018-01-01'
		WHERE a.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.attribution_model,
	stack.touch_mkt_channel,
	stack.touch_start_tstamp::DATE AS session_date,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL

-- touch basic attributes
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_module_touch_basic_attributes mtba
		WHERE mtba.touch_start_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touch_basic_attributes mtba
		WHERE mtba.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.touch_experience,
	stack.touch_start_tstamp::DATE AS date,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL

-- touch channel
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_module_touch_marketing_channel mtm
		WHERE mtm.touch_start_tstamp >= '2018-01-01'
		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touch_marketing_channel mtba
		WHERE mtba.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.touch_start_tstamp::DATE AS session_date,
	stack.touch_mkt_channel,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL

-- touched booking form views
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_module_touched_booking_form_views mtbfv
		WHERE mtbfv.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touched_booking_form_views m
		WHERE m.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_category,
	stack.event_tstamp::DATE AS bfv_date,
	COUNT(*)                 AS bfvs
FROM stack
GROUP BY ALL

-- touched feature flags
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_module_touched_feature_flags mtff
		WHERE mtff.touch_start_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touched_feature_flags f
		WHERE f.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.feature_flag,
	stack.touch_start_tstamp::DATE AS session_date,
	COUNT(*)                       AS feature_flags
FROM stack
GROUP BY ALL

-- touched searches
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_module_touched_searches s
		WHERE s.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touched_searches s
		WHERE s.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.se_brand,
	stack.event_category,
	stack.triggered_by,
	stack.event_tstamp::DATE AS search_date,
	COUNT(*)                 AS searches
FROM stack
GROUP BY ALL

-- touched spvs
WITH
	stack AS (
		SELECT
			'historical' AS source,
			*  EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_module_touched_spvs mts
		WHERE mts.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touched_spvs m
		WHERE m.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_category,
	stack.event_tstamp::DATE AS spv_date,
	COUNT(*)                 AS spvs
FROM stack
GROUP BY ALL

-- touched transactions
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical.unioned_data.historical_module_touched_transactions mtt
		WHERE mtt.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'archive' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touched_transactions m
		WHERE m.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_subcategory,
	stack.event_tstamp::DATE AS event_date,
	COUNT(*)                 AS transactions
FROM stack
GROUP BY ALL