-- top 100 deals by GPV by day week and month, UK DE, member‚

-- gpv in the last 7 days

-- share of domestic vs international
-- spvs by day
-- bookings by day

-- also check this view: data_science.predictive_modeling.generic_deal_recommendation
-- https://github.com/secretescapes/data-science/blob/master/models/ETL/source_views.sql

USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.sale_spvs_by_date AS
SELECT
	sts.event_tstamp::DATE                                                   AS spv_date,
	se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
	sts.se_sale_id,
	COUNT(DISTINCT stba.attributed_user_id_hash)                             AS users,
	COUNT(*)                                                                 AS spvs
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_basic_attributes stba
	ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= '2025-01-01'
GROUP BY ALL
;



CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.sale_margin_by_date AS
SELECT
	fcb.booking_completed_date,
	se.data.territory_id_from_territory_name(fcb.territory) AS territory_id,
	fcb.se_sale_id,
	COUNT(*)                                                AS bookings,
	SUM(fcb.margin_gross_of_toms_gbp)                       AS margin_gbp,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency)     AS margin_gbp_constant_currency
FROM se.data.fact_complete_booking fcb
GROUP BY ALL
;

-- SELECT
-- 	sc.date_value
-- FROM se.data.se_calendar sc
-- WHERE sc.date_value BETWEEN '2025-01-01' AND CURRENT_DATE
-- ;

-- manufacture a spine of sale id by date, this is based on spvs in scratch.robinpatel.sale_spvs_by_date
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.sale_by_date_grain AS
WITH
	first_spv AS (
		SELECT
			ssbd.se_sale_id,
			territory_id,
			MIN(spv_date) AS first_spv_date
		FROM scratch.robinpatel.sale_spvs_by_date ssbd
		GROUP BY 1, 2
	)
SELECT
	se_calendar.date_value,
	first_spv.territory_id,
	first_spv.se_sale_id
FROM first_spv
LEFT JOIN se.data.se_calendar
	ON se_calendar.date_value
	BETWEEN first_spv.first_spv_date AND CURRENT_DATE - 1
;

SELECT *
FROM scratch.robinpatel.sale_by_date_grain sbdg
WHERE sbdg.se_sale_id = 'A39915'
--
-- SELECT *
-- FROM scratch.robinpatel.sale_by_date_grain sbdg
-- WHERE sbdg.se_sale_id = 'A52675'
-- ;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.sale_by_date__spvs_and_margin AS
SELECT
	sbdg.date_value              AS date,
	sbdg.territory_id,
	sbdg.se_sale_id,
	COALESCE(ssbd.spvs, 0)       AS spvs,
	COALESCE(ssbd.users, 0)      AS users,
	COALESCE(smbd.bookings, 0)   AS bookings,
	COALESCE(smbd.margin_gbp, 0) AS margin_gbp
FROM scratch.robinpatel.sale_by_date_grain sbdg
LEFT JOIN scratch.robinpatel.sale_spvs_by_date ssbd
	ON sbdg.se_sale_id = ssbd.se_sale_id
	AND sbdg.date_value = ssbd.spv_date
	AND sbdg.territory_id = ssbd.territory_id
LEFT JOIN scratch.robinpatel.sale_margin_by_date smbd
	ON ssbd.se_sale_id = smbd.se_sale_id
	AND ssbd.spv_date = smbd.booking_completed_date
	AND ssbd.territory_id = smbd.territory_id
;

SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin sbdg
WHERE sbdg.se_sale_id = 'A39915' AND sbdg.territory_id = 1

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv AS
WITH
	gpv_calc AS
		(
			SELECT
				sale_metrics.date,
				sale_metrics.territory_id,
				sale_metrics.se_sale_id,
				sale_metrics.users,
				sale_metrics.spvs,
				sale_metrics.bookings,
				sale_metrics.margin_gbp,
				SUM(sale_metrics.spvs)
					OVER (PARTITION BY sale_metrics.se_sale_id, sale_metrics.territory_id
						ORDER BY sale_metrics.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7_days_spvs,
				SUM(sale_metrics.users)
					OVER (PARTITION BY sale_metrics.se_sale_id, sale_metrics.territory_id
						ORDER BY sale_metrics.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7_days_users,
				SUM(sale_metrics.margin_gbp)
					OVER (PARTITION BY sale_metrics.se_sale_id, sale_metrics.territory_id
						ORDER BY sale_metrics.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7_days_margin,
				IFF(last_7_days_spvs >= 500, last_7_days_margin / NULLIF(last_7_days_spvs, 0),
					UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM()))                         AS last_7_days_gpv,
				IFF(last_7_days_spvs >= 500, last_7_days_margin / NULLIF(last_7_days_users, 0),
					UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM()))                         AS last_7_days_gpv_using_users,
				COALESCE(
						IFF(last_7_days_spvs >= 500, last_7_days_margin / last_7_days_users,
							UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM())),
						UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM())
				)                                                                            AS user_deal_events_gpv
			FROM scratch.robinpatel.sale_by_date__spvs_and_margin sale_metrics

		)
SELECT
	gpv.*,
	ROW_NUMBER() OVER (PARTITION BY gpv.date, gpv.territory_id ORDER BY gpv.last_7_days_gpv DESC NULLS LAST) <=
	100                     AS is_day_top_100,
	ROW_NUMBER() OVER (PARTITION BY gpv.date, gpv.territory_id ORDER BY gpv.last_7_days_gpv_using_users DESC NULLS LAST) <=
	100                     AS is_day_top_100_using_users,
	ROW_NUMBER() OVER (PARTITION BY gpv.date, gpv.territory_id ORDER BY gpv.user_deal_events_gpv DESC NULLS LAST) <=
	100                     AS is_day_top_100_user_deal_events,
	last_7_days_spvs >= 500 AS has_more_500_spvs_last_7_days,
	ds.travel_type,
	ds.product_type,
	CASE
		WHEN ds.product_type IN ('Package', 'WRD') THEN 'Package'
		ELSE ds.travel_type
	END                     AS category
FROM gpv_calc gpv
INNER JOIN se.data.dim_sale ds
	ON gpv.se_sale_id = ds.se_sale_id
;


------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv


SELECT
	gpv.date,
	COUNT(DISTINCT gpv.se_sale_id)                                            AS top_100_sales,
	COUNT(DISTINCT IFF(gpv.category = 'International', gpv.se_sale_id, NULL)) AS international_sales,
	international_sales / top_100_sales                                       AS international_share,
	COUNT(DISTINCT IFF(gpv.category = 'Domestic', gpv.se_sale_id, NULL))      AS domestic_sales,
	domestic_sales / top_100_sales                                            AS domestic_share,
	COUNT(DISTINCT IFF(gpv.category = 'Package', gpv.se_sale_id, NULL))       AS package_sales,
	package_sales / top_100_sales                                             AS package_share,
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv gpv
WHERE gpv.territory_id = 1
  AND is_day_top_100
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, gpv.date)                                               AS month,
	COUNT(DISTINCT gpv.se_sale_id)                                            AS top_100_sales,
	COUNT(DISTINCT IFF(gpv.category = 'International', gpv.se_sale_id, NULL)) AS international_sales,
	international_sales / top_100_sales                                       AS international_share,
	COUNT(DISTINCT IFF(gpv.category = 'Domestic', gpv.se_sale_id, NULL))      AS domestic_sales,
	domestic_sales / top_100_sales                                            AS domestic_share,
	COUNT(DISTINCT IFF(gpv.category = 'Package', gpv.se_sale_id, NULL))       AS package_sales,
	package_sales / top_100_sales                                             AS package_share,
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv gpv
WHERE gpv.territory_id = 1
  AND is_day_top_100
GROUP BY 1
;


SELECT
	COUNT(DISTINCT gpv.se_sale_id)                                            AS top_100_sales,
	COUNT(DISTINCT IFF(gpv.category = 'International', gpv.se_sale_id, NULL)) AS international_sales,
	international_sales / top_100_sales                                       AS international_share,
	COUNT(DISTINCT IFF(gpv.category = 'Domestic', gpv.se_sale_id, NULL))      AS domestic_sales,
	domestic_sales / top_100_sales                                            AS domestic_share,
	COUNT(DISTINCT IFF(gpv.category = 'Package', gpv.se_sale_id, NULL))       AS package_sales,
	package_sales / top_100_sales                                             AS package_share,
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv gpv
WHERE gpv.territory_id = 1
  AND is_day_top_100
  AND gpv.date = CURRENT_DATE - 1


WITH
	int_v_dom AS
		(
			SELECT
				gdr.*,
				CASE
					WHEN ds.product_type IN ('Package', 'WRD') THEN 'Package'
					ELSE ds.travel_type
				END AS category
			FROM data_science.predictive_modeling.generic_deal_recommendations gdr
			INNER JOIN se.data.dim_sale ds
				ON gdr.deal_id = ds.se_sale_id
			WHERE territory_id = 1
-- 			QUALIFY ROW_NUMBER() OVER (PARTITION BY territory_id ORDER BY gpv DESC NULLS LAST) <= 100
			QUALIFY ROW_NUMBER() OVER (PARTITION BY territory_id ORDER BY rank_score DESC NULLS LAST) <= 100
		)
SELECT
	COUNT(DISTINCT deal_id)                                                AS top_100_sales,
	COUNT(DISTINCT IFF(gpv.category = 'International', gpv.deal_id, NULL)) AS international_sales,
	international_sales / top_100_sales                                    AS international_share,
	COUNT(DISTINCT IFF(gpv.category = 'Domestic', gpv.deal_id, NULL))      AS domestic_sales,
	domestic_sales / top_100_sales                                         AS domestic_share,
	COUNT(DISTINCT IFF(gpv.category = 'Package', gpv.deal_id, NULL))       AS package_sales,
	package_sales / top_100_sales                                          AS package_share,
FROM int_v_dom gpv
;


SELECT *
FROM data_science.predictive_modeling.generic_deal_recommendations
WHERE territory_id = 1 AND generic_deal_recommendations.deal_id = 'A39915'

-- robin query
SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.se_sale_id = 'A39915' AND
	  sale_by_date__spvs_and_margin_and_gpv.territory_id = 1
;


-- scv
SELECT
	sts.event_tstamp::DATE                                                   AS spv_date,
	se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
	sts.se_sale_id,
	stba.touch_se_brand,
	COUNT(*)                                                                 AS spvs
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_basic_attributes stba
	ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= '2025-01-01'
  AND sts.se_sale_id = 'A39915'
GROUP BY ALL
;

-- user deal events
SELECT
	user_deal_events.evt_date,
	COUNT(*)
FROM data_science.predictive_modeling.user_deal_events
WHERE user_deal_events.deal_id = 'A39915'
  AND user_deal_events.evt_date >= '2025-01-01'
  AND user_deal_events.territory_id = 1
  AND user_deal_events.evt_name = 'deal-view'
GROUP BY ALL
;



WITH
	user_deal_events AS (
		SELECT
			se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
			sts.se_sale_id                                                           AS deal_id,
			stba.attributed_user_id::INT                                             AS user_id,
			'deal-view'                                                              AS evt_name,
			sts.event_tstamp::DATE                                                   AS evt_date,
			sts.event_tstamp                                                         AS event_ts
		FROM se.data.scv_touched_spvs sts
		INNER JOIN se.data_pii.scv_touch_basic_attributes stba
			ON sts.touch_id = stba.touch_id
			AND stba.stitched_identity_type = 'se_user_id'
		INNER JOIN se.data.scv_touch_marketing_channel stmc
			ON sts.touch_id = stmc.touch_id
		INNER JOIN se.data.se_user_attributes ua
			ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
		-- filter to look at last 5 days minus today
		WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
		  AND sts.se_sale_id = 'A39915'
		  AND territory_id = 1
	)
SELECT
	user_deal_events.evt_date,
	COUNT(*)
FROM user_deal_events
GROUP BY 1
;


-- user deal events has 40% spvs than scv

------------------------------------------------------------------------------------------------------------------------


WITH
	se_brand_spvs_before_today AS (
		-- historic spvs computed by scv
		SELECT
			se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
			sts.se_sale_id                                                           AS deal_id,
			stba.attributed_user_id::INT                                             AS user_id,
			'deal-view'                                                              AS evt_name,
			sts.event_tstamp::DATE                                                   AS evt_date,
			sts.event_tstamp                                                         AS event_ts
		FROM se.data.scv_touched_spvs sts
		INNER JOIN se.data_pii.scv_touch_basic_attributes stba
			ON sts.touch_id = stba.touch_id
			AND stba.stitched_identity_type = 'se_user_id'
		INNER JOIN se.data.scv_touch_marketing_channel stmc
			ON sts.touch_id = stmc.touch_id
		INNER JOIN se.data.se_user_attributes ua
			ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
		-- filter to look at last 5 days minus today
		WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
		  AND sts.se_sale_id = 'A39915'
		  AND se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) = 1
	),

	se_brand_spvs_today AS (
		-- today's spvs deduced directly from the event stream
		SELECT
			COALESCE(
					se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
					ds.posa_territory_id,
					ua.current_affiliate_territory_id
			)::INT                                                              AS territory_id,
			es.se_sale_id                                                       AS deal_id,
			COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
			'deal-view'                                                         AS evt_name,
			es.event_tstamp::DATE                                               AS evt_date,
			es.event_tstamp                                                     AS event_ts
		FROM se.data_pii.scv_event_stream es
			-- to utilise the identity stitching if available, but revert to tracking user id if not
		LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
			ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
			   COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
			AND mis.stitched_identity_type = 'se_user_id'
		LEFT JOIN se.data.se_user_attributes ua
			ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
		LEFT JOIN se.data.dim_sale ds
			ON es.se_sale_id = ds.se_sale_id AND ds.data_model = 'New Data Model'
		WHERE es.event_tstamp::DATE = CURRENT_DATE
		  -- remove spvs that we cannot associate with anyone
		  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) IS NOT NULL
		  AND es.se_sale_id IS NOT NULL
		  AND (
			-- app spv filter
			(
				es.device_platform LIKE 'native app%'
					AND
				(
					es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR =
					'sale'
						OR
					es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR =
					'sale page'
					)
				)
				OR
				-- web spv filter
			(
				es.device_platform NOT LIKE 'native app%'
					AND es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
					AND PARSE_URL(es.page_url, 1)['path']::VARCHAR NOT LIKE
						'%/sale-offers' -- remove issue where spv events were firing on offer pages
					AND es.is_server_side_event = TRUE
				)
				OR
				-- wrd spv filter
			es.se_category = 'web redirect click'
			)
		  AND es.se_sale_id = 'A39915'
		  AND COALESCE(
					  se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
					  ds.posa_territory_id,
					  ua.current_affiliate_territory_id
			  )::INT = 1

	),
	stack AS (
		SELECT
			se_sbt.territory_id,
			se_sbt.deal_id,
			se_sbt.user_id,
			se_sbt.evt_name,
			se_sbt.evt_date,
			se_sbt.event_ts,
			'SE Brand' AS se_brand
		FROM se_brand_spvs_before_today se_sbt
		UNION ALL
		SELECT
			se_st.territory_id,
			se_st.deal_id,
			se_st.user_id,
			se_st.evt_name,
			se_st.evt_date,
			se_st.event_ts,
			'SE Brand' AS se_brand
		FROM se_brand_spvs_today se_st

	)
SELECT
	s.evt_date,
	COUNT(*)
FROM stack s
GROUP BY ALL
;


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.user_deal_views AS (
	WITH
		se_brand_spvs_before_today AS (
			-- historic spvs computed by scv
			SELECT
				se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
				sts.se_sale_id                                                           AS deal_id,
				stba.attributed_user_id::INT                                             AS user_id,
				'deal-view'                                                              AS evt_name,
				sts.event_tstamp::DATE                                                   AS evt_date,
				sts.event_tstamp                                                         AS event_ts
			FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba
				ON sts.touch_id = stba.touch_id
				AND stba.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
				ON sts.touch_id = stmc.touch_id
			INNER JOIN se.data.se_user_attributes ua
				ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
			-- filter to look at last 5 days minus today
			WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
		),
		tvl_spvs_before_today AS (
			-- historic spvs computed by scv
			SELECT
				se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
				sts.tb_offer_id                                                          AS deal_id,
				stba.attributed_user_id::INT                                             AS user_id,
				'deal-view'                                                              AS evt_name,
				sts.event_tstamp::DATE                                                   AS evt_date,
				sts.event_tstamp                                                         AS event_ts
			FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba
				ON sts.touch_id = stba.touch_id
				AND stba.stitched_identity_type = 'tvl_user_id'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
				ON sts.touch_id = stmc.touch_id
			INNER JOIN se.data.tvl_user_attributes ua
				ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.tvl_user_id
			-- filter to look at last 5 days minus today
			WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
		),
		se_brand_spvs_today AS (
			-- today's spvs deduced directly from the event stream
			SELECT
				COALESCE(
						se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
						ds.posa_territory_id,
						ua.current_affiliate_territory_id
				)::INT                                                              AS territory_id,
				es.se_sale_id                                                       AS deal_id,
				COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
				'deal-view'                                                         AS evt_name,
				es.event_tstamp::DATE                                               AS evt_date,
				es.event_tstamp                                                     AS event_ts
			FROM se.data_pii.scv_event_stream es
				-- to utilise the identity stitching if available, but revert to tracking user id if not
			LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
				ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
				   COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
				AND mis.stitched_identity_type = 'se_user_id'
			LEFT JOIN se.data.se_user_attributes ua
				ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
			LEFT JOIN se.data.dim_sale ds
				ON es.se_sale_id = ds.se_sale_id AND ds.data_model = 'New Data Model'
			WHERE es.event_tstamp::DATE = CURRENT_DATE
			  -- remove spvs that we cannot associate with anyone
			  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) IS NOT NULL
			  AND es.se_sale_id IS NOT NULL
			  AND (
				-- app spv filter
				(
					es.device_platform LIKE 'native app%'
						AND
					(
						es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR =
						'sale'
							OR
						es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR =
						'sale page'
						)
					)
					OR
					-- web spv filter
				(
					es.device_platform NOT LIKE 'native app%'
						AND es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
						AND PARSE_URL(es.page_url, 1)['path']::VARCHAR NOT LIKE
							'%/sale-offers' -- remove issue where spv events were firing on offer pages
						AND es.is_server_side_event = TRUE
					)
					OR
					-- wrd spv filter
				es.se_category = 'web redirect click'
				)
		),
		tvl_spvs_today AS (
			-- today's spvs deduced directly from the event stream
			SELECT
				COALESCE(
						se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
						ds.posa_territory_id,
						ua.territory_id)::INT                                        AS territory_id,
				ds.tb_offer_id                                                       AS deal_id,
				COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.tvl_user_id)::INT AS user_id,
				'deal-view'                                                          AS evt_name,
				es.event_tstamp::DATE                                                AS evt_date,
				es.event_tstamp                                                      AS event_ts
			FROM se.data_pii.scv_event_stream es
				-- to utilise the identity stitching if available, but revert to tracking user id if not
			LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
				ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
				   COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
				AND mis.stitched_identity_type = 'tvl_user_id'
			LEFT JOIN se.data.tvl_user_attributes ua
				ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.tvl_user_id)::INT = ua.tvl_user_id
			LEFT JOIN se.data.dim_sale ds
				ON 'TVL' || es.se_sale_id = ds.se_sale_id AND ds.se_brand = 'Travelist'
			WHERE es.event_tstamp::DATE = CURRENT_DATE
			  -- remove spvs that we cannot associate with anyone
			  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.tvl_user_id) IS NOT NULL
			  AND es.se_sale_id IS NOT NULL
			  AND
			  -- note: removed all app spv filter as we have no app
			  -- web spv filter
				(
					es.device_platform NOT LIKE 'native app%'
						AND es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
						AND es.is_server_side_event = TRUE
					)
		),
		stack AS (
			SELECT
				se_sbt.territory_id,
				se_sbt.deal_id,
				se_sbt.user_id,
				se_sbt.evt_name,
				se_sbt.evt_date,
				se_sbt.event_ts,
				'SE Brand' AS se_brand
			FROM se_brand_spvs_before_today se_sbt
			UNION ALL
			SELECT
				tvl_sbt.territory_id,
				tvl_sbt.deal_id,
				tvl_sbt.user_id,
				tvl_sbt.evt_name,
				tvl_sbt.evt_date,
				tvl_sbt.event_ts,
				'Travelist' AS se_brand
			FROM tvl_spvs_before_today tvl_sbt
			UNION ALL
			SELECT
				se_st.territory_id,
				se_st.deal_id,
				se_st.user_id,
				se_st.evt_name,
				se_st.evt_date,
				se_st.event_ts,
				'SE Brand' AS se_brand
			FROM se_brand_spvs_today se_st
			UNION ALL
			SELECT
				tvl_st.territory_id,
				tvl_st.deal_id,
				tvl_st.user_id,
				tvl_st.evt_name,
				tvl_st.evt_date,
				tvl_st.event_ts,
				'Travelist' AS se_brand
			FROM tvl_spvs_today tvl_st
		)
	SELECT
		s.territory_id,
		s.deal_id,
		s.user_id,
		s.evt_name,
		s.evt_date,
		s.se_brand,
		MAX(s.event_ts) AS max_event_ts
	FROM stack s
	GROUP BY 1, 2, 3, 4, 5, 6
)
;

SELECT
	udv.evt_date,
	COUNT(*)
FROM scratch.robinpatel.user_deal_views udv
WHERE udv.deal_id = 'A39915'
  AND udv.territory_id = 1
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------

-- robin query
SELECT
-- 	sale_by_date__spvs_and_margin_and_gpv.date,
DATE_TRUNC(MONTH, sale_by_date__spvs_and_margin_and_gpv.date) AS month,
SUM(sale_by_date__spvs_and_margin_and_gpv.spvs)               AS spvs,
SUM(sale_by_date__spvs_and_margin_and_gpv.users)              AS users,
SUM(sale_by_date__spvs_and_margin_and_gpv.margin_gbp)         AS margin_gbp
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.territory_id = 1
GROUP BY ALL
;


-- scv
SELECT
	sts.event_tstamp::DATE AS spv_date,
	COUNT(*)               AS spvs
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_basic_attributes stba
	ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= '2025-01-01'
  AND se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) = 1
  AND stba.touch_se_brand = 'SE Brand'
GROUP BY ALL
;

-- user deal events
SELECT
-- 	user_deal_events.evt_date,
DATE_TRUNC(MONTH, user_deal_events.evt_date) AS month,
COUNT(*)
FROM data_science.predictive_modeling.user_deal_events
WHERE user_deal_events.evt_date >= '2025-01-01'
  AND user_deal_events.territory_id = 1
  AND user_deal_events.evt_name = 'deal-view'
GROUP BY ALL
;
------------------------------------------------------------------------------------------------------------------------

SELECT
	DATE_TRUNC(MONTH, sale_by_date__spvs_and_margin_and_gpv.date) AS month,
	sale_by_date__spvs_and_margin_and_gpv.category,
	SUM(sale_by_date__spvs_and_margin_and_gpv.spvs)               AS spvs,
	SUM(sale_by_date__spvs_and_margin_and_gpv.users)              AS users,
	SUM(sale_by_date__spvs_and_margin_and_gpv.margin_gbp)         AS margin_gbp
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.territory_id = 1
GROUP BY ALL
;


SELECT
	DATE_TRUNC(MONTH, user_deal_events.evt_date) AS month,
	CASE
		WHEN ds.product_type IN ('Package', 'WRD') THEN 'Package'
		ELSE ds.travel_type
	END                                          AS category,
	COUNT(*)
FROM data_science.predictive_modeling.user_deal_events
INNER JOIN se.data.dim_sale ds
	ON user_deal_events.deal_id = ds.se_sale_id
WHERE user_deal_events.evt_date >= '2025-01-01'
  AND user_deal_events.territory_id = 1
  AND user_deal_events.evt_name = 'deal-view'
GROUP BY ALL
;

------------------------------------------------------------------------------------------------------------------------
-- checking gpv

SELECT *
FROM data_science.predictive_modeling.generic_deal_recommendations gmdr
WHERE gmdr.territory_id = 1
;

SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.territory_id = 1
;


SELECT *
FROM data_science.predictive_modeling.generic_deal_recommendations gmdr
WHERE gmdr.territory_id = 1 AND gmdr.deal_id = 'A77750'
;

SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.territory_id = 1 AND
	  sale_by_date__spvs_and_margin_and_gpv.se_sale_id = 'A77750'
;

SELECT
	deal_id,
	territory_id,
	user_deal_events.evt_date,
	COUNT(*) AS spv
FROM data_science.predictive_modeling.user_deal_events
WHERE evt_date > CURRENT_DATE - 7
  AND evt_name = 'deal-view'
  AND user_deal_events.deal_id = 'A77750'
GROUP BY ALL



WITH
	margin_table AS (
		SELECT
			se.data.territory_id_from_territory_name(fcb.territory)                   AS territory_id,
			-- NOTE: we are stacking se_sale_id for SE Brand and tb_offer_id for Travelist in deal_id
			IFF(fcb.se_brand = 'Travelist', fcb.tb_offer_id::VARCHAR, fcb.se_sale_id) AS deal_id,
			fcb.se_brand,
			SUM(fcb.margin_gross_of_toms_gbp)                                         AS margin_gbp
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.booking_completed_date BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 2
		GROUP BY 1, 2, 3
	),
	total_spv_events AS (
		SELECT
			deal_id,
			territory_id,
			COUNT(*) AS spv
		FROM data_science.predictive_modeling.user_deal_events
		WHERE evt_date BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 2
		  AND evt_name = 'deal-view'
		GROUP BY 1, 2
	),
	gpv_table AS (
		SELECT
			tse.deal_id,
			tse.territory_id,
			tse.spv,
			COALESCE(mt.margin_gbp, 0) AS margin_gbp,
			COALESCE(
					IFF(tse.spv >= 500, mt.margin_gbp / tse.spv, UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM())),
					UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM())
			)                          AS gpv
		FROM total_spv_events tse
		INNER JOIN margin_table mt
			ON tse.deal_id = mt.deal_id
			AND tse.territory_id = mt.territory_id
	)
SELECT
	vvd.territory_id,
	vvd.deal_id,
	gt.margin_gbp,
	gt.spv,
	gt.gpv,
	COALESCE(gt.gpv, UNIFORM(0::FLOAT, 0.09::FLOAT, RANDOM())) AS rank_score,
	vvd.se_brand
FROM data_science.predictive_modeling.vw_valid_deals vvd
LEFT JOIN gpv_table gt
	ON vvd.deal_id = gt.deal_id
	AND vvd.territory_id = gt.territory_id
WHERE gt.deal_id = 'A77750'
;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.territory_id = 1 AND
	  sale_by_date__spvs_and_margin_and_gpv.se_sale_id = 'A69210'
;


WITH
	margin_table AS (
		SELECT
			se.data.territory_id_from_territory_name(fcb.territory)                   AS territory_id,
			-- NOTE: we are stacking se_sale_id for SE Brand and tb_offer_id for Travelist in deal_id
			IFF(fcb.se_brand = 'Travelist', fcb.tb_offer_id::VARCHAR, fcb.se_sale_id) AS deal_id,
			fcb.se_brand,
			SUM(fcb.margin_gross_of_toms_gbp)                                         AS margin_gbp
		FROM se.data.fact_complete_booking fcb
		WHERE fcb.booking_completed_date BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 2
		GROUP BY 1, 2, 3
	),
	total_spv_events AS (
		SELECT
			deal_id,
			territory_id,
			COUNT(*) AS spv
		FROM data_science.predictive_modeling.user_deal_events
		WHERE evt_date BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 2
		  AND evt_name = 'deal-view'
		GROUP BY 1, 2
	),
	gpv_table AS (
		SELECT
			tse.deal_id,
			tse.territory_id,
			tse.spv,
			COALESCE(mt.margin_gbp, 0) AS margin_gbp,
			COALESCE(
					IFF(tse.spv >= 500, mt.margin_gbp / tse.spv, UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM())),
					UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM())
			)                          AS gpv
		FROM total_spv_events tse
		INNER JOIN margin_table mt
			ON tse.deal_id = mt.deal_id
			AND tse.territory_id = mt.territory_id
	)
SELECT
	vvd.territory_id,
	vvd.deal_id,
	gt.margin_gbp,
	gt.spv,
	gt.gpv,
	COALESCE(gt.gpv, UNIFORM(0::FLOAT, 0.09::FLOAT, RANDOM())) AS rank_score,
	vvd.se_brand
FROM data_science.predictive_modeling.vw_valid_deals vvd
LEFT JOIN gpv_table gt
	ON vvd.deal_id = gt.deal_id
	AND vvd.territory_id = gt.territory_id
WHERE gt.deal_id = 'A69210'
;


SELECT
	gpv.date,
	COUNT(DISTINCT gpv.se_sale_id)
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv gpv
WHERE gpv.is_day_top_100_user_deal_events
  AND gpv.territory_id = 1
GROUP BY 1
;

SELECT
-- 	DATE_TRUNC(MONTH, gpv.date)                                                  AS month,
gpv.date,
COUNT(*),
COUNT(DISTINCT IFF(category = 'International', gpv.se_sale_id, NULL))        AS international_sales,
COUNT(DISTINCT IFF(category = 'Package', gpv.se_sale_id, NULL))              AS package_sales,
COUNT(DISTINCT IFF(category = 'Domestic', gpv.se_sale_id, NULL))             AS domestic_sales,
COUNT(DISTINCT IFF(gpv.travel_type = 'Domestic', gpv.se_sale_id, NULL))      AS travel_type_domestic_sales,
COUNT(DISTINCT IFF(gpv.travel_type = 'International', gpv.se_sale_id, NULL)) AS travel_type_international_sales,
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv gpv
-- INNER JOIN data_science.predictive_modeling.vw_valid_deals valid_deals
-- 	ON gpv.se_sale_id = valid_deals.deal_id AND gpv.territory_id = valid_deals.territory_id
WHERE gpv.is_day_top_100_user_deal_events
  AND gpv.territory_id = 1
GROUP BY ALL
;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.territory_id = 1
  AND sale_by_date__spvs_and_margin_and_gpv.is_day_top_100_user_deal_events
  AND date >= CURRENT_DATE - 3
;


WITH
	top_100 AS (
		SELECT
			rd.*,
			ds.travel_type
		FROM data_science.predictive_modeling.generic_deal_recommendations rd
		INNER JOIN se.data.dim_sale ds
			ON ds.se_sale_id = rd.deal_id
		WHERE territory_id = 1
		ORDER BY rank_score DESC
		LIMIT 100
	)
SELECT
	travel_type,
	COUNT(*)
FROM top_100
GROUP BY 1
;


SELECT
	sts.event_tstamp::DATE                                         AS spv_date,
-- 	se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
-- 	sts.se_sale_id,
	COUNT(DISTINCT stba.attributed_user_id_hash || sts.se_sale_id) AS users,
	COUNT(*)                                                       AS spvs
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_basic_attributes stba
	ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
INNER JOIN se.data.scv_touch_marketing_channel stmc
	ON stba.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= CURRENT_DATE - 8
  AND se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) = 1
GROUP BY ALL
;

SELECT
	ude.evt_date,
	COUNT(*)
FROM data_science.predictive_modeling.user_deal_events ude
WHERE ude.territory_id = 1
  AND ude.evt_name = 'deal-view'
  AND ude.evt_date >= CURRENT_DATE - 8
GROUP BY 1;


SELECT *
FROM scratch.robinpatel.sale_by_date__spvs_and_margin_and_gpv
WHERE sale_by_date__spvs_and_margin_and_gpv.territory_id = 1
  AND sale_by_date__spvs_and_margin_and_gpv.is_day_top_100_user_deal_events
  AND date = '2025-08-29' -- removing a date because sends  would work on previous day's data
;