WITH
	events AS (
		SELECT
			t.id                    AS territory_id,
			ssel.attributed_user_id AS user_id,
			sts.se_sale_id          AS deal_id,
			'deal-view'             AS evt_name,
			sts.event_tstamp::DATE  AS evt_date,
			MAX(sts.event_tstamp)   AS max_event_ts
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data_pii.scv_session_events_link ssel
					   ON sts.event_hash = ssel.event_hash
						   AND ssel.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON sts.touch_id = stmc.touch_id
			INNER JOIN latest_vault.cms_mysql.territory t
					   ON stmc.touch_affiliate_territory = t.name
			-- TODO adjust to include more dates:
		WHERE sts.event_tstamp >= CURRENT_DATE - 5
		GROUP BY 1, 2, 3, 4, 5

		UNION ALL

		SELECT
			t.id                    AS territory_id,
			ssel.attributed_user_id AS user_id,
			fcb.se_sale_id          AS deal_id,
			'order'                 AS evt_name,
			stt.event_tstamp::DATE  AS evt_date,
			MAX(stt.event_tstamp)   AS max_event_ts
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.fact_complete_booking fcb
					   ON stt.booking_id = fcb.booking_id
			INNER JOIN se.data_pii.scv_session_events_link ssel
					   ON stt.touch_id = ssel.touch_id
						   AND ssel.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON stt.touch_id = stmc.touch_id
			INNER JOIN latest_vault.cms_mysql.territory t
					   ON stmc.touch_affiliate_territory = t.name
			-- TODO adjust to include more dates:
		WHERE stt.event_tstamp >= CURRENT_DATE - 5
		GROUP BY 1, 2, 3, 4, 5

		UNION ALL

		SELECT
			t.id                    AS territory_id,
			ssel.attributed_user_id AS user_id,
			ses.se_sale_id          AS deal_id,
			'book-form'             AS evt_name,
			ses.event_tstamp::DATE  AS evt_date,
			MAX(ses.event_tstamp)   AS max_event_ts
		FROM se.data_pii.scv_event_stream ses
			INNER JOIN se.data_pii.scv_session_events_link ssel
					   ON ses.event_hash = ssel.event_hash
						   AND ssel.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data.scv_touch_marketing_channel stmc
					   ON ssel.touch_id = stmc.touch_id
			INNER JOIN latest_vault.cms_mysql.territory t
					   ON stmc.touch_affiliate_territory = t.name
			-- TODO adjust to include more dates:
		WHERE ses.event_tstamp >= CURRENT_DATE - 5
		  AND ses.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
		GROUP BY 1, 2, 3, 4, 5
	)
SELECT
	events.*
FROM events
	INNER JOIN data_science.tmp.valid_deals_filter vdf
			   ON vdf.deal_id = events.deal_id
				   AND vdf.territory_id = events.territory_id
;


------------------------------------------------------------------------------------------------------------------------
-- gecko killer code for spvs
SELECT
	se.data.posa_category_from_territory(COALESCE(
			se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
			REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
			REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
		))                            AS territory,
	DATE_TRUNC('hour', event_tstamp)  AS hour,
	event_tstamp::DATE = CURRENT_DATE AS today,
	e.event_tstamp_yesterday,
	e.event_tstamp_today_last_week,
	e.event_tstamp_today_ly,
	e.event_tstamp_today_lly,
	e.event_tstamp_today_2019,
	COUNT(*)                          AS spvs
FROM data_vault_mvp.dwh.trimmed_event_stream e
WHERE (--app spvs
	( -- new world native app event data
				e.collector_tstamp >= '2020-02-28 00:00:00'
			AND
				(
							e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
						OR
							e.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
					)
		)
	)
   OR (--web spvs
	(--server side tracking, post implementation/validation
				e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
			AND PARSE_URL(e.page_url, 1)['path']::VARCHAR NOT LIKE
				'%/sale-offers' -- remove issue where spv events were firing on offer pages
			AND e.is_server_side_event = TRUE
		)
	)
   OR --wrd spvs
			e.se_category = 'web redirect click'
		AND DATE_TRUNC('hour', event_tstamp) < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
------------------------------------------------------------------------------------------------------------------------
SELECT
	t.id                                                           AS territory_id,
	COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) AS user_id,
	es.se_sale_id                                                  AS deal_id,
	'deal-view'                                                    AS evt_name,
	es.event_tstamp::DATE                                          AS evt_date,
	MAX(es.event_tstamp)                                           AS max_event_ts
FROM hygiene_vault_mvp.snowplow.event_stream es
	INNER JOIN latest_vault.cms_mysql.territory t
			   ON COALESCE(
						  se.data.territory_from_affiliate_url_string(es.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
						  REPLACE(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR,
								  'GB', 'UK'),
						  REGEXP_REPLACE(es.app_id, '[ios_app |android_app ]')
					  ) = t.name
				   -- to utilise the identity stitching if available, but revert to tracking user id if not
	LEFT JOIN  data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
			   ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
				  COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
				   AND mis.stitched_identity_type = 'se_user_id'
WHERE es.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_TIMESTAMP
  -- remove spvs that we cannot associate with anyone
  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) IS NOT NULL
  AND es.se_sale_id IS NOT NULL
  AND (
	-- app spv filter
		(
					es.device_platform LIKE 'native app%'
				AND
					(
								es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
							OR
								es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
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
GROUP BY 1, 2, 3, 4, 5
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	t.id                    AS territory_id,
	ssel.attributed_user_id AS user_id,
	sts.se_sale_id          AS deal_id,
	'deal-view'             AS evt_name,
	sts.event_tstamp::DATE  AS evt_date,
	MAX(sts.event_tstamp)   AS max_event_ts
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON sts.event_hash = ssel.event_hash
				   AND ssel.stitched_identity_type = 'se_user_id'
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sts.touch_id = stmc.touch_id
	INNER JOIN latest_vault.cms_mysql.territory t
			   ON stmc.touch_affiliate_territory = t.name
	-- TODO adjust to include more dates:
WHERE sts.event_tstamp >= CURRENT_DATE - 5
GROUP BY 1, 2, 3, 4, 5
;

------------------------------------------------------------------------------------------------------------------------
-- validating output num rows
SELECT
	COUNT(*)
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON sts.event_hash = ssel.event_hash
				   AND ssel.stitched_identity_type = 'se_user_id'
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sts.touch_id = stmc.touch_id
	INNER JOIN latest_vault.cms_mysql.territory t
			   ON stmc.touch_affiliate_territory = t.name
	-- TODO adjust to include more dates:
WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
;


SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
	INNER JOIN latest_vault.cms_mysql.territory t
			   ON COALESCE(
						  se.data.territory_from_affiliate_url_string(es.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
						  REPLACE(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR,
								  'GB', 'UK'),
						  REGEXP_REPLACE(es.app_id, '[ios_app |android_app ]')
					  ) = t.name
				   -- to utilise the identity stitching if available, but revert to tracking user id if not
	LEFT JOIN  data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
			   ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
				  COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
				   AND mis.stitched_identity_type = 'se_user_id'
WHERE es.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
  -- remove spvs that we cannot associate with anyone
  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) IS NOT NULL
  AND es.se_sale_id IS NOT NULL
  AND (
	-- app spv filter
		(
					es.device_platform LIKE 'native app%'
				AND
					(
								es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
							OR
								es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
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
;


------------------------------------------------------------------------------------------------------------------------

SELECT
	t.id                    AS territory_id,
	stba.attributed_user_id AS user_id,
	sts.se_sale_id          AS deal_id,
	'deal-view'             AS evt_name,
	sts.event_tstamp::DATE  AS evt_date,
	MAX(sts.event_tstamp)   AS max_event_ts
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba
			   ON sts.touch_id = stba.touch_id
				   AND stba.stitched_identity_type = 'se_user_id'
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sts.touch_id = stmc.touch_id
	INNER JOIN latest_vault.cms_mysql.territory t
			   ON stmc.touch_affiliate_territory = t.name
	-- TODO adjust to include more dates:
WHERE sts.event_tstamp >= CURRENT_DATE - 5
GROUP BY 1, 2, 3, 4, 5
;


SELECT
	t.id                    AS territory_id,
	ssel.attributed_user_id AS user_id,
	sts.se_sale_id          AS deal_id,
	'deal-view'             AS evt_name,
	sts.event_tstamp::DATE  AS evt_date,
	MAX(sts.event_tstamp)   AS max_event_ts
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON sts.touch_id = ssel.touch_id
				   AND ssel.stitched_identity_type = 'se_user_id'
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sts.touch_id = stmc.touch_id
	INNER JOIN latest_vault.cms_mysql.territory t
			   ON stmc.touch_affiliate_territory = t.name
	-- TODO adjust to include more dates:
WHERE sts.event_tstamp >= CURRENT_DATE - 5
GROUP BY 1, 2, 3, 4, 5


------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.new_ds_spvs AS (
	SELECT
		ua.current_affiliate_territory_id                                   AS territory_id,
		COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
		es.se_sale_id                                                       AS deal_id,
		'deal-view'                                                         AS evt_name,
		es.event_tstamp::DATE                                               AS evt_date,
		MAX(es.event_tstamp)                                                AS max_event_ts
	FROM hygiene_vault_mvp.snowplow.event_stream es
		-- to utilise the identity stitching if available, but revert to tracking user id if not
		LEFT JOIN  data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
				   ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
					  COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
					   AND mis.stitched_identity_type = 'se_user_id'
		INNER JOIN data_vault_mvp.dwh.user_attributes ua
				   ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
	WHERE es.event_tstamp BETWEEN CURRENT_DATE - 1 AND CURRENT_TIMESTAMP -- TODO adjust
	  -- remove spvs that we cannot associate with anyone
	  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) IS NOT NULL
	  AND es.se_sale_id IS NOT NULL
	  AND (
		-- app spv filter
			(
						es.device_platform LIKE 'native app%'
					AND
						(
									es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
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
	GROUP BY 1, 2, 3, 4, 5
)
;


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.old_ds_spvs AS (
	SELECT
		t.id                         AS territory_id,
		ssel.attributed_user_id::INT AS user_id,
		sts.se_sale_id               AS deal_id,
		'deal-view'                  AS evt_name,
		sts.event_tstamp::DATE       AS evt_date,
		MAX(sts.event_tstamp)        AS max_event_ts
	FROM se.data.scv_touched_spvs sts
		INNER JOIN se.data_pii.scv_session_events_link ssel
				   ON sts.event_hash = ssel.event_hash
					   AND ssel.stitched_identity_type = 'se_user_id'
		INNER JOIN se.data.scv_touch_marketing_channel stmc
				   ON sts.touch_id = stmc.touch_id
		INNER JOIN latest_vault.cms_mysql.territory t
				   ON stmc.touch_affiliate_territory = t.name
		-- TODO adjust to include more dates:
	WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 1 AND CURRENT_TIMESTAMP
	GROUP BY 1, 2, 3, 4, 5
)
;


------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM scratch.robinpatel.new_ds_spvs nds
WHERE evt_date < CURRENT_DATE
;


SELECT *
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.attributed_user_id = '4049873'
  AND sts.event_tstamp >= CURRENT_DATE - 1
;



SELECT *
FROM scratch.robinpatel.new_ds_spvs nds
WHERE nds.user_id = '4049873'
;



SELECT *
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE stba.attributed_user_id = '24260633'
  AND sts.event_tstamp >= CURRENT_DATE - 1
;



SELECT *
FROM scratch.robinpatel.new_ds_spvs nds
WHERE nds.user_id = '24260633'
;

------------------------------------------------------------------------------------------------------------------------

SELECT
	COUNT(DISTINCT user_id)
FROM scratch.robinpatel.new_ds_spvs
WHERE new_ds_spvs.evt_date = CURRENT_DATE - 1
;

SELECT
	COUNT(DISTINCT user_id)
FROM scratch.robinpatel.old_ds_spvs
WHERE old_ds_spvs.evt_date = CURRENT_DATE - 1
;

SELECT
	COUNT(DISTINCT user_id)
FROM scratch.robinpatel.new_ds_spvs
;

------------------------------------------------------------------------------------------------------------------------

SELECT
	COUNT(DISTINCT deal_id)
FROM scratch.robinpatel.new_ds_spvs
WHERE new_ds_spvs.evt_date = CURRENT_DATE - 1
;

SELECT
	COUNT(DISTINCT deal_id)
FROM scratch.robinpatel.old_ds_spvs
WHERE old_ds_spvs.evt_date = CURRENT_DATE - 1
;

SELECT
	COUNT(DISTINCT deal_id)
FROM scratch.robinpatel.new_ds_spvs
;

------------------------------------------------------------------------------------------------------------------------
-- spvs
WITH
	spvs_before_today AS (
		-- historic spvs computed by scv
		SELECT
			ua.current_affiliate_territory_id AS territory_id,
			stba.attributed_user_id::INT      AS user_id,
			sts.se_sale_id                    AS deal_id,
			'deal-view'                       AS evt_name,
			sts.event_tstamp::DATE            AS evt_date,
			sts.event_tstamp                  AS event_ts
		FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id
						   AND stba.stitched_identity_type = 'se_user_id'
			INNER JOIN data_vault_mvp.dwh.user_attributes ua
					   ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
			-- filter to look at last 5 days minus today
		WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
	),
	spvs_today AS (
		-- today's spvs deduced directly from the event stream
		SELECT
			COALESCE(ua.current_affiliate_territory_id, ds.posa_territory_id,
					 se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR)) AS territory_id,
			COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT                                                                       AS user_id,
			es.se_sale_id                                                                                                                             AS deal_id,
			'deal-view'                                                                                                                               AS evt_name,
			es.event_tstamp::DATE                                                                                                                     AS evt_date,
			es.event_tstamp                                                                                                                           AS event_ts
		FROM hygiene_vault_mvp.snowplow.event_stream es
			-- to utilise the identity stitching if available, but revert to tracking user id if not
			LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
					  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
						 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
						  AND mis.stitched_identity_type = 'se_user_id'
			LEFT JOIN data_vault_mvp.dwh.user_attributes ua
					  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
			LEFT JOIN data_vault_mvp.dwh.dim_sale ds
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
	stack AS (
		SELECT
			sbt.territory_id,
			sbt.user_id,
			sbt.deal_id,
			sbt.evt_name,
			sbt.evt_date,
			sbt.event_ts
		FROM spvs_before_today sbt
		UNION ALL
		SELECT
			st.territory_id,
			st.user_id,
			st.deal_id,
			st.evt_name,
			st.evt_date,
			st.event_ts
		FROM spvs_today st
	)
SELECT
	s.territory_id,
	s.user_id,
	s.deal_id,
	s.evt_name,
	s.evt_date,
	MAX(s.event_ts) AS max_event_ts
FROM stack s
GROUP BY 1, 2, 3, 4, 5


------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM latest_vault.cms_mysql.territory t
WHERE id IN (1, 2, 4, 8, 9, 10, 11, 12, 14, 15, 25, 26, 27)
;

-- ID	NAME
-- 1	UK
-- 2	SE
-- 4	DE
-- 8	DK
-- 9	NO
-- 10	CH
-- 11	IT
-- 12	NL
-- 14	BE
-- 15	FR
-- 25	TB-BE_FR
-- 26	TB-BE_NL
-- 27	TB-NL

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_science.tmp.site_events_all AS (
	WITH
		deal_views AS (
			SELECT
				t.id                    AS territory_id,
				ssel.attributed_user_id AS user_id,
				sts.se_sale_id          AS deal_id,
				'deal-view'             AS evt_name,
				sts.event_tstamp::DATE  AS evt_date,
				MAX(sts.event_tstamp)   AS max_event_ts
			FROM se.data.scv_touched_spvs sts
				INNER JOIN se.data_pii.scv_session_events_link ssel
						   ON sts.touch_id = ssel.touch_id
							   AND ssel.stitched_identity_type = 'se_user_id'
				INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
				INNER JOIN latest_vault.cms_mysql.territory t ON stmc.touch_affiliate_territory = t.name
				-- TODO adjust to include more dates:
			WHERE sts.event_tstamp >= CURRENT_DATE - 5
			GROUP BY 1, 2, 3, 4, 5
		),
		orders AS (
			SELECT
				t.id                    AS territory_id,
				ssel.attributed_user_id AS user_id,
				fcb.se_sale_id          AS deal_id,
				'order'                 AS evt_name,
				stt.event_tstamp::DATE  AS evt_date,
				MAX(stt.event_tstamp)   AS max_event_ts
			FROM se.data.scv_touched_transactions stt
				INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
				INNER JOIN se.data_pii.scv_session_events_link ssel
						   ON stt.touch_id = ssel.touch_id
							   AND ssel.stitched_identity_type = 'se_user_id'
				INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
				INNER JOIN latest_vault.cms_mysql.territory t ON stmc.touch_affiliate_territory = t.name
				-- TODO adjust to include more dates:
			WHERE stt.event_tstamp >= CURRENT_DATE - 5
			GROUP BY 1, 2, 3, 4, 5
		),
		booking_forms AS (
			SELECT
				t.id                    AS territory_id,
				ssel.attributed_user_id AS user_id,
				ses.se_sale_id          AS deal_id,
				'book-form'             AS evt_name,
				ses.event_tstamp::DATE  AS evt_date,
				MAX(ses.event_tstamp)   AS max_event_ts
			FROM se.data_pii.scv_event_stream ses
				INNER JOIN se.data_pii.scv_session_events_link ssel
						   ON ses.event_hash = ssel.event_hash
							   AND ssel.stitched_identity_type = 'se_user_id'
				INNER JOIN se.data.scv_touch_marketing_channel stmc ON ssel.touch_id = stmc.touch_id
				INNER JOIN latest_vault.cms_mysql.territory t ON stmc.touch_affiliate_territory = t.name
				-- TODO adjust to include more dates:
			WHERE ses.event_tstamp >= CURRENT_DATE - 5
			  AND ses.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
			GROUP BY 1, 2, 3, 4, 5


		),
		events AS (
			SELECT
				ds.territory_id,
				ds.user_id,
				ds.deal_id,
				ds.evt_name,
				ds.evt_date,
				ds.max_event_ts
			FROM deal_views ds
			UNION ALL
			SELECT
				o.territory_id,
				o.user_id,
				o.deal_id,
				o.evt_name,
				o.evt_date,
				o.max_event_ts
			FROM orders o
			UNION ALL
			SELECT
				bf.territory_id,
				bf.user_id,
				bf.deal_id,
				bf.evt_name,
				bf.evt_date,
				bf.max_event_ts
			FROM booking_forms bf
		)
	SELECT
		e.*
	FROM events e
		INNER JOIN data_science.tmp.valid_deals_filter vdf
				   ON vdf.deal_id = e.deal_id
					   AND vdf.territory_id = e.territory_id
)
;

------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------ORDERS----------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.old_ds_bookings AS (
	SELECT
		t.id                    AS territory_id,
		ssel.attributed_user_id AS user_id,
		fcb.se_sale_id          AS deal_id,
		'order'                 AS evt_name,
		stt.event_tstamp::DATE  AS evt_date,
		MAX(stt.event_tstamp)   AS max_event_ts
	FROM se.data.scv_touched_transactions stt
		INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
		INNER JOIN se.data_pii.scv_session_events_link ssel
				   ON stt.touch_id = ssel.touch_id
					   AND ssel.stitched_identity_type = 'se_user_id'
		INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
		INNER JOIN latest_vault.cms_mysql.territory t ON stmc.touch_affiliate_territory = t.name
		-- TODO adjust to include more dates:
	WHERE stt.event_tstamp >= CURRENT_DATE - 5
	GROUP BY 1, 2, 3, 4, 5
)
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.new_ds_bookings AS (
	SELECT
		ua.current_affiliate_territory_id                                   AS territory_id,
		COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
		COALESCE(es.se_sale_id, fb.se_sale_id)                              AS deal_id,
		'order'                                                             AS evt_name,
		es.event_tstamp::DATE                                               AS evt_date,
		es.event_tstamp                                                     AS max_event_ts
	FROM hygiene_vault_mvp.snowplow.event_stream es
		-- to utilise the identity stitching if available, but revert to tracking user id if not
		LEFT JOIN  data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
				   ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
					  COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
					   AND mis.stitched_identity_type = 'se_user_id'

					   -- this join can be removed when ticket: https://secretescapes.atlassian.net/browse/SPI-3844 is resolved
		LEFT JOIN  data_vault_mvp.dwh.fact_booking fb ON
				es.useragent = 'data_team_artificial_insemination_transactions'
			AND es.booking_id = fb.booking_id
		INNER JOIN data_vault_mvp.dwh.user_attributes ua
				   ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
	WHERE es.event_tstamp >= CURRENT_DATE - 5
-- remove bookings that we cannot associate with anyone
	  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) IS NOT NULL
	  AND (
			( -- server side transactions
					( -- SE, we are using booking confirmation page view events due to latency of
						--update events not always able to be fired at time of the session
								es.event_name = 'page_view'
							AND es.v_tracker LIKE 'java-%' --SE
							AND
								es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM 'transaction complete'
						)
					OR
					( -- TB
								es.event_name = 'booking_update_event'
							AND es.v_tracker LIKE 'py-%' --TB
							AND
								es.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed'
						)
				)
			OR ( -- transaction events for transactions that weren't tracked.
				es.useragent = 'data_team_artificial_insemination_transactions'
				)
		)
-- booking confirmations can double fire, we want to take the earliest one
	QUALIFY ROW_NUMBER() OVER (PARTITION BY es.booking_id ORDER BY es.event_tstamp) = 1
)
;

SELECT *
FROM scratch.robinpatel.new_ds_bookings
;

SELECT *
FROM scratch.robinpatel.old_ds_bookings
;


SELECT
	COUNT(*)
FROM scratch.robinpatel.new_ds_bookings
WHERE evt_date = CURRENT_DATE - 1
;

SELECT
	COUNT(*)
FROM scratch.robinpatel.old_ds_bookings
WHERE evt_date = CURRENT_DATE - 1
;


SELECT
	COUNT(DISTINCT deal_id)
FROM scratch.robinpatel.new_ds_bookings
WHERE evt_date = CURRENT_DATE - 1
;

SELECT
	COUNT(DISTINCT deal_id)
FROM scratch.robinpatel.old_ds_bookings
WHERE evt_date = CURRENT_DATE - 1
;

SELECT
	user_id
FROM scratch.robinpatel.new_ds_bookings
WHERE evt_date = CURRENT_DATE - 1
EXCEPT

SELECT
	old_ds_bookings.user_id
FROM scratch.robinpatel.old_ds_bookings
WHERE evt_date = CURRENT_DATE - 1


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;
-- spvs
WITH
	spvs_before_today AS (
		-- historic spvs computed by scv
		SELECT
			se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
			stba.attributed_user_id::INT                                             AS user_id,
			sts.se_sale_id                                                           AS deal_id,
			'deal-view'                                                              AS evt_name,
			sts.event_tstamp::DATE                                                   AS evt_date,
			sts.event_tstamp                                                         AS event_ts
		FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
					   ON sts.touch_id = stba.touch_id
						   AND stba.stitched_identity_type = 'se_user_id'
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc
					   ON sts.touch_id = stmc.touch_id
			INNER JOIN data_vault_mvp.dwh.user_attributes ua
					   ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
			-- filter to look at last 5 days minus today
		WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
	),
	spvs_today AS (
		-- today's spvs deduced directly from the event stream
		SELECT
			COALESCE(
					se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
					ds.posa_territory_id,
					ua.current_affiliate_territory_id
				)::INT                                                          AS territory_id,
			es.se_sale_id                                                       AS deal_id,
			COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
			'deal-view'                                                         AS evt_name,
			es.event_tstamp::DATE                                               AS evt_date,
			es.event_tstamp                                                     AS event_ts
		FROM hygiene_vault_mvp.snowplow.event_stream es
			-- to utilise the identity stitching if available, but revert to tracking user id if not
			LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
					  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
						 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
						  AND mis.stitched_identity_type = 'se_user_id'
			LEFT JOIN data_vault_mvp.dwh.user_attributes ua
					  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
			LEFT JOIN data_vault_mvp.dwh.dim_sale ds
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
	stack AS (
		SELECT
			sbt.territory_id,
			sbt.user_id,
			sbt.deal_id,
			sbt.evt_name,
			sbt.evt_date,
			sbt.event_ts
		FROM spvs_before_today sbt
		UNION ALL
		SELECT
			st.territory_id,
			st.user_id,
			st.deal_id,
			st.evt_name,
			st.evt_date,
			st.event_ts
		FROM spvs_today st
	)
SELECT
	s.territory_id,
	s.user_id,
	s.deal_id,
	s.evt_name,
	s.evt_date,
	MAX(s.event_ts) AS max_event_ts
FROM stack s
GROUP BY 1, 2, 3, 4, 5


------------------------------------------------------------------------------------------------------------------------


-- bookings from fact booking up to previous date,
-- bookings from event_stream for any booking that doesn't exist in fact booking
WITH
	bookings_before_today AS (
		-- historic bookings that we can get from transactional databases
		SELECT
			se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
			fb.shiro_user_id                                                         AS user_id,
			fb.se_sale_id                                                            AS deal_id,
			'order'                                                                  AS evt_name,
			fb.booking_completed_date                                                AS evt_date,
			fb.booking_completed_timestamp                                           AS event_ts
		FROM data_vault_mvp.dwh.fact_booking fb
			INNER JOIN data_vault_mvp.dwh.user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt
					   ON fb.booking_id = mtt.booking_id
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc
					   ON mtt.touch_id = stmc.touch_id
		WHERE fb.booking_completed_date BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
		  AND fb.booking_status_type IN ('live', 'cancelled')
	),
	bookings_today AS (
		-- today's bookings deduced from the event stream, we remove double fire booking confirmation events
		-- by checking if these already exist in the booking data
		SELECT
			COALESCE(
					se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
					ds.posa_territory_id, -- if the spv is new data model sale, take it from sale data
					ua.current_affiliate_territory_id -- if these user joined before today
				)::INT                                                          AS territory_id,
			COALESCE(es.se_sale_id, fb.se_sale_id)                              AS deal_id,
			COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
			'order'                                                             AS evt_name,
			es.event_tstamp::DATE                                               AS evt_date,
			es.event_tstamp                                                     AS event_ts
		FROM hygiene_vault_mvp.snowplow.event_stream es
			-- to utilise the identity stitching if available, but revert to tracking user id if not
			LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
					  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
						 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
						  AND mis.stitched_identity_type = 'se_user_id'
			LEFT JOIN data_vault_mvp.dwh.fact_booking fb ON es.booking_id = fb.booking_id
			LEFT JOIN data_vault_mvp.dwh.user_attributes ua
					  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
			LEFT JOIN data_vault_mvp.dwh.dim_sale ds
					  ON es.se_sale_id = ds.se_sale_id AND ds.data_model = 'New Data Model'
		WHERE es.event_tstamp >= CURRENT_DATE
		  -- remove bookings that we are getting from fact booking and double fire booking events
		  AND fb.booking_id IS NULL
		  -- remove bookings that we cannot associate with anyone
		  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id) IS NOT NULL
		  AND (
			( -- server side transactions
					( -- SE, we are using booking confirmation page view events due to latency of
						--update events not always able to be fired at time of the session
								es.event_name = 'page_view'
							AND es.v_tracker LIKE 'java-%' --SE
							AND
								es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM 'transaction complete'
						)
					OR
					( -- TB
								es.event_name = 'booking_update_event'
							AND es.v_tracker LIKE 'py-%' --TB
							AND
								es.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed'
						)
				)
			)
		  -- booking confirmations can double fire, we want to take the earliest one
		QUALIFY ROW_NUMBER() OVER (PARTITION BY es.booking_id ORDER BY es.event_tstamp) = 1
	),

	stack AS (
		SELECT
			bbt.territory_id,
			bbt.user_id,
			bbt.deal_id,
			bbt.evt_name,
			bbt.evt_date,
			bbt.event_ts
		FROM bookings_before_today bbt
		UNION ALL
		SELECT
			bt.territory_id,
			bt.user_id,
			bt.deal_id,
			bt.evt_name,
			bt.evt_date,
			bt.event_ts
		FROM bookings_today bt
	)
SELECT
	s.territory_id,
	s.user_id,
	s.deal_id,
	s.evt_name,
	s.evt_date,
	MIN(s.event_ts) AS max_event_ts
FROM stack s
GROUP BY 1, 2, 3, 4, 5
;
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.new_ds_booking_form AS
WITH
	booking_form_before_today AS (
		SELECT
			se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
			ssel.attributed_user_id                                                  AS user_id,
			ses.se_sale_id                                                           AS deal_id,
			'book-form'                                                              AS evt_name,
			ses.event_tstamp::DATE                                                   AS evt_date,
			ses.event_tstamp                                                         AS event_ts
		FROM se.data_pii.scv_event_stream ses
			INNER JOIN se.data_pii.scv_session_events_link ssel
					   ON ses.event_hash = ssel.event_hash
						   AND ssel.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON ssel.touch_id = stmc.touch_id
			-- TODO adjust to include more dates:
		WHERE ses.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
		  AND ses.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
	),
	booking_form_today AS (
		SELECT
			COALESCE(
					se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
					ds.posa_territory_id, -- if the spv is new data model sale, take it from sale data
					ua.current_affiliate_territory_id -- if these user joined before today
				)::INT                                                          AS territory_id,
			es.se_sale_id                                                       AS deal_id,
			COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
			'book-form'                                                         AS evt_name,
			es.event_tstamp::DATE                                               AS evt_date,
			es.event_tstamp                                                     AS event_ts
		FROM hygiene_vault_mvp.snowplow.event_stream es
			LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
					  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
						 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
			LEFT JOIN data_vault_mvp.dwh.user_attributes ua
					  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
			LEFT JOIN data_vault_mvp.dwh.dim_sale ds
					  ON es.se_sale_id = ds.se_sale_id AND ds.data_model = 'New Data Model'
		WHERE es.event_tstamp >= CURRENT_DATE
		  AND es.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
-- remove booking form views where we cannot identify the user at all
		  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT IS NOT NULL
		  AND territory_id IS NULL
	),
	stack AS (
		SELECT
			bfbt.territory_id,
			bfbt.user_id,
			bfbt.deal_id,
			bfbt.evt_name,
			bfbt.evt_date,
			bfbt.event_ts
		FROM booking_form_before_today bfbt
		UNION ALL
		SELECT
			bft.territory_id,
			bft.user_id,
			bft.deal_id,
			bft.evt_name,
			bft.evt_date,
			bft.event_ts
		FROM booking_form_today bft
	)
SELECT
	s.territory_id,
	s.user_id,
	s.deal_id,
	s.evt_name,
	s.evt_date,
	MIN(s.event_ts) AS max_event_ts
FROM stack s
GROUP BY 1, 2, 3, 4, 5

------------------------------------------------------------------------------------------------------------------------
--old
WITH
	input_spvs AS (
		SELECT *
		FROM data_science.predictive_modeling.user_deal_events ude
		WHERE ude.evt_name = 'deal-view'
		  AND ude.evt_date >= CURRENT_DATE - 5
	)
SELECT
	iss.evt_date,
	COUNT(*),
	COUNT(DISTINCT iss.deal_id) AS deals,
	COUNT(DISTINCT iss.user_id) AS users
FROM input_spvs iss
GROUP BY 1
;


--new
WITH
	valid_deals_filter AS (
		WITH
			dim_sale AS (
				-- flattening old data model sales into multiple rows per territory
				SELECT
					se_sale_id,
					sale_active,
					sale_start_date,
					sale_end_date,
					TRIM(territory.value) AS posa_territory
				FROM se.data.dim_sale,
					 LATERAL SPLIT_TO_TABLE(IFNULL(posa_territory, ''), '|') AS territory
			)

		SELECT DISTINCT
			sa.se_sale_id AS deal_id,
			ts.id         AS territory_id
		FROM se.data.sale_active sa
			INNER JOIN dim_sale ds ON sa.se_sale_id = ds.se_sale_id
			INNER JOIN latest_vault.cms_mysql.territory ts ON ds.posa_territory = ts.name
		WHERE sa.view_date >= CURRENT_DATE - 60
		  AND ts.id IN (1, 2, 4, 8, 9, 10, 11, 12, 14, 15, 25, 26, 27)
	)
SELECT
	nds.evt_date,
	COUNT(*),
	COUNT(DISTINCT nds.deal_id) AS deals,
	COUNT(DISTINCT nds.user_id) AS users
FROM scratch.robinpatel.new_ds_spvs nds
	INNER JOIN valid_deals_filter vdf
			   ON vdf.deal_id = nds.deal_id
				   AND vdf.territory_id = nds.territory_id
GROUP BY 1
;

-- found that there is a big difference in the number of deals

-- New query
/*
EVT_DATE	COUNT(*)	DEALS	USERS
2023-05-11	390782		11279	136954
2023-05-12	341302		11646	124104
2023-05-13	310973		11440	106251
2023-05-14	459974		12194	151078
2023-05-15	418516		12246	144583
2023-05-16	99258		6384	42469
*/

-- Old query
/*
EVT_DATE	COUNT(*)	DEALS	USERS
2023-05-11	385469		8544	135287
2023-05-12	334364		8886	121676
2023-05-13	304967		9083	104026
2023-05-14	453440		9367	149324
2023-05-15	2615		1495	956
2023-05-19	1			1		1

 */

--new
SELECT
	nds.territory_id,
	nds.deal_id,
	nds.user_id,
	nds.evt_name,
	nds.evt_date
FROM scratch.robinpatel.new_ds_spvs nds
WHERE nds.evt_date = CURRENT_DATE - 5
EXCEPT
--old
SELECT
	ude.territory_id,
	ude.deal_id,
	ude.user_id,
	ude.evt_name,
	ude.evt_date
FROM data_science.predictive_modeling.user_deal_events ude
WHERE ude.evt_name = 'deal-view'
  AND ude.evt_date = CURRENT_DATE - 5

;


--new
SELECT
	nds.user_id,
	COUNT(DISTINCT nds.deal_id)
FROM scratch.robinpatel.new_ds_spvs nds
WHERE nds.evt_date = CURRENT_DATE - 5
GROUP BY 1
EXCEPT
--old
SELECT
	ude.user_id,
	COUNT(DISTINCT ude.deal_id)
FROM data_science.predictive_modeling.user_deal_events ude
WHERE ude.evt_name = 'deal-view'
  AND ude.evt_date = CURRENT_DATE - 5
GROUP BY 1

;


-- investigate user spvs on the 11th May 38995996

--old
SELECT
	ude.territory_id,
	ude.deal_id,
	ude.user_id,
	ude.evt_name,
	ude.evt_date,
	ude.max_event_ts
FROM data_science.predictive_modeling.user_deal_events ude
WHERE ude.evt_name = 'deal-view'
  AND ude.evt_date = CURRENT_DATE - 5
  AND ude.user_id = 38995996
;
--new
SELECT
	nds.territory_id,
	nds.deal_id,
	nds.user_id,
	nds.evt_name,
	nds.evt_date,
	nds.max_event_ts
FROM scratch.robinpatel.new_ds_spvs nds
WHERE nds.evt_date = CURRENT_DATE - 5
  AND nds.user_id = 38995996
;

SELECT *
FROM se.data.scv_touched_spvs sts
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 5
  AND stba.attributed_user_id = '38995996'
;

SELECT
	ds.posa_territory_id
FROM se.data.dim_sale ds


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--dev testing
CREATE SCHEMA data_science_dev_robin.predictive_modeling
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.predictive_modeling.user_deal_events_20230516 CLONE data_science_dev_robin.predictive_modeling.user_deal_events
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.predictive_modeling.user_deal_events CLONE data_science.predictive_modeling.user_deal_events
;



