-- USE ROLE personal_role__robinpatel;
CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.predictive_modeling.user_deal_events CLONE data_science.predictive_modeling.user_deal_events
-- -- ;
-- Data modelling process model
-- https://app.diagrams.net/#G12tQTmOwrOsQxFtucGfvr1DEMX6gYkbyf
-- The prevailing pattern of modelling this event data is to utilise the single
-- customer view data to power all events prior to current day, this allows us
-- to leverage the accuracy of the scv when available. Current day events are
-- deduced directly from the event stream, accuracy of this data isn't
-- compromised but might slightly understate if we don't have enough supporting
-- information about the user available. This will be rectified in the next day
-- processing.

-- Set defaults for session
CREATE TABLE IF NOT EXISTS data_science_dev_robin.predictive_modeling.user_deal_events
(
	territory_id INT,
	deal_id      VARCHAR(30),
	user_id      INT,
	evt_name     VARCHAR(30),
	evt_date     DATE,
	max_event_ts TIMESTAMP
)
;

-- Creating a list of valid deals based on following logic:
-- filtering territory sales that are within specific territories and have
-- been active within the last 60 days
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

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.tmp.valid_deals_filter AS (
	WITH
		flattened_dim_sale AS (
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
		INNER JOIN flattened_dim_sale ds ON sa.se_sale_id = ds.se_sale_id
		INNER JOIN se.data.se_territory ts ON ds.posa_territory = ts.name
	WHERE sa.view_date >= CURRENT_DATE - 60
	  AND ts.id IN (1, 2, 4, 8, 9, 10, 11, 12, 14, 15, 25, 26, 27)
)
;

-- compute user spv metrics, this is split into two separate pieces of logic
-- first computes spvs metrics using the single customer view
-- second part uses event stream to compute spvs for current date
CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.tmp.user_deal_views AS (
	WITH
		spvs_before_today AS (
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
		stack AS (
			SELECT
				sbt.territory_id,
				sbt.deal_id,
				sbt.user_id,
				sbt.evt_name,
				sbt.evt_date,
				sbt.event_ts
			FROM spvs_before_today sbt
			UNION ALL
			SELECT
				st.territory_id,
				st.deal_id,
				st.user_id,
				st.evt_name,
				st.evt_date,
				st.event_ts
			FROM spvs_today st
		)
	SELECT
		s.territory_id,
		s.deal_id,
		s.user_id,
		s.evt_name,
		s.evt_date,
		MAX(s.event_ts) AS max_event_ts
	FROM stack s
	GROUP BY 1, 2, 3, 4, 5
)
;


-- compute user booking metrics, this is split into two separate pieces of logic
-- first computes bookings metrics using our transactional database data
-- second part uses event stream to compute bookings for current date
CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.tmp.user_orders AS (
	WITH
		bookings_before_today AS (
			-- historic bookings that we can get from transactional databases
			SELECT
				se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
				fb.se_sale_id                                                            AS deal_id,
				fb.shiro_user_id                                                         AS user_id,
				'order'                                                                  AS evt_name,
				fb.booking_completed_date                                                AS evt_date,
				fb.booking_completed_timestamp                                           AS event_ts
			FROM se.data.fact_booking fb
				INNER JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
				INNER JOIN se.data.scv_touched_transactions mtt
						   ON fb.booking_id = mtt.booking_id
				INNER JOIN se.data.scv_touch_marketing_channel stmc
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
			FROM se.data_pii.scv_event_stream es
				-- to utilise the identity stitching if available, but revert to tracking user id if not
				LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
						  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
							 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
							  AND mis.stitched_identity_type = 'se_user_id'
				LEFT JOIN se.data.fact_booking fb ON es.booking_id = fb.booking_id
				LEFT JOIN se.data.se_user_attributes ua
						  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
				LEFT JOIN se.data.dim_sale ds
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
				bbt.deal_id,
				bbt.user_id,
				bbt.evt_name,
				bbt.evt_date,
				bbt.event_ts
			FROM bookings_before_today bbt
			UNION ALL
			SELECT
				bt.territory_id,
				bt.deal_id,
				bt.user_id,
				bt.evt_name,
				bt.evt_date,
				bt.event_ts
			FROM bookings_today bt
		)
	SELECT
		s.territory_id,
		s.deal_id,
		s.user_id,
		s.evt_name,
		s.evt_date,
		MIN(s.event_ts) AS max_event_ts
	FROM stack s
	GROUP BY 1, 2, 3, 4, 5
)
;

-- compute booking form views using the event stream
CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.tmp.user_book_forms AS (
	WITH
		booking_form_before_today AS (
			SELECT
				se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
				ses.se_sale_id                                                           AS deal_id,
				ssel.attributed_user_id                                                  AS user_id,
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
			FROM se.data_pii.scv_event_stream es
				LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
						  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
							 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
				LEFT JOIN se.data.se_user_attributes ua
						  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
				LEFT JOIN se.data.dim_sale ds
						  ON es.se_sale_id = ds.se_sale_id AND ds.data_model = 'New Data Model'
			WHERE es.event_tstamp >= CURRENT_DATE
			  AND es.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
			  -- remove booking form views where we cannot identify the user at all
			  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT IS NOT NULL
		),
		stack AS (
			SELECT
				bfbt.territory_id,
				bfbt.deal_id,
				bfbt.user_id,
				bfbt.evt_name,
				bfbt.evt_date,
				bfbt.event_ts
			FROM booking_form_before_today bfbt
			UNION ALL
			SELECT
				bft.territory_id,
				bft.deal_id,
				bft.user_id,
				bft.evt_name,
				bft.evt_date,
				bft.event_ts
			FROM booking_form_today bft
		)
	SELECT
		s.territory_id,
		s.deal_id,
		s.user_id,
		s.evt_name,
		s.evt_date,
		MIN(s.event_ts) AS max_event_ts
	FROM stack s
	GROUP BY 1, 2, 3, 4, 5
)
;


-- Create input list of events
CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.tmp.site_events_all AS (
	WITH
		events AS (
			SELECT
				ds.territory_id,
				ds.deal_id,
				ds.user_id,
				ds.evt_name,
				ds.evt_date,
				ds.max_event_ts
			FROM data_science_dev_robin.tmp.user_deal_views ds
			UNION ALL
			SELECT
				uo.territory_id,
				uo.deal_id,
				uo.user_id,
				uo.evt_name,
				uo.evt_date,
				uo.max_event_ts
			FROM data_science_dev_robin.tmp.user_orders uo
			UNION ALL
			SELECT
				bf.territory_id,
				bf.deal_id,
				bf.user_id,
				bf.evt_name,
				bf.evt_date,
				bf.max_event_ts
			FROM data_science_dev_robin.tmp.user_book_forms bf
		)
	SELECT
		e.*
	FROM events e
		INNER JOIN data_science_dev_robin.tmp.valid_deals_filter vdf
				   ON vdf.deal_id = e.deal_id
					   AND vdf.territory_id = e.territory_id
)
;

SELECT *
FROM data_science_dev_robin.tmp.site_events_all
LIMIT 100
;

BEGIN TRANSACTION
;

DELETE
FROM data_science_dev_robin.predictive_modeling.user_deal_events
WHERE evt_date >= CURRENT_DATE - 5
;

COMMIT
;

INSERT INTO data_science_dev_robin.predictive_modeling.user_deal_events (territory_id,
																		 deal_id,
																		 user_id,
																		 evt_name,
																		 evt_date,
																		 max_event_ts)
SELECT
	territory_id,
	deal_id,
	user_id,
	evt_name,
	evt_date,
	max_event_ts
FROM data_science_dev_robin.tmp.site_events_all g
;


DROP TABLE IF EXISTS data_science_dev_robin.tmp.valid_deals_filter
;

DROP TABLE IF EXISTS data_science_dev_robin.tmp.user_deal_views
;

DROP TABLE IF EXISTS data_science_dev_robin.tmp.user_orders
;

DROP TABLE IF EXISTS data_science_dev_robin.tmp.user_book_forms
;

DROP TABLE IF EXISTS data_science_dev_robin.tmp.site_events_all
;


-- print example data
SELECT *
FROM data_science_dev_robin.predictive_modeling.user_deal_events
ORDER BY user_id, max_event_ts
LIMIT 100
;

SELECT DISTINCT
	evt_name
FROM data_science_dev_robin.predictive_modeling.user_deal_events
;

SELECT
	territory_id,
	MAX(evt_date) AS max_date,
	MIN(evt_date) AS min_date
FROM data_science_dev_robin.predictive_modeling.user_deal_events
GROUP BY 1
;

WITH
	orders AS (
		SELECT
			territory_id,
			COUNT(*) AS order_count
		FROM data_science_dev_robin.predictive_modeling.user_deal_events
		WHERE evt_name = 'order'
		GROUP BY 1
	),
	booking_forms AS (
		SELECT
			territory_id,
			COUNT(*) AS booking_form_count
		FROM data_science_dev_robin.predictive_modeling.user_deal_events
		WHERE evt_name = 'book-form'
		GROUP BY 1
	),
	spvs AS (
		SELECT
			territory_id,
			COUNT(*) AS spv_count
		FROM data_science_dev_robin.predictive_modeling.user_deal_events
		WHERE evt_name = 'deal-view'
		GROUP BY 1
	)
SELECT
	o.territory_id,
	t.name                               AS territory_code,
	o.order_count,
	b.booking_form_count,
	s.spv_count,
	b.booking_form_count / s.spv_count   AS booking_form_to_spv_count,
	o.order_count / b.booking_form_count AS order_to_booking_form_count
FROM orders o
	INNER JOIN booking_forms b ON o.territory_id = b.territory_id
	INNER JOIN spvs s ON o.territory_id = s.territory_id
	INNER JOIN latest_vault.cms_mysql.territory t ON o.territory_id = t.id
ORDER BY 1
;


------------------------------------------------------------------------------------------------------------------------
-- dev daily summary
SELECT
	ude.evt_date,
	COUNT(*),
	COUNT(DISTINCT ude.deal_id) AS deals,
	COUNT(DISTINCT ude.user_id)
FROM data_science_dev_robin.predictive_modeling.user_deal_events ude
WHERE ude.evt_date >= CURRENT_DATE - 10
GROUP BY 1
;

-- prod daily summary
SELECT
	ude.evt_date,
	COUNT(*),
	COUNT(DISTINCT ude.deal_id) AS deals,
	COUNT(DISTINCT ude.user_id)
FROM data_science.predictive_modeling.user_deal_events ude
WHERE ude.evt_date >= CURRENT_DATE - 10
GROUP BY 1
;

-- check diffs by event type


-- dev event daily summary
SELECT
	ude.evt_date,
	ude.evt_name,
	COUNT(*),
	COUNT(DISTINCT ude.deal_id) AS deals,
	COUNT(DISTINCT ude.user_id)
FROM data_science_dev_robin.predictive_modeling.user_deal_events ude
WHERE ude.evt_date >= CURRENT_DATE - 10
GROUP BY 1, 2
;

-- prod event daily summary
SELECT
	ude.evt_date,
	ude.evt_name,
	COUNT(*),
	COUNT(DISTINCT ude.deal_id) AS deals,
	COUNT(DISTINCT ude.user_id)
FROM data_science.predictive_modeling.user_deal_events ude
WHERE ude.evt_date >= CURRENT_DATE - 10
GROUP BY 1, 2
;


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
FROM se.data_pii.scv_event_stream es
	-- to utilise the identity stitching if available, but revert to tracking user id if not
	LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
			  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
				 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
				  AND mis.stitched_identity_type = 'se_user_id'
	LEFT JOIN se.data.fact_booking fb ON es.booking_id = fb.booking_id
	LEFT JOIN se.data.se_user_attributes ua
			  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
	LEFT JOIN se.data.dim_sale ds
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


