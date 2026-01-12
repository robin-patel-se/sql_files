--AB test conditions
-- All territories
-- MWeb, Web, and Tablet Web only - excludes app
-- Sessions have seen at least 1 SPV
-- TODO look at logged in sessions only?
-- 50:50 traffic split
-- measure only the 80% of sales that see reviews using the COLLAB.MARKETING.TRUSTYOU__HOTEL_MAPPING table
-- Go Live Date: '2023-07-12'

--1: Gather together all the relevant sales for this test
WITH
	sales_80 AS (
		SELECT
			se_sale_id,
			hm.client_id
			-- this is distinct on se_sale_id, not distinct on hotel_code
		FROM se.data.se_sale_attributes AS ssa
			INNER JOIN collab.marketing.trustyou__hotel_mapping AS hm
					   ON ssa.hotel_code = hm.client_id
	),
-- distinct on se_sale_id; clean
-- won't be distinct on client_id because this is equivalent to hotel_code, and hotels can have multiple sale_ids.

--2: Identify sessions that have had at least 1 spv
	identify_spvs_80 AS (
		SELECT
			spvs.event_hash,
			spvs.se_sale_id,
			spvs.touch_id,
			spvs.event_tstamp,
			DATE(spvs.event_tstamp) AS spv_date
		FROM se.data.scv_touched_spvs AS spvs
			INNER JOIN sales_80 AS s8
					   ON spvs.se_sale_id = s8.se_sale_id
	),
-- distinct on event_hash; clean

--Aggregate this to touch level
	identify_touches_80 AS (
		SELECT DISTINCT
			touch_id
		FROM identify_spvs_80
	),
-- distinct on touch_id;clean
--We don't care about the spvs themselves - just want to know which touches that were eligible for reviews had an spv.

--3: Identify bookings
	transactions_cte AS (
		SELECT
			stt.booking_id,
			stt.touch_id,
			-- added as a way to check; 100% of rows should say live
			fcb.booking_status_type
			-- stt contains live bookings as well as other booking types
		FROM se.data.scv_touched_transactions AS stt
			-- inner joining means we will only return live bookings
			INNER JOIN se.data.fact_booking AS fcb
					   ON stt.booking_id = fcb.booking_id
			LEFT JOIN  se.data.dim_sale AS ds
					   ON fcb.se_sale_id = ds.se_sale_id
		WHERE fcb.booking_status_type IN ('live', 'cancelled') -- include cancelled bookings because people still booked these; use cancelled bookings as well as live unless we think the change will have an impact on canx rates
	),
-- distinct on booking_id; clean

--4: Identify Feature Flags
	feature_flag_selector AS (
		SELECT
			touch_id,
			feature_flag,
			COUNT(touch_id) OVER (PARTITION BY touch_id) AS rows_
		FROM se.data.scv_touched_feature_flags
		WHERE DATE(touch_start_tstamp) >= '2023-07-12' -- go live date
		  AND feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
	),
-- NOT distinct on touch_id; this is because any touch can have multiple feature flags, so we're likely to see multiple rows for the same feature flag
-- 0 dupes (flags aren't live yet though! 23/07/11)

-- identifies duplicates in feature_flags_selector
	duplicate_identifier AS (
		SELECT DISTINCT
			touch_id
		FROM feature_flag_selector
		WHERE rows_ > 1
	),


--5: Aggregate to session level
	session_level AS (
		SELECT
			tba.touch_id,
			tba.touch_start_tstamp,
			DATE(tba.touch_start_tstamp) AS session_date,
			touch_hostname_territory     AS territory,
			platform                     AS device,
			MAX(CASE
					WHEN ffs.feature_flag = 'sale.reviews.enabled' THEN 'Test'
					WHEN ffs.feature_flag = 'sale.reviews.control' THEN 'Control'
				END)                     AS feature_flag,
			COUNT(trx.booking_id)        AS bookings
		FROM se.data_pii.scv_touch_basic_attributes AS tba
			INNER JOIN identify_touches_80 AS is80 -- inner joining bc we only want sessions w an spv for the 80% of sales w reviews
					   ON is80.touch_id = tba.touch_id -- clean join
			LEFT JOIN  feature_flag_selector AS ffs
					   ON tba.touch_id = ffs.touch_id -- clean join
			LEFT JOIN  duplicate_identifier AS di
					   ON di.touch_id = tba.touch_id -- clean join
			-- join last because this isn't at touch level
			LEFT JOIN  transactions_cte AS trx
					   ON tba.touch_id = trx.touch_id
		WHERE
		  -- date rollout was first set live
			DATE(tba.touch_start_tstamp) >= '2023-07-12'
		  AND platform IN ('Web', 'Mobile Web', 'Tablet Web')
		  AND di.touch_id IS NULL -- this should remove duplicates identified in the duplicates_identifier bit
		GROUP BY 1, 2, 3, 4, 5
	),
-- distinct on touch_id; clean

	sessions_agg AS (
		SELECT
			session_date,
			feature_flag,
			device,
			territory,
			SUM(bookings)   AS num_bookings,
			COUNT(touch_id) AS num_sessions
		FROM session_level
		GROUP BY 1, 2, 3, 4
		ORDER BY 1, 2, 3, 4
	)

SELECT *
FROM sessions_agg


------------------------------------------------------------------------------------------------------------------------


WITH
	session_test_data AS (
		SELECT
			stff.touch_id,
			COUNT(*) > 1                                                   AS has_any_test,
			SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0 AS review_test,
			SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 AS review_control,
			review_test AND review_control                                 AS review_both
		FROM se.data.scv_touched_feature_flags stff
-- 		WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
		GROUP BY 1
	),

	touch_ids_with_spv AS (
		SELECT DISTINCT
			sts.touch_id
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= '2023-06-01'
		  -- remove spvs on other domains (eg. tracy)
		  AND PARSE_URL(sts.page_url)['host']::VARCHAR = 'www.secretescapes.com'
	),


;



USE WAREHOUSE pipe_xlarge;

SELECT * FROM data_vault_mvp.dwh.se_sale ss;


USE ROLE personal_role__robinpatel;