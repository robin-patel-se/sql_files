--AB test conditions as follows:
-- All territories
-- MWeb, Web, and Tablet Web only - excludes app
-- Users have clicked on at least 1 relevant sale card (in our reporting this is proxied by Search results page isolated to just the relevant sales returned) AND seen at least 1 relevant SPV
-- The sale cards in scope for this test are actually found across multiple site pages (including Search results, Homepage, collection etc) but for purpose of analysis we are only looking at search results page
-- 50:50 traffic split
-- Go Live Date:  '2023-11-16'
-- There have been many iterations of the mapping for sales in scope for this test as sales have been added and removed  since this test went live. The most recent is V4
--V1:  '2023-07-12' - '2023-07-21'
--V2: '2023-07-22' - '2023-08-29'
--V3:  '2023-07-30' - '2023-09-01'
--V4: '2023-09-02' - current_date
-- This test is currently its FOURTH iteration, which is a slimmed-down version of the original test, shown on sale card as well as sale page,
-- plus showing reviews on ALL quality bands
-- Test 1 Go Live Date:'2023-07-12'
-- Test 2 (Slimmed down) Go Live Date: '2023-08-17', consider results from '2023-08-18'
-- Test 3 (Slimmed down, Best Quality) Go Live Date: '2023-09-26', consider results from '2023-09-27'
-- Test 4 (Sale Card & Sale Page, All Quality Bands) Go Live Date: '2023-11-16', consider results from '2023-11-18' due to bug on 17th

--Part 1: Identify which spv and search events are in scope - to be used later
-- Identify relevant sales in scope
WITH
	sales AS (
		SELECT
			se_sale_id,
			start_date,
			COALESCE(end_date, CURRENT_DATE) AS end_date,
			mapping_version
		FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales
	)
-- per sale_id, per mapping_version; not distinct on sale_id

-- identify where any spv happened as well as where these in-scope sales had in-scope spvs
		,
	all_spvs AS
		(
			SELECT
				spvs.event_hash,
				spvs.se_sale_id,
				spvs.touch_id,
				DATE(spvs.event_tstamp)                                AS spv_date,
				(CASE WHEN s.se_sale_id IS NOT NULL THEN 1 ELSE 0 END) AS is_in_scope_spv,
				MAX(spvs.event_tstamp)
			FROM se.data.scv_touched_spvs spvs
				LEFT JOIN sales s
						  ON spvs.se_sale_id = s.se_sale_id
							  AND DATE(spvs.event_tstamp) BETWEEN s.start_date AND s.end_date
			GROUP BY 1, 2, 3, 4, 5
		)
-- distinct on event_hash; clean

--pull out sale_ID information from the searches table, so we know what sale IDs were returned upon completion of a search
		,
	all_search_results AS
		(
			SELECT
				sts.event_hash,
				DATE(sts.event_tstamp)                  AS search_date, --,sts.touch_id
				search_results.value['saleId']::VARCHAR AS se_sale_id
			FROM se.data.scv_touched_searches sts,
				 LATERAL FLATTEN(INPUT => sts.search_context['results'], OUTER => TRUE) AS search_results
			WHERE se_sale_id IS NOT NULL
		)
-- 1 row per search event
-- NOT distinct on event_hash, because 1 event could return multiple search results and therefore multiple sale Ids/rows; will tidy this up in the next line
--HOWEVER this is not distinct on by search by sales (there are duplicate rows in there)
--I don't think this matters because of the next step? We just care about whether the search returned at least 1 in-scope search

--identify where these sales had SEARCHES; this is our proxy for sale cards that have reviews.
-- this table should isolate to just the searches that resulted in at least 1 sale IDs that are in scope; these are the searches/sale cards we want to INCLUDE for this test
		,
	in_scope_searches AS (
		SELECT DISTINCT
			event_hash,
			search_date
--,asr.se_sale_id
		FROM all_search_results asr
			INNER JOIN sales s
					   ON asr.se_sale_id = s.se_sale_id
						   AND asr.search_date BETWEEN s.start_date AND s.end_date
	)
-- this only returns search events that had at least 1 search result in scope
--originally NOT distinct on event_hash because 1 event could return multiple search results and therefore multiple sale Ids/rows
-- distinct on event_hash now that we have added the distinct function
--HOWEVER this is not distinct on by search by sales (there are duplicate rows in there)
-- I think it's ok to just say distinct event_hash here though because we just care about the events, not how many in-scope searches were made

-- Part 2: Identify bookings
		,
	transactions_cte AS
		(
			SELECT
				stt.booking_id,
				stt.touch_id
			FROM se.data.scv_touched_transactions stt --  contains live bookings as well as other booking types
				INNER JOIN se.data.fact_booking fcb
						   ON stt.booking_id = fcb.booking_id
			WHERE fcb.booking_status_type IN ('live', 'cancelled') -- include cancelled bookings because people booked these
		)
-- distinct on booking_id; clean


--Identify Feature Flags
		,
	feature_flag_selector AS (
		SELECT
			touch_id,
			feature_flag,
			COUNT(touch_id) OVER (PARTITION BY touch_id) AS rows_
		FROM se.data.scv_touched_feature_flags stff
		WHERE DATE(touch_start_tstamp) >= '2023-11-16' -- date test was first set live
		  AND feature_flag IN ('reviews', 'reviews.control')
	)
--NOT distinct on touch_id; this is because any touch can have multiple feature flags

-- identifies duplicates in feature_flags_selector
		,
	duplicate_identifier AS (
		SELECT DISTINCT
			touch_id
		FROM feature_flag_selector
		WHERE rows_ > 1
	)
-- distinct on touch_id; clean


--from all events, identify where in scope spvs and searches have happened
		,
	event_1 AS (
		SELECT
			ses.event_hash,
			ses.event_tstamp,
			DATE(ses.event_tstamp)                                                      AS event_date,
			tba.touch_id,
			tba.touch_start_tstamp,
			tba.platform                                                                AS device,
			ROW_NUMBER() OVER (PARTITION BY tba.touch_id ORDER BY ses.event_tstamp ASC) AS event_rnk_asc,     -- order all events within each touch_id
			(CASE WHEN is_spv.event_hash IS NOT NULL THEN 1 ELSE 0 END)                 AS is_an_spv,         -- all spvs, not just in-scope ones (to calculate overall CTR- main KPI)
			is_in_scope_spv,                                                                                  -- in scope SPV only
			(CASE WHEN is_search.event_hash IS NOT NULL THEN 1 ELSE 0 END)              AS is_in_scope_search -- in-scope searches
		FROM se.data_pii.scv_event_stream ses
			INNER JOIN se.data_pii.scv_session_events_link el -- clean join
					   ON el.event_hash = ses.event_hash
			INNER JOIN se.data_pii.scv_touch_basic_attributes tba
					   ON tba.touch_id = el.touch_id
			LEFT JOIN  all_spvs is_spv
					   ON is_spv.event_hash = ses.event_hash -- clean join, no events dropped
			LEFT JOIN  in_scope_searches is_search
					   ON is_search.event_hash = ses.event_hash -- clean left join (no dupes) but 300k events gained?
-- this should be a left join because we want to know where in scope searches happened for the self-join later, inner joining will not tell us this.
		WHERE DATE(ses.event_tstamp) >= '2023-11-16'
		  AND tba.stitched_identity_type = 'se_user_id'
		  AND device IN ('Web', 'Mobile Web', 'Tablet Web')
	)
-- distinct on event_hash; clean
--searches are NOT coming through as expected

--self-join table to identify where spvs have happened immediately following serach
		,
	self_join AS (
		SELECT
			e1.*,
			(CASE WHEN e1.is_in_scope_search = 1 AND e2.is_an_spv = 1 THEN 1 ELSE 0 END) AS is_spv_following_search,         --identifies where is_an_spv follows is_in_scope_search using ranking in join (where first event is an in-scope search and the second is any spv)
			(CASE
				 WHEN e1.is_in_scope_search = 1 AND e2.is_in_scope_spv = 1 THEN 1
				 ELSE 0
			 END)                                                                        AS is_in_scope_spv_following_search --identifies where is_in_scope_spv follows is_in_scopesearch using ranking in join (where first event is an in-scope search and the second is an in-scope spv)
		FROM event_1 e1
			LEFT JOIN event_1 e2
					  ON e1.touch_id = e2.touch_id
						  AND e2.event_rnk_asc = e1.event_rnk_asc +
												 1 -- joins on touch ID and also where event rank of table 2 being 1 greater than table 1
	)
-- distinct on event_hash; clean

--5: Aggregate to session level
		,
	session_level AS
		(
			SELECT
				sj.touch_id,
				sj.touch_start_tstamp,
				DATE(sj.touch_start_tstamp)              AS session_date,
				mkt.touch_affiliate_territory            AS territory,
				device,
				MAX(CASE
						WHEN ffs.feature_flag = 'reviews' THEN 'Test'
						WHEN ffs.feature_flag = 'reviews.control' THEN 'Control'
						ELSE NULL
					END)                                 AS feature_flag_control_test,
				COUNT(DISTINCT trx.booking_id)           AS bookings,
				SUM(sj.is_in_scope_search)               AS searches,                      -- in scope searches
				SUM(sj.is_an_spv)                        AS spvs,                          -- all spvs
				SUM(sj.is_in_scope_spv)                  AS spvs_in_scope,                 -- all in scope SPVS, not just those that directly follow a search
				SUM(is_spv_following_search)             AS spvs_following_search,         -- all SPVS that directly followed a search
				SUM(sj.is_in_scope_spv_following_search) AS in_scope_spvs_following_search -- in scope spvs which directly follow a search
			FROM self_join sj
				LEFT JOIN feature_flag_selector ffs
						  ON sj.touch_id = ffs.touch_id
				LEFT JOIN duplicate_identifier di
						  ON di.touch_id = sj.touch_id
				LEFT JOIN se.data.scv_touch_marketing_channel mkt
						  ON mkt.touch_id = sj.touch_id
				LEFT JOIN transactions_cte trx -- join last because this isn't at touch level
						  ON sj.touch_id = trx.touch_id
			WHERE DATE(sj.touch_start_tstamp) >= '2023-11-16' -- date of go live
			  AND di.touch_id IS NULL                         -- this should remove duplicates identified in the duplicates_identifier bit
			GROUP BY 1, 2, 3, 4, 5
		)
-- distinct on touch_id; clean (due to group by)

		,
	sessions_agg AS (
		SELECT
			session_date,
			feature_flag_control_test,
			device,
			territory,
			SUM(bookings)                       AS num_bookings,
			COUNT(touch_id)                     AS num_sessions,
			SUM(searches)                       AS num_searches,                      -- in scope searches
			SUM(spvs)                           AS num_spvs,                          -- all spvs
			SUM(spvs_in_scope)                  AS num_spvs_in_scope,                 -- in scope spvs
			SUM(spvs_following_search)          AS num_spvs_following_search,         -- all SPVS that directly follow a search
			SUM(in_scope_spvs_following_search) AS num_in_scope_spvs_following_search -- in scope spvs which directly follow a search
		FROM session_level
		GROUP BY 1, 2, 3, 4
	)

SELECT *
FROM sessions_agg

--Proposed Calculations (in Tableau):
-- Overall SPV CTR = Num SPVs (all) Following Search/Num Searches in Scope
-- In scope SPV CTR = Num SPVs in scope Following Search/Num Searches in Scope
;

USE WAREHOUSE pipe_large
;

WITH
	sales AS (
		SELECT
			se_sale_id,
			start_date,
			COALESCE(end_date, CURRENT_DATE) AS end_date,
			mapping_version
		FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales
	),
	all_search_results AS
		(
			SELECT
				sts.event_hash,
				DATE(sts.event_tstamp)                  AS search_date, --,sts.touch_id
				search_results.value['saleId']::VARCHAR AS se_sale_id
			FROM se.data.scv_touched_searches sts,
				 LATERAL FLATTEN(INPUT => sts.search_context['results'], OUTER => TRUE) AS search_results
			WHERE search_results.value['saleId']::VARCHAR IS NOT NULL
			  AND DATE(sts.event_tstamp) >= '2023-11-16'
		),
-- 1 row per search event
-- NOT distinct on event_hash, because 1 event could return multiple search results and therefore multiple sale Ids/rows; will tidy this up in the next line
--HOWEVER this is not distinct on by search by sales (there are duplicate rows in there)
--I don't think this matters because of the next step? We just care about whether the search returned at least 1 in-scope search

--identify where these sales had SEARCHES; this is our proxy for sale cards that have reviews.
-- this table should isolate to just the searches that resulted in at least 1 sale IDs that are in scope; these are the searches/sale cards we want to INCLUDE for this test
	in_scope_searches AS (
		SELECT DISTINCT
			asr.event_hash,
			asr.search_date
--,asr.se_sale_id
		FROM all_search_results asr
			INNER JOIN sales s
					   ON asr.se_sale_id = s.se_sale_id
						   AND asr.search_date BETWEEN s.start_date AND s.end_date
	)
-- this only returns search events that had at least 1 search result in scope
--originally NOT distinct on event_hash because 1 event could return multiple search results and therefore multiple sale Ids/rows
-- distinct on event_hash now that we have added the distinct function
--HOWEVER this is not distinct on by search by sales (there are duplicate rows in there)
-- I think it's ok to just say distinct event_hash here though because we just care about the events, not how many in-scope searches were made

-- Part 2: Identify bookings

		,
	event_data AS (
		SELECT
			ses.event_hash,
			ses.event_tstamp,
			DATE(ses.event_tstamp)                                                      AS event_date,
			tba.touch_id,
			tba.touch_start_tstamp,
			tba.platform                                                                AS device,
			ROW_NUMBER() OVER (PARTITION BY tba.touch_id ORDER BY ses.event_tstamp ASC) AS event_rnk_asc,     -- order all events within each touch_id
			(CASE WHEN is_search.event_hash IS NOT NULL THEN 1 ELSE 0 END)              AS is_in_scope_search -- in-scope searches
		FROM se.data_pii.scv_event_stream ses
			INNER JOIN se.data_pii.scv_session_events_link el -- clean join
					   ON el.event_hash = ses.event_hash
			INNER JOIN se.data_pii.scv_touch_basic_attributes tba
					   ON tba.touch_id = el.touch_id
			LEFT JOIN  in_scope_searches is_search
					   ON is_search.event_hash = ses.event_hash -- clean left join (no dupes) but 300k events gained?
-- this should be a left join because we want to know where in scope searches happened for the self-join later, inner joining will not tell us this.
		WHERE DATE(ses.event_tstamp) >= '2023-11-16'
		  AND tba.stitched_identity_type = 'se_user_id'
		  AND tba.platform IN ('Web', 'Mobile Web', 'Tablet Web')
	)
SELECT *
FROM event_data
WHERE event_data.is_in_scope_search > 0
;


SELECT *
FROM se.data.scv_touched_searches sts
WHERE sts.event_tstamp::DATE = '2023-12-02' AND
	  sts.event_hash = '75fcd8647199f5f8bd460062c2a5761dcc0a34d3f6b50d489495d8a02a9553cc';


SELECT *
FROM se.data_pii.scv_session_events_link sts
WHERE sts.event_tstamp::DATE = '2023-12-02' AND
	  sts.event_hash = '75fcd8647199f5f8bd460062c2a5761dcc0a34d3f6b50d489495d8a02a9553cc';


SELECT *
FROM se.data_pii.scv_event_stream sts
WHERE sts.event_tstamp::DATE = '2023-12-02' AND
	  sts.event_hash = '75fcd8647199f5f8bd460062c2a5761dcc0a34d3f6b50d489495d8a02a9553cc';

