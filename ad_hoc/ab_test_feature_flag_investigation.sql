WITH
	user_feature_flags AS (
		SELECT DISTINCT
			ses.se_user_id,
			ff.value::VARCHAR AS presearch_flag
		FROM se.data_pii.scv_event_stream ses,
			 LATERAL FLATTEN(INPUT => contexts_com_secretescapes_user_state_context_1[0]:feature_flags, OUTER =>
							 TRUE) ff
		WHERE ses.contexts_com_secretescapes_user_state_context_1[0]['feature_flags']::VARCHAR IS NOT NULL
		  AND ses.event_tstamp >= '2023-01-24'
		  AND ff.value LIKE 'abtest.presearchtoggle%'
		  AND ses.se_user_id IS NOT NULL
	),
	flag_agg AS (
		SELECT
			uff.se_user_id,
			COUNT(DISTINCT uff.presearch_flag) AS num_flags
		FROM user_feature_flags uff
		GROUP BY 1
	)
SELECT
	fa.num_flags,
	COUNT(DISTINCT fa.se_user_id) AS users
FROM flag_agg fa
GROUP BY 1
;


-- 1 day logged in users
-- 168829 -- users have 1 flag
-- 19 -- have 2 flags

-- 10 days logged in users
-- 683319 -- users have 1 flag
-- 488 -- have 2 flags


WITH
	user_flag AS (
		SELECT DISTINCT
			ses.se_user_id,
			ff.value::VARCHAR AS presearch_flag
		FROM se.data_pii.scv_event_stream ses,
			 LATERAL FLATTEN(INPUT => contexts_com_secretescapes_user_state_context_1[0]:feature_flags, OUTER =>
							 TRUE) ff
		WHERE ses.contexts_com_secretescapes_user_state_context_1[0]['feature_flags']::VARCHAR IS NOT NULL
		  AND ses.event_tstamp >= '2024-02-01'
		  AND ff.value LIKE 'data.test%'
		  AND ses.se_user_id IS NOT NULL
	)
SELECT
	uf.presearch_flag,
	COUNT(*)
FROM user_flag uf
GROUP BY 1
;


WITH
	user_flag AS (
		SELECT DISTINCT
			ses.se_user_id,
			ff.value::VARCHAR AS presearch_flag
		FROM se.data_pii.scv_event_stream ses,
			 LATERAL FLATTEN(INPUT => contexts_com_secretescapes_user_state_context_1[0]:feature_flags, OUTER =>
							 TRUE) ff
		WHERE ses.contexts_com_secretescapes_user_state_context_1[0]['feature_flags']::VARCHAR IS NOT NULL
		  AND ses.event_tstamp BETWEEN '2024-01-24' AND '2024-01-31'
		  AND ff.value LIKE 'abtest.presearchtoggle%'
		  AND ses.se_user_id IS NOT NULL
	)
SELECT
	uf.presearch_flag,
	COUNT(*)
FROM user_flag uf
GROUP BY 1
;


WITH
	user_flag AS (
		SELECT DISTINCT
			ses.se_user_id,
			ff.value::VARCHAR AS presearch_flag
		FROM se.data_pii.scv_event_stream ses,
			 LATERAL FLATTEN(INPUT => contexts_com_secretescapes_user_state_context_1[0]:feature_flags, OUTER =>
							 TRUE) ff
		WHERE ses.contexts_com_secretescapes_user_state_context_1[0]['feature_flags']::VARCHAR IS NOT NULL
		  AND ses.event_tstamp >= '2024-01-24'
		  AND ff.value LIKE 'abtest.presearchtoggle%'
		  AND ses.se_user_id IS NOT NULL
	)
SELECT
	uf.presearch_flag,
	COUNT(*)
FROM user_flag uf
GROUP BY 1
;

USE WAREHOUSE pipe_xlarge
;

WITH
	presearch_flags AS (
-- get presearch toggle flags at event level
		SELECT
			ses.event_hash,
			ses.se_user_id,
			ff.value::VARCHAR AS presearch_flag,
			ff.index          AS flag_index
		FROM se.data_pii.scv_event_stream ses,
			 LATERAL FLATTEN(INPUT => contexts_com_secretescapes_user_state_context_1[0]:feature_flags, OUTER =>
							 TRUE) ff
		WHERE ses.contexts_com_secretescapes_user_state_context_1[0]['feature_flags']::VARCHAR IS NOT NULL
		  AND ses.event_tstamp >= CURRENT_DATE - 30
		  AND ff.value LIKE 'abtest.presearchtoggle%'
-- 		  AND ses.se_user_id IS NOT NULL
	),
	touch_id_for_flags AS (
		SELECT
			ssel.touch_id,
			pf.event_hash,
			pf.se_user_id,
			pf.presearch_flag,
			pf.flag_index
		FROM presearch_flags pf
			INNER JOIN se.data_pii.scv_session_events_link ssel ON pf.event_hash = ssel.event_hash
	)
SELECT DISTINCT
	touch_id,
	COUNT(*) OVER (PARTITION BY tiff.touch_id)                                                  AS num_flags,
	FIRST_VALUE(tiff.presearch_flag) OVER (PARTITION BY tiff.touch_id ORDER BY tiff.flag_index) AS first_presearch_flag,
	LAST_VALUE(tiff.presearch_flag) OVER (PARTITION BY tiff.touch_id ORDER BY tiff.flag_index)  AS last_presearch_flag
FROM touch_id_for_flags tiff
;


SELECT *
FROM se.data.scv_touch_basic_attributes stba
	LEFT JOIN se.data.scv_touched_feature_flags stff ON stba.touch_id = stff.touch_id
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 30
  AND stba.touch_se_brand = 'SE Brand'
;

SELECT *
FROM se.data.se_sale_tags_snapshot ssts
;

