USE WAREHOUSE pipe_xlarge
;

SELECT
	contexts_com_secretescapes_user_context_1[0]['unique_browser_id']::VARCHAR                                 AS unique_browser_id,
	event_stream.user_id,
	IFF(event_stream.user_id IS NOT NULL, TRUE, FALSE)                                                         AS inferred_login,
	event_stream.collector_tstamp,
	event_stream.contexts_com_secretescapes_user_state_context_1[0]['feature_flags']                           AS feature_flags,
	CASE
		WHEN ARRAY_CONTAINS('abtest.price.elasticity.variant'::VARIANT, feature_flags) THEN 'variant'
		WHEN ARRAY_CONTAINS('abtest.price.elasticity.control'::VARIANT, feature_flags) THEN 'control'
	END                                                                                                        AS test_group,
	event_stream.page_url,
	LOWER(contexts_com_secretescapes_all_pages_session_login_type_context_1[0]['session_login_type']::VARCHAR) AS login_type,
	*
FROM snowplow.atomic.events event_stream
WHERE event_stream.collector_tstamp::DATE = '2025-10-31'
-- AND user_id = '77895526'
  AND unique_browser_id = '1b10e94c-f2a5-47aa-abad-dc8648e752cc'
ORDER BY event_stream.collector_tstamp DESC
;



SELECT
	contexts_com_secretescapes_user_context_1[0]['unique_browser_id']::VARCHAR       AS unique_browser_id,
	event_stream.se_user_id,
	IFF(event_stream.se_user_id IS NOT NULL, TRUE, FALSE)                            AS inferred_login,
	module_touchification.attributed_user_id,
	module_touchification.touch_id,
	event_stream.event_tstamp,
	event_stream.contexts_com_secretescapes_user_state_context_1[0]['feature_flags'] AS feature_flags,
	CASE
		WHEN ARRAY_CONTAINS('abtest.price.elasticity.variant'::VARIANT, feature_flags) THEN 'variant'
		WHEN ARRAY_CONTAINS('abtest.price.elasticity.control'::VARIANT, feature_flags) THEN 'control'
	END                                                                              AS test_group,
	event_stream.page_url,
	event_stream.login_type
FROM hygiene_vault_mvp.snowplow.event_stream
INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification
	ON event_stream.event_hash = module_touchification.event_hash
	AND module_touchification.event_tstamp::DATE = '2025-10-31'
WHERE event_stream.event_tstamp::DATE = '2025-10-31'
-- AND user_id = '77895526'
  AND event_stream.unique_browser_id = '1b10e94c-f2a5-47aa-abad-dc8648e752cc'
ORDER BY event_stream.event_tstamp DESC
;

-- touch id for events: 906c3c1ae49540a6f31918cc1b689e481fdeb8d7a80230b9ee95750a28f76be7

SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE stff.touch_id = '906c3c1ae49540a6f31918cc1b689e481fdeb8d7a80230b9ee95750a28f76be7'
  AND stff.touch_start_tstamp::DATE = '2025-10-31'
  AND stff.feature_flag LIKE 'abtest.price.elasticity.%'