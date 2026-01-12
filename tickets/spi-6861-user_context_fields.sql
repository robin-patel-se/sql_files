USE WAREHOUSE pipe_xlarge
;


WITH
	input_data AS (
		SELECT
			es.event_hash,
			es.event_tstamp,
			es.contexts_com_secretescapes_user_context_1
-- 			es.unstruct_event_com_secretescapes_user_context_1
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.event_tstamp >= '2024-01-01' -- todo change this to 30 days
		  AND es.event_name = 'page_view'
		  AND es.is_server_side_event
		  AND es.se_brand = 'SE Brand'
	),
	flatten AS (
		SELECT
			ind.*,
			keys.key,
			keys.value
		FROM input_data ind,
			 LATERAL FLATTEN(ind.contexts_com_secretescapes_user_context_1[0], OUTER => TRUE) keys
	)
SELECT
	f.key,
	ANY_VALUE(f.value),
	COUNT(*) AS keys
FROM flatten f
GROUP BY 1
;



WITH
	input_data AS (
		SELECT
			es.event_hash,
			es.event_tstamp,
			es.contexts_com_secretescapes_environment_context_1
-- 			es.unstruct_event_com_secretescapes_user_context_1
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.event_tstamp >= '2024-01-01' -- todo change this to 30 days
		  AND es.event_name = 'page_view'
		  AND es.is_server_side_event
		  AND es.se_brand = 'SE Brand'
	),
	flatten AS (
		SELECT
			ind.*,
			keys.key,
			keys.value
		FROM input_data ind,
			 LATERAL FLATTEN(ind.contexts_com_secretescapes_environment_context_1[0], OUTER => TRUE) keys
	)
SELECT
	f.key,
	ANY_VALUE(f.value),
	COUNT(*) AS keys
FROM flatten f
GROUP BY 1
;


/*
 KEY	ANY_VALUE(F.VALUE)	KEYS
tracking_platform	"""server-side"""	265389587
device_platform	"""MOBILE_WEB"""	265389587
environment	"""prod"""	265389587
affiliate	"""Secret Escapes DE"""	265389587

 */


WITH
	input_data AS (
		SELECT
			es.event_hash,
			es.event_tstamp,
			es.contexts_com_secretescapes_content_context_1
-- 			es.unstruct_event_com_secretescapes_user_context_1
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.event_tstamp >= '2024-01-01' -- todo change this to 30 days
		  AND es.event_name = 'page_view'
		  AND es.is_server_side_event
		  AND es.se_brand = 'SE Brand'
	),
	flatten AS (
		SELECT
			ind.*,
			keys.key,
			keys.value
		FROM input_data ind,
			 LATERAL FLATTEN(ind.contexts_com_secretescapes_content_context_1[0], OUTER => TRUE) keys
	)
-- SELECT *
-- FROM flatten
-- WHERE flatten.key IN (
-- 					  'x_referrer_url',
-- 					  'x_client_url',
-- 					  'org_client_url',
-- 					  'arg_client_url',
-- 					  'arg_referrer_url'
-- 	)

SELECT
	f.key,
	ANY_VALUE(f.value),
	COUNT(*) AS keys
FROM flatten f
GROUP BY 1
;


/*


 */



 WITH
	input_data AS (
		SELECT
			es.event_hash,
			es.event_tstamp,
			es.contexts_com_secretescapes_secret_escapes_sale_context_1
-- 			es.unstruct_event_com_secretescapes_user_context_1
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.event_tstamp >= '2024-01-01' -- todo change this to 30 days
		  AND es.event_name = 'page_view'
		  AND es.is_server_side_event
		  AND es.se_brand = 'SE Brand'
	),
	flatten AS (
		SELECT
			ind.*,
			keys.key,
			keys.value
		FROM input_data ind,
			 LATERAL FLATTEN(ind.contexts_com_secretescapes_secret_escapes_sale_context_1[0], OUTER => TRUE) keys
	)
SELECT
	f.key,
	ANY_VALUE(f.value),
	COUNT(*) AS keys
FROM flatten f
GROUP BY 1
;


 WITH
	input_data AS (
		SELECT
			es.event_hash,
			es.event_tstamp,
			es.contexts_com_secretescapes_product_display_context_1
-- 			es.unstruct_event_com_secretescapes_user_context_1
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.event_tstamp >= '2024-01-01' -- todo change this to 30 days
		  AND es.event_name = 'page_view'
		  AND es.is_server_side_event
		  AND es.se_brand = 'SE Brand'
	),
	flatten AS (
		SELECT
			ind.*,
			keys.key,
			keys.value
		FROM input_data ind,
			 LATERAL FLATTEN(ind.contexts_com_secretescapes_product_display_context_1[0], OUTER => TRUE) keys
	)
SELECT
	f.key,
	ANY_VALUE(f.value),
	COUNT(*) AS keys
FROM flatten f
GROUP BY 1
;