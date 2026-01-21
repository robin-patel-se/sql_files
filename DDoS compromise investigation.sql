SELECT
	client_ip,
	user_name,
	COUNT(*)
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
GROUP BY ALL
;

SELECT
	client_ip,
	PARSE_IP(client_ip, 'INET'),
--        user_name,
	COUNT(*)
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
GROUP BY ALL
;

-- DATASCIENCERUNNER
-- DATASCIENCEAPI


-- 6th of December is a date that was reported


SELECT *
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
;


WITH
	login_info AS (
		SELECT *
		FROM snowflake.account_usage.login_history
		WHERE user_name IN (
							'DATASCIENCERUNNER',
							'DATASCIENCEAPI'
			)
--   AND event_timestamp::DATE = '2025-12-06'
		  AND event_timestamp BETWEEN '2025-12-06 00:00:00.000000 +00:00' AND '2025-12-06 23:59:00.000000 +00:00'
	)
SELECT
	client_ip,
	COUNT(*)
FROM login_info
GROUP BY ALL
;



SELECT *
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND event_timestamp::DATE = '2025-12-06'
  AND client_ip IN (
					'34.244.179.168',
					'34.251.7.209',
					'54.216.131.52',
					'34.255.157.120',
					'54.247.215.140',
					'34.241.178.72'
	)
;



SELECT
	reported_client_type,
	reported_client_version,
	COUNT(*)
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND event_timestamp::DATE BETWEEN '2025-12-06' AND CURRENT_DATE + 1
GROUP BY ALL
;

-- these are all ghassen
SELECT *
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND event_timestamp::DATE BETWEEN '2025-12-06' AND CURRENT_DATE + 1
  AND reported_client_type = 'JDBC_DRIVER'
;


SELECT *
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND event_timestamp::DATE BETWEEN '2025-12-06' AND CURRENT_DATE + 1
  AND reported_client_type = 'JDBC_DRIVER'
  AND reported_client_version = '3.27.0'
;


-- shared a short list of all client type and client version shared with Donald for him to correlate with DS applications

SELECT
	event_timestamp::DATE AS date,
	COUNT(*)              AS logins
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND event_timestamp::DATE BETWEEN '2025-12-06' AND CURRENT_DATE + 1
;

SELECT *
FROM (
	SELECT
		event_timestamp::DATE            AS login_date,
		DATE_PART(HOUR, event_timestamp) AS login_hour,
		1                                AS login_count -- Used for summing in the pivot
	FROM snowflake.account_usage.login_history
	WHERE user_name IN ('DATASCIENCERUNNER', 'DATASCIENCEAPI')
	  AND event_timestamp::DATE BETWEEN '2025-11-01' AND CURRENT_DATE()
)
	PIVOT (
	COUNT(login_count)
	FOR login_hour IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
	)
ORDER BY login_date DESC
;



SELECT *
FROM snowflake.account_usage.login_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
--   AND event_timestamp::DATE = '2025-12-06'
  AND event_timestamp BETWEEN '2025-12-06 07:00:00.000000 +00:00' AND '2025-12-06 07:59:59.000000 +00:00'



SELECT *
FROM (
	SELECT
		start_time::DATE            AS query_date,
		DATE_PART(HOUR, start_time) AS query_hour,
		1                           AS query_count
	FROM snowflake.account_usage.query_history
	WHERE user_name IN (
						'DATASCIENCERUNNER',
						'DATASCIENCEAPI'
		)
	  AND start_time::DATE BETWEEN '2025-11-01' AND CURRENT_DATE()
)
	PIVOT (
	COUNT(query_count)
	FOR query_hour IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
	)
;

WITH
	queries_of_interest AS (
		SELECT *
		FROM snowflake.account_usage.query_history
		WHERE user_name IN (
							'DATASCIENCERUNNER',
							'DATASCIENCEAPI'
			)
		  AND start_time::DATE = '2025-12-07'
		  AND start_time BETWEEN '2025-12-07 18:00:00.000000 +00:00' AND '2025-12-07 23:59:59.000000 +00:00'
	)
SELECT
	query_type,
	COUNT(*)
FROM queries_of_interest
GROUP BY ALL

WITH
	queries_of_interest AS (
		SELECT *
		FROM snowflake.account_usage.query_history
		WHERE user_name IN (
							'DATASCIENCERUNNER',
							'DATASCIENCEAPI'
			)
		  AND start_time::DATE = '2025-11-30'
		  AND start_time BETWEEN '2025-11-30 18:00:00.000000 +00:00' AND '2025-11-30 23:59:59.000000 +00:00'
	)
SELECT
	query_type,
	COUNT(*)
FROM queries_of_interest
GROUP BY ALL
;


-- investigating the 8th of december
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-08'
  AND start_time BETWEEN '2025-12-08 14:00:00.000000 +00:00' AND '2025-12-08 17:59:59.000000 +00:00'

-- checking how 8th compares to 9th
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-09'
  AND start_time BETWEEN '2025-12-09 14:00:00.000000 +00:00' AND '2025-12-09 17:59:59.000000 +00:00'
;

-- looks like a lot of odysseus data loading


-- looking specifically at SELECT queries
SELECT *
FROM (
	SELECT
		start_time::DATE            AS query_date,
		DATE_PART(HOUR, start_time) AS query_hour,
		1                           AS query_count
	FROM snowflake.account_usage.query_history
	WHERE user_name IN (
						'DATASCIENCERUNNER',
						'DATASCIENCEAPI'
		)
	  AND query_type = 'SELECT'
	  AND start_time::DATE BETWEEN '2025-11-01' AND CURRENT_DATE()
)
	PIVOT (
	COUNT(query_count)
	FOR query_hour IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
	)
;
-- looking at 6th at 12:00PM
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-06'
  AND start_time BETWEEN '2025-12-06 12:00:00.000000 +00:00' AND '2025-12-06 12:59:59.000000 +00:00'
  AND query_type = 'SELECT'
;

-- looking at 6th at 16:00PM to 19:00PM
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-06'
  AND start_time BETWEEN '2025-12-06 16:00:00.000000 +00:00' AND '2025-12-06 18:59:59.000000 +00:00'
  AND query_type = 'SELECT'
;

-- looking at 8th at 23:00PM
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-08'
  AND start_time BETWEEN '2025-12-08 23:00:00.000000 +00:00' AND '2025-12-08 23:59:59.000000 +00:00'
  AND query_type = 'SELECT'
;

-- looking at 9th at 06:00AM
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-09'
  AND start_time BETWEEN '2025-12-09 06:00:00.000000 +00:00' AND '2025-12-09 06:59:59.000000 +00:00'
  AND query_type = 'SELECT'
;

-- looking at 9th at 09:00AM
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-09'
  AND start_time BETWEEN '2025-12-09 09:00:00.000000 +00:00' AND '2025-12-09 09:59:59.000000 +00:00'
  AND query_type = 'SELECT'
;

-- looking at 9th at 17:00PM
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-09'
  AND start_time BETWEEN '2025-12-09 17:00:00.000000 +00:00' AND '2025-12-09 17:59:59.000000 +00:00'
  AND query_type = 'SELECT'
;

-- looking at 11th at 13:00PM
SELECT *
FROM snowflake.account_usage.query_history
WHERE user_name IN (
					'DATASCIENCERUNNER',
					'DATASCIENCEAPI'
	)
  AND start_time::DATE = '2025-12-11'
  AND start_time BETWEEN '2025-12-11 13:00:00.000000 +00:00' AND '2025-12-11 13:59:59.000000 +00:00'
  AND query_type = 'SELECT'
;

-- choosing an abritary date where we believe we were not compromised (23rd of November
-- look at tables that were referenced in the sql
-- looking only at SELECT query types
WITH
	table_refs AS (
		SELECT
			query_id,
			query_text,
			-- This regex looks for patterns after FROM/JOIN and handles schema.table formats
			REGEXP_SUBSTR_ALL(LOWER(query_text), '(from|join)\\s+([\\w\\.]+)', 1, 1, 'e', 2) AS referenced_objects
		FROM snowflake.account_usage.query_history
		WHERE user_name IN (
							'DATASCIENCERUNNER',
							'DATASCIENCEAPI'
			)
		  AND start_time::DATE = '2025-11-23'
		  AND query_type = 'SELECT'
	),
	flatten_objects AS (
		SELECT
			query_id,
			query_text,
			objects.value::VARCHAR AS referenced_object
		FROM table_refs,
			 LATERAL FLATTEN(INPUT => table_refs.referenced_objects, OUTER => TRUE) objects
	)
SELECT
	flatten_objects.referenced_object,
	COUNT(*)
FROM flatten_objects
WHERE flatten_objects.referenced_object LIKE '%.%' -- to remove CTE refs
GROUP BY ALL
;

-- rerunning same query on the 7th of december
WITH
	table_refs AS (
		SELECT
			query_id,
			query_text,
			-- This regex looks for patterns after FROM/JOIN and handles schema.table formats
			REGEXP_SUBSTR_ALL(LOWER(query_text), '(from|join)\\s+([\\w\\.]+)', 1, 1, 'e', 2) AS referenced_objects
		FROM snowflake.account_usage.query_history
		WHERE user_name IN (
							'DATASCIENCERUNNER',
							'DATASCIENCEAPI'
			)
		  AND start_time::DATE = '2025-12-07'
		  AND query_type = 'SELECT'
	),
	flatten_objects AS (
		SELECT
			query_id,
			query_text,
			objects.value::VARCHAR AS referenced_object
		FROM table_refs,
			 LATERAL FLATTEN(INPUT => table_refs.referenced_objects, OUTER => TRUE) objects
	)
SELECT
	flatten_objects.referenced_object,
	COUNT(*)
FROM flatten_objects
WHERE flatten_objects.referenced_object LIKE '%.%' -- to remove CTE refs
GROUP BY ALL
;


-- rerunning same query from 6th to 22nd
WITH
	table_refs AS (
		SELECT
			query_id,
			query_text,
			-- This regex looks for patterns after FROM/JOIN and handles schema.table formats
			REGEXP_SUBSTR_ALL(LOWER(query_text), '(from|join)\\s+([\\w\\.]+)', 1, 1, 'e', 2) AS referenced_objects
		FROM snowflake.account_usage.query_history
		WHERE user_name IN (
							'DATASCIENCERUNNER',
							'DATASCIENCEAPI'
			)
		  AND start_time::DATE BETWEEN '2025-12-06' AND '2025-12-22'
		  AND query_type = 'SELECT'
	),
	flatten_objects AS (
		SELECT
			query_id,
			query_text,
			objects.value::VARCHAR AS referenced_object
		FROM table_refs,
			 LATERAL FLATTEN(INPUT => table_refs.referenced_objects, OUTER => TRUE) objects
	)
SELECT
	flatten_objects.referenced_object,
	COUNT(*)
FROM flatten_objects
WHERE flatten_objects.referenced_object LIKE '%.%' -- to remove CTE refs
GROUP BY ALL
;

-- the only object reference that is interesting is a call to se.data.fact_complete_booking

WITH
	table_refs AS (
		SELECT
			query_id,
			query_text,
			-- This regex looks for patterns after FROM/JOIN and handles schema.table formats
			REGEXP_SUBSTR_ALL(LOWER(query_text), '(from|join)\\s+([\\w\\.]+)', 1, 1, 'e', 2) AS referenced_objects
		FROM snowflake.account_usage.query_history
		WHERE user_name IN (
							'DATASCIENCERUNNER',
							'DATASCIENCEAPI'
			)
		  AND start_time::DATE BETWEEN '2025-12-06' AND '2025-12-22'
		  AND query_type = 'SELECT'
	),
	flatten_objects AS (
		SELECT
			query_id,
			query_text,
			objects.value::VARCHAR AS referenced_object
		FROM table_refs,
			 LATERAL FLATTEN(INPUT => table_refs.referenced_objects, OUTER => TRUE) objects
	)
SELECT *
FROM flatten_objects
WHERE flatten_objects.referenced_object = 'se.data.fact_complete_booking'

-- All 6 of these look like they are new development to calculate trip duration