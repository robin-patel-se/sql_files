-- WAREHOUSING PART:
-- -—we could check which VWHs have not been used

USE ROLE accountadmin
;

USE somedb.someschema
;

SHOW WAREHOUSES IN ACCOUNT
;

CREATE TEMPORARY TABLE warehouses_table AS
SELECT
	"name"
FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))
;

SELECT
	"name",
	COALESCE(last_used::string, 'not been used for 30 days')
FROM warehouses_table wt
	LEFT JOIN
(
	SELECT
		MAX(start_time) AS last_used,
		warehouse_name
	FROM snowflake.account_usage.warehouse_metering_history
	WHERE warehouse_name <> 'CLOUD_SERVICES_ONLY'
	  AND start_time::date > CURRENT_DATE - 30
	GROUP BY 2
) sub
ON wt."name" = sub.warehouse_name
;

-- IGH CLOUD SERVICES USAGE % on pipe_small

SELECT
	SUBSTR(query_text, 1, 50),
	credits_used_cloud_services
FROM snowflake.account_usage.query_history
WHERE warehouse_name = 'COMPUTE_WH'
  AND start_time::date >= CURRENT_DATE - 30
  AND credits_used_cloud_services > 0
ORDER BY credits_used_cloud_services DESC
;

-- Discussion on the case where we want to track costs inside a warehouse.


-- Estimates the utilization % from usage
-- Calculates the execution time of all queries
-- Compares this to actual billable hours (Credits / nodes)
--
-- Assumption:  The warehouse size is static for the period.
--Step 1
USE ROLE accountadmin
;

SHOW WAREHOUSES
;

--Step 2 Modify to whatever database.schema desired
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.t_warehouse_list AS
SELECT *
FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))
;

SELECT *
FROM scratch.robinpatel.t_warehouse_list
;
--Step 3
WITH
	usage AS (
		SELECT
			u.warehouse_id,
			u.warehouse_name,
			MAX(cluster_number)                   AS max_clusters_running,
			SUM(total_elapsed_time) / 1000 / 3600 AS elapsed_query_hours,
			COUNT(u.query_id)                     AS query_counts,
			CASE
				WHEN wh."size" = 'X-Small' THEN 1
				WHEN wh."size" = 'Small' THEN 2
				WHEN wh."size" = 'Medium' THEN 4
				WHEN wh."size" = 'Large' THEN 8
				WHEN wh."size" = 'X-Large' THEN 16
				WHEN wh."size" = '2X-Large' THEN 32
				WHEN wh."size" = '3X-Large' THEN 64
				WHEN wh."size" = '4X-Large' THEN 128
			END                                   AS vwh_node_count,
			wh."size"                             AS wh_size,
			wh."auto_suspend"                     AS auto_suspend,
			(
				SELECT
					SUM(credits_used) --change to credits_used_compute??
				FROM snowflake.account_usage.warehouse_metering_history wmh
				WHERE wmh.warehouse_id = u.warehouse_id
				  AND wmh.start_time > DATEADD(DAY, -31, CURRENT_DATE())
			)                                     AS credits_used
		FROM snowflake.account_usage.query_history u
			JOIN scratch.robinpatel.t_warehouse_list wh
				 ON wh."name" = u.warehouse_name
		WHERE u.start_time > DATEADD(DAY, -31, CURRENT_DATE())
		  AND (u.bytes_scanned > 0 OR u.bytes_written > 0) --exclude metadata and cache queries
		GROUP BY u.warehouse_id,
				 u.warehouse_name,
				 CASE
					 WHEN wh."size" = 'X-Small' THEN 1
					 WHEN wh."size" = 'Small' THEN 2
					 WHEN wh."size" = 'Medium' THEN 4
					 WHEN wh."size" = 'Large' THEN 8
					 WHEN wh."size" = 'X-Large' THEN 16
					 WHEN wh."size" = '2X-Large' THEN 32
					 WHEN wh."size" = '3X-Large' THEN 64
					 WHEN wh."size" = '4X-Large' THEN 128
				 END
				, wh."size"
				, wh."auto_suspend"
	)
SELECT
	usage.warehouse_name,
	usage.query_counts,
	ROUND(usage.credits_used, 2)                                     AS credits_used,
	usage.wh_size,
	usage.vwh_node_count,
	max_clusters_running,
	ROUND(usage.elapsed_query_hours, 2)                              AS elapsed_query_hours,
	ROUND(usage.credits_used / usage.vwh_node_count, 2)              AS elapsed_billed_hours,
	ROUND(usage.elapsed_query_hours / elapsed_billed_hours * 100, 1) AS utilisation_pct,
	usage.auto_suspend
--     , usage.min_cluster_count
--     , usage.max_cluster_count
--     , usage.scaling_policy
FROM usage
WHERE elapsed_billed_hours > 0 --exclude those whs with no credits to avoid divide by zero
ORDER BY credits_used DESC NULLS LAST
;


---sizing of warehouses

-- Size Virtual Warehouse--
-- This query returns for every Virtual Warehouse
-- o The current size of the warehouse (XSMALL to X4LARGE)
-- o The percentage of large queries (over 1Gb scanned)
-- o Percentage of small queries (under 1Gb scanned)
-- o The average size of large queries (Gb's returned by queries over 1Gb in size)
-- o The average query time (seconds) for large queries
-- o Total count of queries
--
-- The purpose is to provide an indication of the workload by warehouse
--
-- It should be used to detect:
-- o Large queries (>1Gb) being executed on relatively small (XSMALL, SMALL or MEDIUM) warehouses
-- o Small queries (<1Gb) being executed on relatively large (LARGE, XLARGE to X4LARGE) warehouses
--


WITH
	credits AS (
		SELECT
			wmh.warehouse_name
				,
			SUM(credits_used) AS credits_used
		FROM snowflake.account_usage.warehouse_metering_history wmh
		WHERE wmh.start_time > DATEADD(MONTH, -1, CURRENT_DATE())
		GROUP BY wmh.warehouse_name
	),
	queries AS (
		SELECT -- Queries over 1Gb in size
			   qu.warehouse_name,
			   warehouse_size,
			   AVG(CASE WHEN bytes_scanned >= 1000000000 THEN bytes_scanned ELSE NULL END) AS avg_large,
			   COUNT(CASE WHEN bytes_scanned >= 1000000000 THEN 1 ELSE NULL END)           AS count_large,
			   COUNT(CASE WHEN bytes_scanned < 1000000000 THEN 1 ELSE NULL END)            AS count_small,
			   AVG(CASE
					   WHEN bytes_scanned >= 1000000000 THEN total_elapsed_time / 1000
					   ELSE NULL
				   END)                                                                    AS avg_large_exe_time,
			   AVG(bytes_scanned)                                                          AS avg_bytes_scanned,
			   AVG(total_elapsed_time) / 1000                                              AS avg_elapsed_time,
			   AVG(execution_time) / 1000                                                  AS avg_execution_time,
			   COUNT(*)                                                                    AS count_queries
		FROM snowflake.account_usage.query_history qu
		WHERE execution_status = 'SUCCESS'
		  AND warehouse_size IS NOT NULL
		  AND end_time > DATEADD(MONTH, -1, CURRENT_DATE())
		  AND bytes_scanned > 0
		GROUP BY qu.warehouse_name,
				 warehouse_size
	)
SELECT
	q.warehouse_name, -- Warehouse Name
	q.warehouse_size,
	ROUND(count_large / count_queries * 100, 0) AS percent_large,
	ROUND(count_small / count_queries * 100, 0) AS percent_small,
	CASE
		WHEN avg_large >= POWER(2, 40) THEN TO_CHAR(ROUND(avg_large / POWER(2, 40), 1)) || ' TB'
		WHEN avg_large >= POWER(2, 30) THEN TO_CHAR(ROUND(avg_large / POWER(2, 30), 1)) || ' GB'
		WHEN avg_large >= POWER(2, 20) THEN TO_CHAR(ROUND(avg_large / POWER(2, 20), 1)) || ' MB'
		WHEN avg_large >= POWER(2, 10) THEN TO_CHAR(ROUND(avg_large / POWER(2, 10), 1)) || ' K'
		ELSE TO_CHAR(avg_large)
	END                                         AS avg_bytes_large,
	ROUND(avg_large_exe_time)                   AS avg_large_exe_time,
	ROUND(avg_execution_time)                   AS avg_all_exe_time,
	count_queries,
	ROUND(c.credits_used)                       AS credits_used
FROM queries q,
	 credits c
WHERE q.warehouse_name = c.warehouse_name
ORDER BY c.credits_used DESC,
		 CASE warehouse_size
			 WHEN 'X-Small' THEN 1
			 WHEN 'Small' THEN 2
			 WHEN 'Medium' THEN 3
			 WHEN 'Large' THEN 4
			 WHEN 'X-Large' THEN 5
			 WHEN '2X-Large' THEN 6
			 WHEN '3X-Large' THEN 7
			 WHEN '4X-Large' THEN 8
			 ELSE 9
		 END DESC
;
