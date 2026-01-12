SELECT
	query_id,
	query_start_time,
	user_name,
	direct_objects_accessed,
	base_objects_accessed,
	objects_modified,
	object_modified_by_ddl,
	policies_referenced
FROM snowflake.account_usage.access_history ah;

USE ROLE accountadmin;

SHOW warehouses;
SELECT * FROM table(result_scan(last_query_id()))