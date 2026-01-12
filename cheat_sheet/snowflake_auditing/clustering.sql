USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_medium

USE DATABASE data_vault_mvp
;

USE SCHEMA single_customer_view_stg
;

SELECT
	SYSTEM$CLUSTERING_INFORMATION('data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes',
								  '(touch_start_tstamp::date, touch_posa_territory, touch_experience)')
;

{
  "cluster_by_keys" : "LINEAR(touch_start_tstamp::date, touch_posa_territory, touch_experience)",
  "total_partition_count" : 10144,
  "total_constant_partition_count" : 0,
  "average_overlaps" : 10143.0,
  "average_depth" : 10144.0,
  "partition_depth_histogram" : {
    "00000" : 0,
    "00001" : 0,
    "00002" : 0,
    "00003" : 0,
    "00004" : 0,
    "00005" : 0,
    "00006" : 0,
    "00007" : 0,
    "00008" : 0,
    "00009" : 0,
    "00010" : 0,
    "00011" : 0,
    "00012" : 0,
    "00013" : 0,
    "00014" : 0,
    "00015" : 0,
    "00016" : 0,
    "16384" : 10144
  },
  "clustering_errors" : [ ]
};

    USE ROLE pipelinerunner

SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 30 AND qh.bytes_spilled_to_remote_storage > 0


