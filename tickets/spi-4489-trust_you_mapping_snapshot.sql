SELECT *
FROM data_vault_mvp.dwh.se_trustyou_matched_properties

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_trustyou_matched_properties CLONE data_vault_mvp.dwh.se_trustyou_matched_properties
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/trustyou/se_trustyou_matched_properties_snapshot.py'  --method 'run' --start '2023-11-23 00:00:00' --end '2023-11-23 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.se_trustyou_matched_properties_snapshot
;

SHOW TABLES IN SCHEMA unload_vault_mvp.trustyou
;



MERGE INTO data_vault_mvp.dwh.se_trustyou_matched_properties_snapshot AS target
	USING (
		SELECT
			TO_DATE('2023-11-17') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231116t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-18') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231117t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-19') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231118t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-20') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231119t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-21') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231120t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-22') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231121t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-23') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231122t000000__daily
		WHERE is_matched_property
	) AS batch
	ON target.view_date = batch.view_date
		AND target.se_hotel_code = batch.se_hotel_code
	WHEN NOT MATCHED THEN INSERT VALUES (CURRENT_TIMESTAMP::TIMESTAMP, -- schedule_tstamp
										 CURRENT_TIMESTAMP::TIMESTAMP, -- run_tstamp
										 'historical_load https://github.com/secretescapes/one-data-pipeline/pull/3589', -- operation_id
										 CURRENT_TIMESTAMP()::TIMESTAMP, -- created_at
										 CURRENT_TIMESTAMP()::TIMESTAMP, -- updated_at
										 batch.view_date,
										 batch.se_hotel_code,
										 batch.trustyou_ty_id)
;

SELECT
	se_hotel_code,
	trustyou_ty_id
FROM data_vault_mvp_dev_robin.dwh.se_trustyou_matched_properties_snapshot
WHERE view_date >= '2023-11-17'


SELECT
	view_date,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.se_trustyou_matched_properties_snapshot
GROUP BY 1


USE ROLE pipelinerunner
;


MERGE INTO data_vault_mvp.dwh.se_trustyou_matched_properties_snapshot AS target
	USING (
		SELECT
			TO_DATE('2023-11-17') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231116t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-18') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231117t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-19') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231118t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-20') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231119t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-21') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231120t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-22') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231121t000000__daily
		WHERE is_matched_property
		UNION ALL
		SELECT
			TO_DATE('2023-11-23') AS view_date,
			target_id             AS se_hotel_code,
			source_id             AS trustyou_ty_id
		FROM unload_vault_mvp.trustyou.trustyou_se_matched_properties__20231122t000000__daily
		WHERE is_matched_property
	) AS batch
	ON target.view_date = batch.view_date
		AND target.se_hotel_code = batch.se_hotel_code
	WHEN NOT MATCHED THEN INSERT VALUES (CURRENT_TIMESTAMP::TIMESTAMP, -- schedule_tstamp
										 CURRENT_TIMESTAMP::TIMESTAMP, -- run_tstamp
										 'historical_load https://github.com/secretescapes/one-data-pipeline/pull/3589', -- operation_id
										 CURRENT_TIMESTAMP()::TIMESTAMP, -- created_at
										 CURRENT_TIMESTAMP()::TIMESTAMP, -- updated_at
										 batch.view_date,
										 batch.se_hotel_code,
										 batch.trustyou_ty_id)
;

SELECT
	view_date,
	COUNT(*)
FROM data_vault_mvp.dwh.se_trustyou_matched_properties_snapshot
GROUP BY 1
;

GRANT SELECT ON TABLE data_vault_mvp.dwh.se_trustyou_matched_properties_snapshot TO ROLE personal_role__dbt_prod
;

SELECT
	start_date,
	COUNT(*)
FROM dbt.bi_data_platform__intermediate.dp_reviews_02_trust_you_mapped_sales
GROUP BY 1