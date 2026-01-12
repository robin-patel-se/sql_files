-- Operation always fails once in HYGIENE_VAULT because the numbers don't match.
-- Operation then succeeds on the next retry because there's nothing to anonymise.

-- BATCH RESULT:
-- Task Instance: AnonymisationOperation__incoming__cms_mysql__shiro_user at 2025-08-19, 00:30:00

-- hygiene_vault: batch_rows=16283 != (anonymised_rows=5595 + skipped_rows=18939)

-- result: 16,283 (total rows in batch to be anonymised)
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250819t003000__1755705517
;

-- result: 10,688
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250819t003000__1755705517
WHERE status = 'SKIPPED'
;

-- result: 5,595
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250819t003000__1755705517
WHERE status = 'IN_PROGRESS'
;

-- result: 5,595 (matches IN_PROGRESS records in batch)
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user
WHERE anonymised = TRUE
  AND anonymisation_metadata:status = 'SUCCESS'
  AND anonymisation_metadata:schedule_tstamp = '2025-08-19 00:30:00'
  AND anonymisation_metadata:run_tstamp = '2025-08-20 15:52:55'
;

-- result: 18,939 (does not match SKIPPED records in batch)
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user
WHERE anonymised = TRUE
  AND anonymisation_metadata:status = 'SKIPPED'
  AND anonymisation_metadata:schedule_tstamp = '2025-08-19 00:30:00'
  AND anonymisation_metadata:run_tstamp = '2025-08-20 15:52:55'
;


-- SKIPPED records were processed with:
UPDATE hygiene_vault.cms_mysql.shiro_user target
SET target.anonymised             = TRUE,
	target.anonymisation_metadata = OBJECT_CONSTRUCT(
		-- (lineage) metadata of the current job
			'schedule_tstamp', '2025-08-19 00:30:00',
			'run_tstamp', '2025-08-20 15:52:55',
			'operation_id', 'AnonymisationOperator__incoming__cms_mysql__shiro_user__20250819T003000__daily_at_00h30',
			'created_at', CURRENT_TIMESTAMP()::TIMESTAMP,
			'updated_at', CURRENT_TIMESTAMP()::TIMESTAMP,

		-- anonymisation_batch_result
			'anonymised_by', 'source-system',
			'status', batch.status,
			'pii_columns', ARRAY_CONSTRUCT(
					'username'
						   )
									)
FROM hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250819t003000 batch
WHERE batch.status = 'SKIPPED'
  AND target.id = batch.id
  AND target.row_dataset_name = batch.row_dataset_name
  AND target.row_dataset_source = batch.row_dataset_source
  AND target.row_loaded_at = batch.row_loaded_at
  AND target.row_schedule_tstamp = batch.row_schedule_tstamp
  AND target.row_run_tstamp = batch.row_run_tstamp
  AND target.row_filename = batch.row_filename
  AND target.row_file_row_number = batch.row_file_row_number
;


------------------------------------------------------------------------------------------------------------------------

-- We get the source user IDs from:

-- result: 53,211,154
SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.user_attributes
WHERE membership_account_status = 'DELETED'
;

-- We deduplicate the IDs

-- result: 53,211,154
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__deduplicate_anonymisation_list__shiro_user__20250819t003000__1755705517
;


-- The anonymisation batch is then created with:

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.cms_mysql.anonymisation_batch__shiro_user__20250819t003000
AS
SELECT
-- (lineage) metadata of the original row
target.row_dataset_name,
target.row_dataset_source,
target.row_loaded_at,
target.row_schedule_tstamp,
target.row_run_tstamp,
target.row_filename,
target.row_file_row_number,

-- identifier
batch.id,

-- pii columns
target.username

FROM hygiene_vault.cms_mysql.shiro_user target
INNER JOIN hygiene_vault.cms_mysql.deduplicate_anonymisation_list__shiro_user__20250819t003000 batch
	ON batch.id = target.id
	AND target.anonymised IS DISTINCT FROM TRUE
;

-- result: 16,283
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__anonymisation_batch__shiro_user__20250819t003000__1755705517
;

-- We then apply validation with:

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250819t003000
AS
SELECT
-- (lineage) metadata of the original row
batch.row_dataset_name,
batch.row_dataset_source,
batch.row_loaded_at,
batch.row_schedule_tstamp,
batch.row_run_tstamp,
batch.row_filename,
batch.row_file_row_number,

-- identifier
batch.id,

-- pii columns
batch.username,

-- validation checks
IFF(LOWER(username) RLIKE '^[a-f0-9]{64}$', 1, NULL)::INT AS fails_validation__username__looks_like_sha256,
IFF(LOWER(username) RLIKE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.*', 1,
	NULL)::INT                                            AS fails_validation__username__anonymised_at_source,
IFF(LOWER(username) RLIKE '[a-f0-9]{40,}@email\.escapes\.tech', 1,
	NULL)::INT                                            AS fails_validation__username__anonymised_at_source_legacy,
IFF(fails_validation__username__looks_like_sha256 = 1
		OR fails_validation__username__anonymised_at_source = 1
		OR fails_validation__username__anonymised_at_source_legacy = 1,
	1,
	NULL
)                                                         AS failed_some_validation,

-- anonymisation_batch_result
CASE
	WHEN failed_some_validation = 1
		THEN 'SKIPPED'
	ELSE 'IN_PROGRESS'
END                                                       AS status

FROM hygiene_vault.cms_mysql.anonymisation_batch__shiro_user__20250819t003000 batch
;

-- result: 16,283
SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250819t003000__1755705517
;


------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;

-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000
-- AS
SELECT
	-- (lineage) metadata of the original row
	batch.row_dataset_name,
	batch.row_dataset_source,
	batch.row_loaded_at,
	batch.row_schedule_tstamp,
	batch.row_run_tstamp,
	batch.row_filename,
	batch.row_file_row_number,

	-- identifier
	batch.id,

	-- pii columns
	batch.username,

	-- validation checks
	IFF(LOWER(username) RLIKE '^[a-f0-9]{64}$', 1, NULL)::INT AS fails_validation__username__looks_like_sha256,
	IFF(LOWER(username) RLIKE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.*', 1,
		NULL)::INT                                            AS fails_validation__username__anonymised_at_source,
	IFF(LOWER(username) RLIKE '[a-f0-9]{40,}@email\.escapes\.tech', 1,
		NULL)::INT                                            AS fails_validation__username__anonymised_at_source_legacy,
	IFF(fails_validation__username__looks_like_sha256 = 1
			OR fails_validation__username__anonymised_at_source = 1
			OR fails_validation__username__anonymised_at_source_legacy = 1,
		1,
		NULL
	)                                                         AS failed_some_validation,

	-- anonymisation_batch_result
	CASE
		WHEN failed_some_validation = 1
			THEN 'SKIPPED'
		ELSE 'IN_PROGRESS'
	END                                                       AS status

FROM hygiene_vault.cms_mysql.shiro_user__anonymisation_batch__shiro_user__20250820t003000__1755737212 batch FROM hygiene_vault.cms_mysql.shiro_user__anonymisation_batch__shiro_user__20250820T003000__1755737212 batch

SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__anonymisation_batch__shiro_user__20250820t003000__1755737212
;


/*
            UPDATE hygiene_vault.cms_mysql.shiro_user target
            SET
                target.anonymised = TRUE,
                target.anonymisation_metadata = OBJECT_CONSTRUCT(
                -- (lineage) metadata of the current job
                    'schedule_tstamp', '2025-08-20 00:30:00',
                    'run_tstamp', '2025-08-21 00:46:07',
                    'operation_id', 'AnonymisationOperator__incoming__cms_mysql__shiro_user__20250820T003000__daily_at_00h30',
                    'created_at', CURRENT_TIMESTAMP()::TIMESTAMP,
                    'updated_at', CURRENT_TIMESTAMP()::TIMESTAMP,

                -- anonymisation_batch_result
                    'anonymised_by', 'source-system',
                    'status', batch.status,
                    'pii_columns', ARRAY_CONSTRUCT(
                        'username'
                    )
                )
            FROM hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250820T003000 batch
            WHERE batch.status = 'SKIPPED'
		AND target.id = batch.id
		AND target.row_dataset_name = batch.row_dataset_name
		AND target.row_dataset_source = batch.row_dataset_source
		AND target.row_loaded_at = batch.row_loaded_at
		AND target.row_schedule_tstamp = batch.row_schedule_tstamp
		AND target.row_run_tstamp = batch.row_run_tstamp
		AND target.row_filename = batch.row_filename
		AND target.row_file_row_number = batch.row_file_row_number
		;
            */


SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1 AND qh.query_id = '01be81ce-0107-53f3-0002-dd012c812a07'
;

-- updated 16,347


hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250820T003000

SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250820t003000__1755737212
;
-- 7,368

SELECT
	status,
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250820t003000__1755737212
GROUP BY ALL
;

SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1 AND qh.query_id = '01be81ce-0107-5771-0002-dd012c81352b'

-- updated 0


UPDATE hygiene_vault.cms_mysql.shiro_user target
SET target.anonymised             = TRUE,
	target.anonymisation_metadata = OBJECT_CONSTRUCT(
		-- (lineage) metadata of the current job
			'schedule_tstamp', '2025-08-20 00:30:00',
			'run_tstamp', '2025-08-21 00:46:07',
			'operation_id', 'AnonymisationOperator__incoming__cms_mysql__shiro_user__20250820T003000__daily_at_00h30',
			'created_at', CURRENT_TIMESTAMP()::TIMESTAMP,
			'updated_at', CURRENT_TIMESTAMP()::TIMESTAMP,

		-- anonymisation_batch_result
			'anonymised_by', 'source-system',
			'status', batch.status,
			'pii_columns', ARRAY_CONSTRUCT(
					'username'
						   )
									)
FROM hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000 batch
WHERE batch.status = 'SKIPPED'
  AND target.id = batch.id
  AND target.row_dataset_name = batch.row_dataset_name
  AND target.row_dataset_source = batch.row_dataset_source
  AND target.row_loaded_at = batch.row_loaded_at
  AND target.row_schedule_tstamp = batch.row_schedule_tstamp
  AND target.row_run_tstamp = batch.row_run_tstamp
  AND target.row_filename = batch.row_filename
  AND target.row_file_row_number = batch.row_file_row_number
;


WITH
	create_id AS (
		SELECT
			target.id ||
			target.row_dataset_name ||
			target.row_dataset_source ||
-- 		target.row_loaded_at ||
			target.created_at ||
			target.row_schedule_tstamp ||
			target.row_run_tstamp ||
			target.row_filename ||
			target.row_file_row_number AS fake_id,
			target.*
		FROM hygiene_vault.cms_mysql.shiro_user target
		INNER JOIN hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250820t003000__1755737212 batch
			ON
			batch.status = 'SKIPPED'
				AND target.id = batch.id
				AND target.row_dataset_name = batch.row_dataset_name
				AND target.row_dataset_source = batch.row_dataset_source
				AND target.created_at = batch.created_at
				AND target.row_schedule_tstamp = batch.row_schedule_tstamp
				AND target.row_run_tstamp = batch.row_run_tstamp
				AND target.row_filename = batch.row_filename
				AND target.row_file_row_number = batch.row_file_row_number
	)
-- SELECT * FROM create_id WHERE id = '438729'


SELECT *
FROM create_id
QUALIFY COUNT(*) OVER (PARTITION BY fake_id) > 1


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.cms_mysql.anonymisation_list__shiro_user__20250820t003000
AS
SELECT
	shiro_user_id AS id
FROM data_vault_mvp.dwh.user_attributes
WHERE membership_account_status = 'DELETED'
;


[2025-08-21, 00:46:35 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c813163
[2025-08-21, 00:46:35 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 1.2916 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00143506
[2025-08-21, 00:46:35 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:35 UTC] {{SQL.py:1913}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.cms_mysql.deduplicate_anonymisation_list__shiro_user__20250820t003000
AS
SELECT DISTINCT
	id
FROM hygiene_vault_dev_robin.cms_mysql.anonymisation_list__shiro_user__20250820t003000 batch
;


[2025-08-21, 00:46:38 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c8131ab
[2025-08-21, 00:46:38 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 2.5135 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00279278
[2025-08-21, 00:46:38 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:38 UTC] {{SQL.py:1913}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.cms_mysql.anonymisation_batch__shiro_user__20250820t003000
AS
SELECT
	-- (lineage) metadata of the original row
	target.row_dataset_name,
	target.row_dataset_source,
	target.row_loaded_at,
	target.row_schedule_tstamp,
	target.row_run_tstamp,
	target.row_filename,
	target.row_file_row_number,
	target.created_at,

	-- identifier
	batch.id,

	-- pii columns
	target.username

FROM hygiene_vault.cms_mysql.shiro_user target
INNER JOIN hygiene_vault_dev_robin.cms_mysql.deduplicate_anonymisation_list__shiro_user__20250820t003000 batch
	ON batch.id = target.id
	AND target.anonymised IS DISTINCT FROM TRUE
;


[2025-08-21, 00:46:46 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-53f3-0002-dd012c812817
[2025-08-21, 00:46:46 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 8.0420 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00893553
[2025-08-21, 00:46:46 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:46 UTC] {{SQL.py:1913}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000
AS
SELECT
	-- (lineage) metadata of the original row
	batch.row_dataset_name,
	batch.row_dataset_source,
	batch.row_loaded_at,
	batch.row_schedule_tstamp,
	batch.row_run_tstamp,
	batch.row_filename,
	batch.row_file_row_number,
	batch.created_at,

	-- identifier
	batch.id,

	-- pii columns
	batch.username,

	-- validation checks
	IFF(LOWER(username) RLIKE '^[a-f0-9]{64}$', 1, NULL)::INT                                                  AS fails_validation__username__looks_like_sha256,
	IFF(LOWER(username) RLIKE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.*', 1,
		NULL)::INT                                                                                             AS fails_validation__username__anonymised_at_source,
	IFF(LOWER(username) RLIKE '[a-f0-9]{40,}@email\.escapes\.tech', 1,
		NULL)::INT                                                                                             AS fails_validation__username__anonymised_at_source_legacy,
	IFF(fails_validation__username__looks_like_sha256 = 1
			OR fails_validation__username__anonymised_at_source = 1
			OR fails_validation__username__anonymised_at_source_legacy = 1,
		1,
		NULL
	)                                                                                                          AS failed_some_validation,

	-- anonymisation_batch_result
	CASE
		WHEN failed_some_validation = 1
			THEN 'SKIPPED'
		ELSE 'IN_PROGRESS'
	END                                                                                                        AS status

FROM hygiene_vault_dev_robin.cms_mysql.anonymisation_batch__shiro_user__20250820t003000 batch
;


[2025-08-21, 00:46:46 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c8133eb
[2025-08-21, 00:46:46 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.6357 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00070632
[2025-08-21, 00:46:46 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:46 UTC] {{SQL.py:1913}} INFO - Query:


SELECT COUNT(*) FROM hygiene_vault_dev_robin.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000;

13,843;

SELECT status, COUNT(*) FROM hygiene_vault_dev_robin.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000
GROUP BY ALL;

/*STATUS	COUNT(*)
IN_PROGRESS	8150
SKIPPED	5693*/


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.cms_mysql.shiro_user CLONE hygiene_vault.cms_mysql.shiro_user;

UPDATE hygiene_vault_dev_robin.cms_mysql.shiro_user target
SET target.anonymised             = TRUE,
	target.anonymisation_metadata = OBJECT_CONSTRUCT(
		-- (lineage) metadata of the current job
			'schedule_tstamp', '2025-08-20 00:30:00',
			'run_tstamp', '2025-08-21 00:46:07',
			'operation_id', 'AnonymisationOperator__incoming__cms_mysql__shiro_user__20250820T003000__daily_at_00h30',
			'created_at', CURRENT_TIMESTAMP()::TIMESTAMP,
			'updated_at', CURRENT_TIMESTAMP()::TIMESTAMP,

		-- anonymisation_batch_result
			'anonymised_by', 'source-system',
			'status', batch.status,
			'pii_columns', ARRAY_CONSTRUCT(
					'username'
						   )
									)
FROM hygiene_vault_dev_robin.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000 batch
WHERE batch.status = 'SKIPPED'
  AND target.id = batch.id
  AND target.row_dataset_name = batch.row_dataset_name
  AND target.row_dataset_source = batch.row_dataset_source
  AND target.created_at = batch.created_at
  AND target.row_schedule_tstamp = batch.row_schedule_tstamp
  AND target.row_run_tstamp = batch.row_run_tstamp
  AND target.row_filename = batch.row_filename
  AND target.row_file_row_number = batch.row_file_row_number
;

-- 9,475

[2025-08-21, 00:46:52 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-53f3-0002-dd012c812a07
[2025-08-21, 00:46:52 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 5.5576 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00617515
[2025-08-21, 00:46:52 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:52 UTC] {{SQL.py:1913}} INFO - Query:


UPDATE hygiene_vault_dev_robin.cms_mysql.shiro_user target
SET
	-- pii columns
	target.username               = CASE
										WHEN target.username IS NOT NULL
											AND LENGTH(TRIM(target.username)) > 0
											THEN SHA2(LOWER(target.username), 256)
										ELSE target.username
									END,

	target.anonymised             = TRUE,
	target.anonymisation_metadata = OBJECT_CONSTRUCT(
		-- (lineage) metadata of the current job
			'schedule_tstamp', '2025-08-20 00:30:00',
			'run_tstamp', '2025-08-21 00:46:07',
			'operation_id', 'AnonymisationOperator__incoming__cms_mysql__shiro_user__20250820T003000__daily_at_00h30',
			'created_at', CURRENT_TIMESTAMP()::TIMESTAMP,
			'updated_at', CURRENT_TIMESTAMP()::TIMESTAMP,

		-- anonymisation_batch_result
			'anonymised_by', 'data-pipeline',
			'status', 'SUCCESS',
			'pii_columns', ARRAY_CONSTRUCT(
					'username'
						   )
									)
FROM hygiene_vault_dev_robin.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000 batch
WHERE batch.status = 'IN_PROGRESS'
  AND target.id = batch.id
  AND target.row_dataset_name = batch.row_dataset_name
  AND target.row_dataset_source = batch.row_dataset_source
  AND target.created_at = batch.created_at
  AND target.row_schedule_tstamp = batch.row_schedule_tstamp
  AND target.row_run_tstamp = batch.row_run_tstamp
  AND target.row_filename = batch.row_filename
  AND target.row_file_row_number = batch.row_file_row_number
;


[2025-08-21, 00:46:52 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c81352b
[2025-08-21, 00:46:52 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.2399 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00026658
[2025-08-21, 00:46:52 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:52 UTC] {{SQL.py:1913}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.cms_mysql.shiro_user__deduplicate_anonymisation_list__shiro_user__20250820t003000__1755737212
AS
SELECT *
FROM hygiene_vault.cms_mysql.deduplicate_anonymisation_list__shiro_user__20250820t003000
;


[2025-08-21, 00:46:53 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c81353b
[2025-08-21, 00:46:53 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 1.0220 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00113556
[2025-08-21, 00:46:53 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:53 UTC] {{SQL.py:1913}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.cms_mysql.shiro_user__anonymisation_batch__shiro_user__20250820t003000__1755737212
AS
SELECT *
FROM hygiene_vault.cms_mysql.anonymisation_batch__shiro_user__20250820t003000
;


[2025-08-21, 00:46:54 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-53f3-0002-dd012c812b6f
[2025-08-21, 00:46:54 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.6687 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00074297
[2025-08-21, 00:46:54 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:54 UTC] {{SQL.py:1913}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.cms_mysql.shiro_user__apply_anonymisation_rules__shiro_user__20250820t003000__1755737212
AS
SELECT *
FROM hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000
;


[2025-08-21, 00:46:55 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c81358b
[2025-08-21, 00:46:55 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.7598 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00084427
[2025-08-21, 00:46:55 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:55 UTC] {{SQL.py:1913}} INFO - Query:


SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.anonymisation_batch__shiro_user__20250820t003000
;


[2025-08-21, 00:46:55 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1033 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00011477
[2025-08-21, 00:46:55 UTC] {{anonymisation_operator.py:811}} INFO - DEBUG count_table_rows (hygiene_vault.cms_mysql.anonymisation_batch__shiro_user__20250820T003000): 7368
[2025-08-21, 00:46:55 UTC] {{anonymisation_operator.py:444}} INFO - DEBUG anonymisation_batch_rows: 7368
[2025-08-21, 00:46:55 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:55 UTC] {{SQL.py:1913}} INFO - Query:


SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user
WHERE anonymised = TRUE
  AND anonymisation_metadata:status = 'SUCCESS'
  AND anonymisation_metadata:schedule_tstamp = '2025-08-20 00:30:00'
  AND anonymisation_metadata:run_tstamp = '2025-08-21 00:46:07'
;


[2025-08-21, 00:46:55 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.3017 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00033518
[2025-08-21, 00:46:55 UTC] {{anonymisation_operator.py:811}} INFO - DEBUG count_table_rows (hygiene_vault.cms_mysql.shiro_user): 0
[2025-08-21, 00:46:55 UTC] {{anonymisation_operator.py:459}} INFO - DEBUG anonymised_rows: 0
[2025-08-21, 00:46:55 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:55 UTC] {{SQL.py:1913}} INFO - Query:


SELECT
	COUNT(*)
FROM hygiene_vault.cms_mysql.shiro_user
WHERE anonymised = TRUE
  AND anonymisation_metadata:status = 'SKIPPED'
  AND anonymisation_metadata:schedule_tstamp = '2025-08-20 00:30:00'
  AND anonymisation_metadata:run_tstamp = '2025-08-21 00:46:07'
;


[2025-08-21, 00:46:56 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.3598 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00039980
[2025-08-21, 00:46:56 UTC] {{anonymisation_operator.py:811}} INFO - DEBUG count_table_rows (hygiene_vault.cms_mysql.shiro_user): 16347
[2025-08-21, 00:46:56 UTC] {{anonymisation_operator.py:472}} INFO - DEBUG skipped_rows: 16347
[2025-08-21, 00:46:56 UTC] {{anonymisation_operator.py:485}} INFO - DEBUG anonymisation_batch_result: [AnonymisationBatchResult(db_name='raw_vault', schema_name='cms_mysql', object_name='shiro_user', anonymisation_batch_rows=1382, anonymised_rows=0, skipped_rows=1382), AnonymisationBatchResult(db_name='hygiene_vault', schema_name='cms_mysql', object_name='shiro_user', anonymisation_batch_rows=7368, anonymised_rows=0, skipped_rows=16347)]
[2025-08-21, 00:46:56 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:56 UTC] {{SQL.py:1913}} INFO - Query:

DROP TABLE IF EXISTS hygiene_vault.cms_mysql.anonymisation_list__shiro_user__20250820t003000
;

[2025-08-21, 00:46:56 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c8135cb
[2025-08-21, 00:46:56 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0867 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00009630
[2025-08-21, 00:46:56 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:56 UTC] {{SQL.py:1913}} INFO - Query:

DROP TABLE IF EXISTS hygiene_vault.cms_mysql.deduplicate_anonymisation_list__shiro_user__20250820t003000
;

[2025-08-21, 00:46:56 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-53f3-0002-dd012c812bef
[2025-08-21, 00:46:56 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0785 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00008727
[2025-08-21, 00:46:56 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:56 UTC] {{SQL.py:1913}} INFO - Query:

DROP TABLE IF EXISTS hygiene_vault.cms_mysql.anonymisation_batch__shiro_user__20250820t003000
;

[2025-08-21, 00:46:56 UTC] {{SQL.py:1921}} INFO - Snowflake query ID = 01be81ce-0107-5771-0002-dd012c8135d3
[2025-08-21, 00:46:56 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1078 seconds, Snowflake WAREHOUSE size = pipe_hygiene_medium, Snowflake credits used (estimate) = 0.00011974
[2025-08-21, 00:46:56 UTC] {{SQL.py:1893}} INFO - USING CUSTOM WAREHOUSE: 'pipe_hygiene_medium'
[2025-08-21, 00:46:56 UTC] {{SQL.py:1913}} INFO - Query:

DROP TABLE IF EXISTS hygiene_vault.cms_mysql.apply_anonymisation_rules__shiro_user__20250820t003000
;