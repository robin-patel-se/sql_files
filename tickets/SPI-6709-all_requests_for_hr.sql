-- current state is to copy paste data directly from hibob into the all requests tab of sheet: https://docs.google.com/spreadsheets/d/1EABZxdpjjVkApPnIzA3R1Yt_b9JEiLXJKWNKsYY5JrU/edit?gid=229960812#gid=229960812
-- ingest this all_requests data using the branch: SPI-6709-ingest-all-requests-dataset
-- run sql query below to get data to be processed for the absence sheet
-- paste results back into the absence gsheet
-- let absence process ingest
-- run downstream


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS raw_vault_dev_robin.hr_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.hr_gsheets.all_requests
	CLONE raw_vault.hr_gsheets.all_requests
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_dev_robin.hr_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.hr_gsheets.all_requests
	CLONE hygiene_vault.hr_gsheets.all_requests
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.hr_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.hr_gsheets.all_requests
	CLONE latest_vault.hr_gsheets.all_requests
;

dataset_task
\
    --include 'incoming.hr_gsheets.all_requests' \
    --kind 'incoming' \
    --operation LatestRecordsOperation \
    --method 'run' \
    --upstream \
    --start '2024-11-14 00:00:00' \
    --end '2024-11-14 00:00:00'

-- branch: SPI-6709-ingest-all-requests-dataset

SELECT
	-- (lineage) metadata for the current job
	'2024-11-14 00:00:00'                                                         AS schedule_tstamp,
	'2024-11-14 12:18:09'                                                         AS run_tstamp,
	'HygieneOperator__incoming__hr_gsheets__all_requests__20241114T000000__daily' AS operation_id,
	CURRENT_TIMESTAMP()::TIMESTAMP                                                AS created_at,
	CURRENT_TIMESTAMP()::TIMESTAMP                                                AS updated_at,

	-- (lineage) original metadata of row itself
	dataset_name::VARCHAR                                                         AS row_dataset_name,
	dataset_source::VARCHAR                                                       AS row_dataset_source,
	loaded_at::TIMESTAMP                                                          AS row_loaded_at,
	schedule_tstamp::TIMESTAMP                                                    AS row_schedule_tstamp,
	run_tstamp::TIMESTAMP                                                         AS row_run_tstamp,
	filename::VARCHAR                                                             AS row_filename,
	file_row_number::INT                                                          AS row_file_row_number,
	extract_metadata::VARIANT                                                     AS row_extract_metadata,


	-- original columns
	"Display name"::VARCHAR                                                       AS display_name,
	email::VARCHAR                                                                AS email,
	"Request Period"::VARCHAR                                                     AS request_period__o,
	"Total Duration"::VARCHAR                                                     AS total_duration__o,
	"Policy type"::VARCHAR                                                        AS policy_type,
	policy::VARCHAR                                                               AS policy,
	"Reason code"::VARCHAR                                                        AS reason_code,
	status::VARCHAR                                                               AS status,
	"Requested on"::VARCHAR                                                       AS requested_on__o,
	approvers::VARCHAR                                                            AS approvers

FROM hygiene_vault_dev_robin.hr_gsheets.get_source_batch__all_requests__20241114t000000



SELECT
	request_period__o,
	request_period,
	TRY_TO_DATE(SPLIT_PART(request_period__o, ' ', 1), 'dd/MM/yyyy')   AS day_away,
	INITCAP(REGEXP_SUBSTR(request_period__o, '\\((.*)\\)', 1, 1, 'e')) AS day_portion
FROM hygiene_vault_dev_robin.hr_gsheets.all_requests
;


------------------------------------------------------------------------------------------------------------------------
-- modelling sql query
SELECT
	display_name,
	email,
	TO_CHAR(day_away, 'dd/MM/yyyy')     AS day_away,
	total_duration,
	'days'                              AS unit,
	day_portion,
	policy_type,
	policy,
	reason_code,
	CASE
		WHEN policy IN
			 (
			  'Business trip abroad',
			  'Work From Anywhere',
			  'Occassional Remote Work PL'
				 )
			THEN 'Yes'
		ELSE 'No'
	END                                 AS working,
	status,
	TO_CHAR(requested_on, 'dd/MM/yyyy') AS requested_on,
	approvers
-- 	request_period,
-- 	requested_on__o,
FROM latest_vault_dev_robin.hr_gsheets.all_requests
;
------------------------------------------------------------------------------------------------------------------------

DELETE FROM raw_vault.hr_gsheets.absence
WHERE loaded_at::DATE = current_date

DELETE FROM hygiene_vault.hr_gsheets.absence
WHERE row_loaded_at::DATE = current_date