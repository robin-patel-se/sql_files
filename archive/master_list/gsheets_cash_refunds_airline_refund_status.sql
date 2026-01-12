SELECT crars.dataset_name,
       crars.dataset_source,
       crars.schedule_interval,
       crars.schedule_tstamp,
       crars.run_tstamp,
       crars.loaded_at,
       crars.filename,
       crars.file_row_number,
       updated,
       flight_carrier,
       type,
       refund_type,
       reported_refund_type,
       extract_metadata
FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status crars;

create table FINANCE_GSHEETS.CASH_REFUNDS_AIRLINE_REFUND_STATUS
(
	DATASET_NAME VARCHAR not null,
	DATASET_SOURCE VARCHAR not null,
	SCHEDULE_INTERVAL VARCHAR not null,
	SCHEDULE_TSTAMP TIMESTAMPNTZ not null,
	RUN_TSTAMP TIMESTAMPNTZ not null,
	LOADED_AT TIMESTAMPNTZ not null,
	FILENAME VARCHAR not null,
	FILE_ROW_NUMBER NUMBER not null,
	UPDATED DATE,
	FLIGHT_CARRIER VARCHAR,
	TYPE VARCHAR,
	REFUND_TYPE VARCHAR,
	REPORTED_REFUND_TYPE VARCHAR,
	EXTRACT_METADATA VARIANT,
	primary key (DATASET_NAME, DATASET_SOURCE, SCHEDULE_INTERVAL, SCHEDULE_TSTAMP, RUN_TSTAMP, FILENAME, FILE_ROW_NUMBER)
)
cluster by (TO_DATE(schedule_tstamp));

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_status clone raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status;

self_describing_task --include 'staging/hygiene/finance_gsheets/cash_refunds_airline_refund_status'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM hygiene_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_status;

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/cash_refunds_airline_refund_status'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

