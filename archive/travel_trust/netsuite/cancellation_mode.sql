SELECT * FROM raw_vault_mvp.cms_mysql.booking_cancellation bc;

------------------------------------------------------------------------------------------------------------------------
python biapp/bau/hygiene/gen_hygiene_files.py \
    --data_source cms_mysql \
    --name booking_cancellation \
    --primary_key_cols id \
    --new_record_col last_updated \

--run on prod
SELECT MIN(last_updated) FROM booking_cancellation bc -- 2015-03-26 17:06:06




dataset_task --include 'cms_mysql.booking_cancellation' --operation ProductionIngestOperation --method 'run' --upstream --start '2015-03-26 00:30:00' --end '2015-03-26 00:30:00'

airflow backfill --start_date '2015-03-26 00:00:00' --end_date '2015-03-27 00:00:00' --reset_dagruns --task_regex '.*' incoming__cms_mysql__booking_cancellation__daily_at_00h30

SELECT * FROM raw_vault_mvp_dev_robin.cms_mysql.booking_cancellation;
SELECT COUNT(*) FROM raw_vault_mvp.cms_mysql.booking_cancellation bc;


DROP TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation;;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation;;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.cms_mysql.booking_cancellation_20210816 CLONE hygiene_vault_mvp.cms_mysql.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation_20210816 CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp.cms_mysql.booking_cancellation_20210816 CLONE raw_vault_mvp.cms_mysql.booking_cancellation;

self_describing_task --include 'staging/hygiene/cms_mysql/booking_cancellation.py'  --method 'run' --start '2021-08-15 00:00:00' --end '2021-08-15 00:00:00'

self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/booking_cancellation.py'  --method 'run' --start '2021-08-15 00:00:00' --end '2021-08-15 00:00:00'

SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation;
SELECT get_ddl('table', 'hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation');
SELECT get_ddl('table', 'hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation');

--dev
create or replace TABLE BOOKING_CANCELLATION (
	BOOKING_ID VARCHAR(16777216),
	BOOKING_FEE_CC NUMBER(19,2),
	CC_FEE_CC NUMBER(19,2),
	HOTEL_GOOD_WILL_CC NUMBER(19,2),
	SE_GOOD_WILL_CC NUMBER(19,2),
	REQUESTED_BY_DOMAIN VARCHAR(16777216),
	ID NUMBER(38,0) NOT NULL,
	VERSION NUMBER(38,0),
	BOOKING_ID__O NUMBER(38,0),
	DATE_CREATED TIMESTAMP_NTZ(9),
	LAST_UPDATED TIMESTAMP_NTZ(9),
	FAULT VARCHAR(16777216),
	REASON VARCHAR(16777216),
	BOOKING_FEE__O NUMBER(19,2),
	CC_FEE__O NUMBER(19,2),
	HOTEL_GOOD_WILL__O NUMBER(19,2),
	REFUND_CHANNEL VARCHAR(16777216),
	REFUND_TYPE VARCHAR(16777216),
	SE_GOOD_WILL__O NUMBER(19,2),
	WHO_PAYS VARCHAR(16777216),
	RESERVATION_ID__O NUMBER(38,0),
	CANCEL_WITH_PROVIDER NUMBER(38,0),
	STATUS VARCHAR(16777216),
	REQUESTED_BY VARCHAR(16777216),
	PAYMENT_PROVIDER_REFUND_STATUS VARCHAR(16777216),
	REBOOKING NUMBER(38,0),
	CANCELLATION_MODE VARCHAR(16777216),
	constraint PK_1 primary key (ID)
);

--prod
create or replace TABLE BOOKING_CANCELLATION (
	BOOKING_ID VARCHAR(16777216),
	BOOKING_FEE_CC FLOAT,
	CC_FEE_CC FLOAT,
	HOTEL_GOOD_WILL_CC FLOAT,
	SE_GOOD_WILL_CC FLOAT,
	ID NUMBER(38,0) NOT NULL,
	VERSION NUMBER(38,0),
	BOOKING_ID__O NUMBER(38,0),
	RESERVATION_ID__O NUMBER(38,0),
	DATE_CREATED TIMESTAMP_NTZ(9),
	LAST_UPDATED TIMESTAMP_NTZ(9),
	FAULT VARCHAR(16777216),
	REASON VARCHAR(16777216),
	BOOKING_FEE__O FLOAT,
	CC_FEE__O FLOAT,
	HOTEL_GOOD_WILL__O FLOAT,
	SE_GOOD_WILL__O FLOAT,
	REFUND_CHANNEL VARCHAR(16777216),
	REFUND_TYPE VARCHAR(16777216),
	WHO_PAYS VARCHAR(16777216),
	CANCEL_WITH_PROVIDER NUMBER(38,0),
	STATUS VARCHAR(16777216),
	REQUESTED_BY VARCHAR(16777216),
	REQUESTED_BY_DOMAIN VARCHAR(16777216),
	PAYMENT_PROVIDER_REFUND_STATUS VARCHAR(16777216),
	primary key (ID)
);

airflow backfill --start_date '2021-08-15 00:00:00' --end_date '2021-08-16 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking_cancellation__daily_at_01h00
self_describing_task --include 'dv/dwh/transactional/se_booking_cancellation.py'  --method 'run' --start '2021-08-15 00:00:00' --end '2021-08-15 00:00:00'


SELECT bc.dataset_name,
       bc.dataset_source,
       bc.schedule_interval,
       bc.schedule_tstamp,
       bc.run_tstamp,
       bc.loaded_at,
       bc.filename,
       bc.file_row_number,
       bc.extract_metadata,
       bc.id,
       bc.version,
       bc.booking_id,
       bc.date_created,
       bc.last_updated,
       bc.fault,
       bc.reason,
       bc.booking_fee,
       bc.cc_fee,
       bc.hotel_good_will,
       bc.refund_channel,
       bc.refund_type,
       bc.se_good_will,
       bc.who_pays,
       bc.reservation_id,
       bc.cancel_with_provider,
       bc.status,
       bc.requested_by,
       bc.payment_provider_refund_status,
       bc.rebooking
FROM raw_vault_mvp.cms_mysql.booking_cancellation bc

DROP TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation;

self_describing_task --include 'staging/hygiene/cms_mysql/booking_cancellation.py'  --method 'run' --start '2021-08-16 00:00:00' --end '2021-08-16 00:00:00'

SELECT * FROm se.data.fact_complete_booking fb;