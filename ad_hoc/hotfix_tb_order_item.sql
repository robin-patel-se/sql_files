
SELECT *
FROM raw_vault_mvp.travelbird_mysql.orders_orderevent oo;

SELECT GET_DDL('table', 'raw_vault_mvp.travelbird_mysql.orders_orderevent');


CREATE OR REPLACE TABLE orders_orderevent CLUSTER BY (TO_DATE(schedule_tstamp))
(
    dataset_name      VARCHAR(16777216) NOT NULL,
    dataset_source    VARCHAR(16777216) NOT NULL,
    schedule_interval VARCHAR(16777216) NOT NULL,
    schedule_tstamp   TIMESTAMP_NTZ(9)  NOT NULL,
    run_tstamp        TIMESTAMP_NTZ(9)  NOT NULL,
    loaded_at         TIMESTAMP_NTZ(9)  NOT NULL,
    filename          VARCHAR(16777216) NOT NULL,
    file_row_number   NUMBER(38, 0)     NOT NULL,
    id                NUMBER(38, 0),
    created_at_dts    TIMESTAMP_NTZ(9),
    event_type        VARCHAR(16777216),
    event_data        VARCHAR(16777216),
    order_id          NUMBER(38, 0),
    extract_metadata  VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);


python biapp/bau/hygiene/gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name orders_orderevent \
    --primary_key_cols id \
    --new_record_col created_at_dts \
    --detect_deleted_records


CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.orders_orderevent clone raw_vault_mvp.travelbird_mysql.orders_orderevent;

self_describing_task --include 'staging/hygiene/travelbird_mysql/orders_orderevent.py'  --method 'run' --start '2021-04-14 00:00:00' --end '2021-04-14 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/orders_orderevent.py'  --method 'run' --start '2021-04-14 00:00:00' --end '2021-04-14 00:00:00'

SELECT * FROM hygiene_vault_mvp_dev_robin.travelbird_mysql.orders_orderevent;

self_describing_task --include 'dv/dwh/transactional/tb_order_item_changelog.py'  --method 'run' --start '2021-04-14 00:00:00' --end '2021-04-14 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog;
SELECT * FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog toic;



python biapp/bau/hygiene/gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name currency_exchangerateupdate \
    --primary_key_cols id \
    --new_record_col updated_at_dts \

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-04-14 00:00:00' --end '2021-04-14 00:00:00'

self_describing_task --include 'dv/fx/tb_rates.py'  --method 'run' --start '2021-04-14 00:00:00' --end '2021-04-14 00:00:00'

SELECT min(loaded_at) FROM raw_vault_mvp.travelbird_mysql.orders_orderevent oo; -- 2021-04-15 08:28:33.298609000
SELECT COUNT(*) FROM raw_vault_mvp.travelbird_mysql.orders_orderevent oo;

airflow backfill --start_date '2021-04-14 01:00:00' --end_date '2021-04-14 01:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__orders_orderevent__daily_at_01h00
DROP TABLE data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot;
airflow backfill --start_date '2021-04-14 07:00:00' --end_date '2021-04-14 07:00:00' --task_regex '.*' dv_create_views__tableau_travelbird_mysql__daily_at_07h00
