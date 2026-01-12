dataset_task --include 'cms_mysql.subscription' --operation ProductionIngestOperation --method 'run' --upstream --start '2021-09-12 00:00:00' --end '2021-09-12 00:00:00'

1970-01-01 00:00:00;


SELECT * FROM raw_vault_mvp_dev_robin.cms_mysql.subscription;

python biapp/bau/hygiene/gen_hygiene_files.py \
    --data_source cms_mysql \
    --name subscription \
    --primary_key_cols id \
    --new_record_col last_updated \
    --detect_deleted_records

self_describing_task --include 'staging/hygiene/cms_mysql/subscription.py'  --method 'run' --start '2021-09-13 00:00:00' --end '2021-09-13 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/subscription.py'  --method 'run' --start '2021-09-13 00:00:00' --end '2021-09-13 00:00:00'


backfill ingest: 1970-01-01 00:00:00

airflow backfill --start_date '1970-01-01 00:00:00' --end_date '1970-01-02 00:00:00' --task_regex '.*' incoming__cms_mysql__subscription__daily_at_00h30

