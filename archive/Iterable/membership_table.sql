python biapp/bau/hygiene/gen_hygiene_files.py \
    --data_source cms_mysql \
    --name membership \
    --primary_key_cols id \
    --new_record_col last_updated \

SELECT MIN(loaded_at) FROM raw_vault_mvp.cms_mysql.membership m;
--2020-03-23 10:14:16.048589000

self_describing_task --include 'staging/hygiene/cms_mysql/membership.py'  --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.membership CLONE raw_vault_mvp.cms_mysql.membership;


self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/membership.py'  --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'

SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.membership;

airflow backfill --start_date '2020-03-23 00:00:00' --end_date '2020-03-24 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__membership__daily_at_01h00