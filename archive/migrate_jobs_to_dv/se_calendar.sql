SELECT * FROM data_vault_mvp.dwh.calendar sc;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar  clone data_vault_mvp.dwh.se_calendar;

ALTER TABLE data_vault_mvp_dev_robin.dwh.se_calendar RENAME TO data_vault_mvp_dev_robin.dwh.calendar;

self_describing_task --include 'dv/dwh/ad_hoc/se_calendar.py'  --method 'run' --start '2021-07-21 00:00:00' --end '2021-07-21 00:00:00'
self_describing_task --include 'se/data/dwh/se_calendar.py'  --method 'run' --start '2021-07-21 00:00:00' --end '2021-07-21 00:00:00'

SELECT * FROM scratch.robinpatel.se_calendar sc
MINUS
SELECT * FROM se.data.se_calendar s;

airflow clear --start_date '2021-07-21 00:00:00' --end_date '2021-07-22 00:00:00' --task_regex '.*' se_bi_object_creation__daily_at_07h00
airflow backfill --start_date '2021-07-21 00:00:00' --end_date '2021-07-22 00:00:00' --task_regex '.*' se_bi_object_creation__daily_at_07h00
airflow clear --start_date '2021-07-21 00:00:00' --end_date '2021-07-22 00:00:00' --task_regex '.*' se_data_object_creation__daily_at_07h00
airflow backfill --start_date '2021-07-21 00:00:00' --end_date '2021-07-22 00:00:00' --task_regex '.*' se_data_object_creation__daily_at_07h00

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.se_calendar AS SELECT * FROM se.data.se_calendar;


SELECT * FROM scratch.robinpatel.se_calendar sc
EXCEPT
SELECT * FROM se.data.se_calendar s;