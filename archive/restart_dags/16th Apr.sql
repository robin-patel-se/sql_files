airflow backfill --start_date '2020-04-15 23:00:00' --end_date '2020-04-15 23:00:00' --task_regex '.*' incoming__cms_mysql__booking__hourly

airflow backfill --start_date '2020-04-15 13:00:00' --end_date '2020-04-15 13:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly
airflow backfill --start_date '2020-04-15 23:00:00' --end_date '2020-04-15 23:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking__hourly


airflow backfill --start_date '2020-04-16 03:00:00' --end_date '2020-04-16 03:00:00' --task_regex '.*' dwh__user_emails__daily_at_03h00


