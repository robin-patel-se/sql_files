airflow backfill --start_date '2020-05-14 03:00:00' --end_date '2020-05-14 03:00:00' --task_regex '.*' single_customer_view__daily_at_03h00
airflow backfill --start_date '2020-05-15 03:00:00' --end_date '2020-05-17 03:00:00' --task_regex '.*' -m single_customer_view__daily_at_03h00
