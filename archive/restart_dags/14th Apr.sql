--jobs complete but marked as failed
airflow backfill --start_date '2020-04-09 18:00:00' --end_date '2020-04-11 15:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__orders_order__hourly

--mark as complete because earliest job was complete
airflow backfill --start_date '2020-04-13 00:00:00' --end_date '2020-04-13 00:00:00' --task_regex '.*'  -m single_customer_view__daily

--catching jobs up
airflow backfill --start_date '2020-04-12 23:00:00' --end_date '2020-04-12 23:00:00' --task_regex '.*'  dwh__transactional__booking__hourly
airflow backfill --start_date '2020-04-13 23:00:00' --end_date '2020-04-13 23:00:00' --task_regex '.*'  dwh__transactional__booking__hourly

airflow backfill --start_date '2020-04-13 21:00:00' --end_date '2020-04-13 21:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__django_content_type__hourly
airflow backfill --start_date '2020-04-13 23:00:00' --end_date '2020-04-13 23:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__django_content_type__hourly

airflow backfill --start_date '2020-04-07 00:00:00' --end_date '2020-04-13 00:00:00' --task_regex '.*' active_user_base__daily

