airflow backfill --start_date '2020-04-16 03:00:00' --end_date '2020-04-16 03:00:00' --task_regex '.*' single_customer_view__daily_at_03h00

airflow backfill --start_date '2020-04-17 03:00:00' --end_date '2020-04-19 03:00:00' --task_regex '.*' -m single_customer_view__daily_at_03h00

airflow clear --start_date '2020-04-15 03:00:00' --end_date '2020-04-19 03:00:00' --task_regex '.*' dwh__transactional__user_subscription__daily_at_03h00
airflow backfill --start_date '2020-04-15 03:00:00' --end_date '2020-04-19 03:00:00' --task_regex '.*' dwh__transactional__user_subscription__daily_at_03h00

airflow backfill --start_date '2020-04-15 03:00:00' --end_date '2020-04-19 03:00:00' --task_regex '.*'  -m active_user_base__daily_at_03h00


SELECT *
FROM se.data.active_user_base;

SELECT *
FROM se.data.user_emails;

