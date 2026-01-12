airflow backfill --start_date '2020-04-20 03:00:00' --end_date '2020-04-20 03:00:00' --task_regex '.*' dwh__transactional__booking__daily_at_03h00

SELECT * FROM raw_vault_mvp.cms_mongodb.booking_summary;