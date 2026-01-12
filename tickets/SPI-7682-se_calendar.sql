USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.calendar
	CLONE data_vault_mvp.dwh.calendar
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.ad_hoc.se_calendar.py' \
    --method 'run' \
    --start '2025-08-26 00:00:00' \
    --end '2025-08-26 00:00:00'


SELECT
	calendar.date_value,
	DAYOFYEAR(calendar.date_value)                                  AS day_of_year,
	DAYOFYEAR(CURRENT_DATE() - 1),
	DAYOFYEAR(calendar.date_value) <= DAYOFYEAR(CURRENT_DATE() - 1) AS is_year_to_date,
	DATE_PART(QUARTER, date_value),
	DATE_PART(QUARTER, CURRENT_DATE() - 1),
	is_year_to_date AND DATE_PART(QUARTER, date_value) =
			DATE_PART(QUARTER, CURRENT_DATE() - 1) AS is_quarter_to_date,
FROM se.data.se_calendar calendar
WHERE calendar.date_value BETWEEN '2024-01-01' AND CURRENT_DATE + 30


SELECT *
FROM snowflake.account_usage.tag_references tr
WHERE tr.object_database = 'DATA_VAULT_MVP'
;