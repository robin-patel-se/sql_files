CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.exchange_rate CLONE raw_vault_mvp.cms_mysql.exchange_rate;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.fpa_gsheets.fx CLONE raw_vault_mvp.fpa_gsheets.fx;

CREATE OR REPLACE VIEW se_dev_robin.data.se_calendar AS
SELECT *
FROM se.data.se_calendar sc;

self_describing_task --include 'dv/fx/rates.py'  --method 'run' --start '2021-07-13 00:00:00' --end '2021-07-13 00:00:00'

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.fx;

SELECT GET_DDL('table', 'data.fx');

airflow backfill --start_date '2022-01-03 00:00:00' --end_date '2022-01-04 00:00:00' --task_regex '.*' dwh__email_performance__daily_at_04h00

self_describing_task --include 'dv/dwh/email/email_performance.py'  --method 'run' --start '2022-01-03 00:00:00' --end '2022-01-03 00:00:00';

SELECT *
FROM se.data.email_performance ep;

SELECT * FROM se.data.crm_events_opens ceo WHERE ceo.crm_platform = 'iterable';

SELECT * FROM latest_vault.iterable.email_open eo;

