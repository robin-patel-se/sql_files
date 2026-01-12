CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking fb;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/00_artificial_transaction_insert/artificial_transaction_insert.py'  --method 'run' --start '2019-09-01 00:00:00' --end '2019-09-01 00:00:00'


SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE created_at > TIMESTAMPADD(minute, -10, CURRENT_TIMESTAMP)



