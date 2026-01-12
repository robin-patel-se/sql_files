self_describing_task --include 'biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/01_artificial_transaction_insert.py'  --method 'run' --start '2024-07-29 00:00:00' --end '2024-07-29 00:00:00'

CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS select * from hygiene_vault_mvp.snowplow.event_stream es;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS select * from data_vault_mvp.dwh.fact_booking es;