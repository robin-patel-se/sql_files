SELECT oos.order_id,
       oos.created_at_dts                                    AS cancellation_date,
       PARSE_JSON(oos.event_data):adjustment_reason::VARCHAR AS adjustment_reason
FROM data_vault_mvp.travelbird_cms.orders_orderevent_snapshot oos
WHERE oos.event_type = 'ORDER_CANCELLED';

SELECT * FROM data_vault_mvp.dwh.se_booking sb;
self_describing_task --include '/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2020-11-19 00:00:00' --end '2020-11-19 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_orderevent_snapshot clone data_vault_mvp.travelbird_cms.orders_orderevent_snapshot;

SELECT tb.updated_at_dts, tb.cancellation_date FROM data_vault_mvp_dev_robin.dwh.tb_booking tb WHERE tb.updated_at_dts::DATE != tb.cancellation_date AND tb.payment_status = 'CANCELLED';

CREATE OR REPLACE TABLE scratch.robinpatel.test AS select 1;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.test AS SELECT 2 as test1;