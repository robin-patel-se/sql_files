SELECT es.contexts_com_secretescapes_search_context_1,
       es.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR AS triggered_by
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 1;


SELECT *
FROM snowplow.atomic.events es
WHERE es.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR IS NOT NULL;


SELECT *
FROM collab.booking_cancellation_data.booking_cancellation bc
WHERE;

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches;

self_describing_task --include 'dv/dwh/events/07_events_of_interest/03_module_touched_searches.py'  --method 'run' --start '2021-11-25 00:00:00' --end '2021-11-25 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches_20211126 CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches ADD COLUMN triggered_by VARCHAR;
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches target
SET target.triggered_by = target.search_context['triggered_by']::VARCHAR;


ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches ADD COLUMN triggered_by VARCHAR;

SELECT get_ddl('table', 'hygiene_vault_mvp_dev_robin.snowplow.event_stream');


self_describing_task --include 'dv/dwh/events/trimmed_event_stream.py'  --method 'run' --start '2021-11-28 00:00:00' --end '2021-11-28 00:00:00'
self_describing_task --include 'se/data_pii/scv/trimmed_event_stream.py'  --method 'run' --start '2021-11-28 00:00:00' --end '2021-11-28 00:00:00'

SELECT * FROm se_dev_robin.data_pii.trimmed_event_stream;

