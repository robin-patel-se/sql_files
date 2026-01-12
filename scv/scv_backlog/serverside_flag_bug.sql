SELECT count(*)
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE v_tracker LIKE 'ios%'
  AND is_server_side_event;

USE WAREHOUSE pipe_xlarge;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream clone hygiene_vault_mvp.snowplow.event_stream;

UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream target
SET is_server_side_event = FALSE
WHERE v_tracker LIKE 'ios%' AND collector_tstamp >= '2020-02-28';


CREATE OR REPLACE TABLE hygiene_vault_mvp.snowplow.event_stream clone hygiene_vault_mvp_dev_robin.snowplow.event_stream;