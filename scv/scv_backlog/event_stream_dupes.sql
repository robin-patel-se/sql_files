USE WAREHOUSE pipe_xlarge;


SELECT event_hash, COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
-- WHERE es.etl_tstamp >= CURRENT_DATE - 1
GROUP BY 1
HAVING count(*) > 1;


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_hash IN (
    SELECT event_hash
    FROM hygiene_vault_mvp.snowplow.event_stream es
-- WHERE es.etl_tstamp >= CURRENT_DATE - 1
    GROUP BY 1
    HAVING count(*) > 1
);

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_bkup CLONE hygiene_vault_mvp.snowplow.event_stream;
self_describing_task --include 'staging/hygiene/snowplow/events'  --method 'run' --start '2020-06-24 00:00:00' --end '2020-06-24 00:00:00'

SELECT COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es; -- 9327808483
SELECT COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es; --9327728601

CREATE OR REPLACE TABLE hygiene_vault_mvp.snowplow.event_stream CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

