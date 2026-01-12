-- https://docs.google.com/spreadsheets/d/11GzZoFPzByTNS7HK1LOFg3EvPdol_f2ttaeGKYNB6FA/edit#gid=0
USE ROLE personal_role__robinpatel;
DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;
DROP TABLE scratch.robinpatel.events;
-- create a static clone of the data
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.events CLONE snowplow.atomic.events;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_backup CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_test CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream_backup;
-- master branch
self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2023-04-19 10:00:00' --end '2023-04-19 10:00:00'
-- record run duration and costs

SELECT *
FROM snowflake.account_usage.query_history
WHERE query_id = '01abbb1e-0302-70f4-0000-02dddb60c286';
SELECT *
FROM snowflake.account_usage.query_history
WHERE query_id = '01abbb1e-0302-70f4-0000-02dddb60c40a';
SELECT *
FROM snowflake.account_usage.query_history
WHERE query_id = '01abbb1e-0302-70f4-0000-02dddb60c546';
--merge

-- replace event stream with backup so apples for apples
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_test CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream_backup;
-- switch to test branch and run again
self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream_test.py'  --method 'run' --start '2023-04-24 14:00:00' --end '2023-04-24 14:00:00'


DROP TABLE scratch.robinpatel.events;

SELECT *
FROM scratch.robinpatel.events
WHERE etl_tstamp >= '2023-04-19 11:00:00'::TIMESTAMP;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;


------------------------------------------------------------------------------------------------------------------------

-- compare event_stream and event_stream_test
USE WAREHOUSE pipe_xlarge;
-- event counts
--prod
SELECT
    COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.etl_tstamp < '2023-04-26 07:00:00';
--test
SELECT
    COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream_test es
WHERE es.etl_tstamp < '2023-04-26 07:00:00';


------------------------------------------------------------------------------------------------------------------------

