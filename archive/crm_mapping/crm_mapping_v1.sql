SELECT s.dataset_name,
       s.dataset_source,
       s.schedule_interval,
       s.schedule_tstamp,
       s.run_tstamp,
       s.loaded_at,
       s.filename,
       s.file_row_number,
       s.client_id,
       s.send_id,
       s.subscriber_key,
       s.email_address,
       s.subscriber_id,
       s.list_id,
       s.event_date,
       s.event_type,
       s.batch_id,
       s.triggered_send_external_key,
       s.extract_metadata
FROM raw_vault_mvp.sfmc.events_sends s
WHERE s.loaded_at >= current_date - 30;

SELECT get_ddl('table', 'raw_vault_mvp.sfmc.events_sends');


CREATE OR REPLACE TABLE events_sends CLUSTER BY (TO_DATE(schedule_tstamp))
(
    dataset_name                VARCHAR,
    dataset_source              VARCHAR,
    schedule_interval           VARCHAR,
    schedule_tstamp             TIMESTAMP,
    run_tstamp                  TIMESTAMP,
    loaded_at                   TIMESTAMP,
    filename                    VARCHAR,
    file_row_number             NUMBER,

    client_id                   NUMBER,
    send_id                     NUMBER,
    subscriber_key              VARCHAR,
    email_address               VARCHAR,
    subscriber_id               NUMBER,
    list_id                     NUMBER,
    event_date                  TIMESTAMP,
    event_type                  VARCHAR,
    batch_id                    NUMBER,
    triggered_send_external_key VARCHAR,
    extract_metadata            VARIANT,

    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);

SELECT *
FROM raw_vault_mvp.sfmc.events_opens_plus_inferred s
WHERE s.loaded_at >= current_date - 30
    QUALIFY count(*) OVER (PARTITION BY s.subscriber_key, s.send_id, s.batch_id) > 1
;

CREATE OR REPLACE SCHEMA raw_vault_mvp_dev_robin.sfmc;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_sends CLONE raw_vault_mvp.sfmc.events_sends;

USE WAREHOUSE pipe_xlarge;

DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.events_sends;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends;


self_describing_task --include 'staging/hygiene/sfmc/events_sends.py'  --method 'run' --start '2020-09-24 00:00:00' --end '2020-09-24 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_sends.py'  --method 'run' --start '2020-09-24 00:00:00' --end '2020-09-24 00:00:00'

SELECT COUNT(*)
FROM hygiene_vault_mvp_dev_robin.sfmc.events_sends es;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends;

SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.events_sends es
    QUALIFY count(*) OVER (PARTITION BY es.event_hash) > 1;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends es
WHERE TRY_TO_NUMBER(subscriber_key) IS NULL
  AND shiro_user_id IS NOT NULL;
;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends;

SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends es;


------------------------------------------------------------------------------------------------------------------------

SELECT get_ddl('table', 'raw_vault_mvp.sfmc.events_opens_plus_inferred');

CREATE OR REPLACE TABLE events_opens_plus_inferred CLUSTER BY (TO_DATE(schedule_tstamp))
(
    dataset_name                VARCHAR,
    dataset_source              VARCHAR,
    schedule_interval           VARCHAR,
    schedule_tstamp             TIMESTAMP,
    run_tstamp                  TIMESTAMP,
    loaded_at                   TIMESTAMP,
    filename                    VARCHAR,
    file_row_number             NUMBER,
    client_id                   NUMBER,
    send_id                     NUMBER,
    subscriber_key              VARCHAR,
    email_address               VARCHAR,
    subscriber_id               NUMBER,
    list_id                     NUMBER,
    event_date                  TIMESTAMP,
    event_type                  VARCHAR,
    batch_id                    NUMBER,
    triggered_send_external_key VARCHAR,
    ip_address                  VARCHAR,
    country                     VARCHAR,
    region                      VARCHAR,
    city                        VARCHAR,
    latitude                    NUMBER,
    longitude                   NUMBER,
    metrocode                   NUMBER,
    area_code                   NUMBER,
    browser                     VARCHAR,
    email_client                VARCHAR,
    operating_system            VARCHAR,
    device                      VARCHAR,
    extract_metadata            VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);


------------------------------------------------------------------------------------------------------------------------


self_describing_task --include 'staging/hygiene/sfmc/events_sends.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM raw_vault_mvp.sfmc.events_opens_plus_inferred eopi;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE raw_vault_mvp.sfmc.events_opens_plus_inferred;

self_describing_task --include 'staging/hygiene/sfmc/events_opens_plus_inferred.py'  --method 'run' --start '2020-09-24 00:00:00' --end '2020-09-24 00:00:00';
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_opens_plus_inferred.py'  --method 'run' --start '2020-09-24 00:00:00' --end '2020-09-24 00:00:00';

DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred;

SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred;
SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_clicks CLONE raw_vault_mvp.sfmc.events_clicks;

SELECT get_ddl('table', 'raw_vault_mvp.sfmc.events_clicks');

CREATE OR REPLACE TABLE events_clicks
(
    dataset_name                VARCHAR,
    dataset_source              VARCHAR,
    schedule_interval           VARCHAR,
    schedule_tstamp             TIMESTAMP,
    run_tstamp                  TIMESTAMP,
    loaded_at                   TIMESTAMP,
    filename                    VARCHAR,
    file_row_number             NUMBER,
    client_id                   NUMBER,
    send_id                     NUMBER,
    subscriber_key              VARCHAR,
    email_address               VARCHAR,
    subscriber_id               NUMBER,
    list_id                     NUMBER,
    event_date                  TIMESTAMP,
    event_type                  VARCHAR,
    send_url_id                 NUMBER,
    url_id                      NUMBER,
    url                         VARCHAR,
    alias                       VARCHAR,
    batch_id                    NUMBER,
    triggered_send_external_key VARCHAR,
    ip_address                  VARCHAR,
    country                     VARCHAR,
    region                      VARCHAR,
    city                        VARCHAR,
    latitude                    NUMBER,
    longitude                   NUMBER,
    metrocode                   NUMBER,
    area_code                   NUMBER,
    browser                     VARCHAR,
    email_client                VARCHAR,
    operating_system            VARCHAR,
    device                      VARCHAR,
    extract_metadata            VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);


self_describing_task --include 'staging/hygiene/sfmc/events_clicks.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_clicks.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT count(*)
FROM hygiene_vault_mvp_dev_robin.sfmc.events_clicks ec;
SELECT count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks ec;


SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred ec
    QUALIFY count OVER (PARTITION BY ec.event_hash ORDER BY ec.created_at) > 1
ORDER BY event_hash;


DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.events_clicks;

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks;


--send events
SELECT count(*)
FROM hygiene_vault_mvp_dev_robin.sfmc.events_sends es;
--198,189,388
--send events deduped
SELECT count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends es;
--198,183,330

--open inferred events
SELECT count(*)
FROM hygiene_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred eopi;
-- open inferred events deduped
SELECT count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred eopi;

--click events
SELECT count(*)
FROM hygiene_vault_mvp_dev_robin.sfmc.events_clicks ec;
--click events deduped
SELECT count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks ec;


self_describing_task --include 'staging/hygiene/sfmc/events_sends.py'  --method 'run' --start '2020-09-01 00:00:00' --end '2020-09-01 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_sends.py'  --method 'run' --start '2020-09-01 00:00:00' --end '2020-09-01 00:00:00'

self_describing_task --include 'staging/hygiene/sfmc/events_opens_plus_inferred.py'  --method 'run' --start '2020-09-01 00:00:00' --end '2020-09-01 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_opens_plus_inferred.py'  --method 'run' --start '2020-09-01 00:00:00' --end '2020-09-01 00:00:00'

self_describing_task --include 'staging/hygiene/sfmc/events_clicks.py'  --method 'run' --start '2020-09-01 00:00:00' --end '2020-09-01 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_clicks.py'  --method 'run' --start '2020-09-01 00:00:00' --end '2020-09-01 00:00:00'

DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.events_sends;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends;


DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred;


DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.events_clicks;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks;


------------------------------------------------------------------------------------------------------------------------
SELECT get_ddl('table', 'raw_vault_mvp.sfmc.jobs_sources');


CREATE OR REPLACE TABLE jobs_sources CLUSTER BY (TO_DATE(schedule_tstamp))
(
    jobid          VARCHAR,
    subscriberkey  VARCHAR,
    datasourcename VARCHAR,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.jobs_sources CLONE raw_vault_mvp.sfmc.jobs_sources;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.jobs_sources js; --2020-07-27 15:00:21.040868000

self_describing_task --include 'staging/hygiene/sfmc/jobs_sources.py'  --method 'run' --start '2020-07-27 00:00:00' --end '2020-07-27 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/jobs_sources.py'  --method 'run' --start '2020-07-27 00:00:00' --end '2020-07-27 00:00:00'

DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.jobs_sources;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources;

SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.jobs_sources js;

------------------------------------------------------------------------------------------------------------------------


SELECT js.data_source_name,
       count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends es
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources js ON es.data_source_key = js.data_source_key
         LEFT JOIN
     where es.send_id = 1160337
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM raw_vault_mvp.crm_gsheets.crm_mapping_de;

SELECT *
FROM raw_vault_mvp.crm_gsheets.crm_mapping_uk;

SELECT *
FROM raw_vault_mvp.sfmc.jobs_list jl
WHERE jl.email_name = '20190103_NEWS_DE_THU_TRDE_1M'


------------------------------------------------------------------------------------------------------------------------
--backfill

SELECT *
FROM hygiene_vault_mvp.sfmc.events_sends;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_sends es; --2020-03-24 18:35:23.282174000
SELECT MIN(updated_at)
FROM hygiene_vault_mvp.sfmc.events_sends es; --2020-03-24 18:35:23.282174000
self_describing_task --include 'staging/hygiene/sfmc/events_sends.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_sends.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'
airflow backfill --start_date '2020-03-24 03:00:00' --end_date '2020-03-24 03:00:00' --task_regex '.*' hygiene_snapshots__sfmc__events_sends__daily_at_03h00
airflow backfill --start_date '2020-09-27 03:00:00' --end_date '2020-09-27 03:00:00' --task_regex '.*' -m hygiene_snapshots__sfmc__events_sends__daily_at_03h00

SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_opens_plus_inferred eopi; --2020-03-24 18:35:15.424981000
self_describing_task --include 'staging/hygiene/sfmc/events_opens_plus_inferred.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_opens_plus_inferred.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'
airflow backfill --start_date '2020-03-24 03:00:00' --end_date '2020-03-24 03:00:00' --task_regex '.*'  hygiene_snapshots__sfmc__events_opens_plus_inferred__daily_at_03h00
airflow backfill --start_date '2020-09-27 03:00:00' --end_date '2020-09-27 03:00:00' --task_regex '.*' -m hygiene_snapshots__sfmc__events_opens_plus_inferred__daily_at_03h00
SELECT count(*)
FROM raw_vault_mvp.sfmc.events_clicks ec; --2019-12-16 14:44:06.836756000

SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_clicks ec; --2019-12-16 14:44:06.836756000
self_describing_task --include 'staging/hygiene/sfmc/events_clicks.py'  --method 'run' --start '2019-12-16 00:00:00' --end '2019-12-16 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/events_clicks.py'  --method 'run' --start '2019-12-16 00:00:00' --end '2019-12-16 00:00:00'
airflow backfill --start_date '2019-12-16 03:00:00' --end_date '2019-12-16 03:00:00' --task_regex '.*' hygiene_snapshots__sfmc__events_clicks__daily_at_03h00
airflow backfill --start_date '2020-09-27 03:00:00' --end_date '2020-09-27 03:00:00' --task_regex '.*' -m hygiene_snapshots__sfmc__events_clicks__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------

SELECT get_ddl('table', 'raw_vault_mvp.sfmc.jobs_list');

CREATE OR REPLACE TABLE jobs_list CLUSTER BY (TO_DATE(schedule_tstamp))
(

    client_id                    NUMBER,
    send_id                      NUMBER,
    from_name                    VARCHAR,
    from_email                   VARCHAR,
    sched_time                   TIMESTAMP,
    sent_time                    TIMESTAMP,
    subject                      VARCHAR,
    email_name                   VARCHAR,
    triggered_send_external_key  VARCHAR,
    send_definition_external_key VARCHAR,
    job_status                   VARCHAR,
    preview_url                  VARCHAR,
    is_multipart                 VARCHAR,
    additional                   VARCHAR
        PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);

SELECT *
FROM raw_vault_mvp.sfmc.jobs_list;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.jobs_list CLONE raw_vault_mvp.sfmc.jobs_list;

self_describing_task --include 'staging/hygiene/sfmc/jobs_list.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfmc/jobs_list.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'

DROP TABLE hygiene_vault_mvp_dev_robin.sfmc.jobs_list;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list;

SELECT date_trunc(MONTH, es.event_date), count(*)
FROM hygiene_vault_mvp.sfmc.events_sends es
GROUP BY 1
ORDER BY 1;


------------------------------------------------------------------------------------------------------------------------

dataset_task \
--include 'crm_gsheets.crm_email_name_mapping*' \
--operation ExtractOperation \
--method 'run' \
--start '2020-09-27 00:00:00' \
--end '2020-09-27 00:00:00'

--load s3 to transient table
dataset_task \
--include 'crm_gsheets.crm_email_name_mapping*' \
--operation RawIngestOperation \
--method 'run' \
--start '2020-09-27 00:00:00' \
--end '2020-09-27 00:00:00'

--load transient table to raw_vault table
dataset_task \
--include 'crm_gsheets.crm_email_name_mapping*' \
--operation ProductionIngestOperation \
--method 'run' \
--start '2020-09-27 00:00:00' \
--end '2020-09-27 00:00:00'

SELECT *
FROM raw_vault_mvp_dev_robin.crm_gsheets.crm_email_name_mapping cenm;


self_describing_task --include 'dv/crm_gsheets_snapshots/crm_gsheets_snapshot_bulk.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.jobs_list;


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list;

SELECT min(jl.loaded_at)
FROM raw_vault_mvp.sfmc.jobs_list jl;
SELECT *
FROM hygiene_vault_mvp_dev_robin.sfmc.jobs_list jl
ORDER BY mapped_crm_date DESC NULLS LAST;

SELECT '20200000_UK_AME_BASKET_AlertV04' REGEXP '\\d{8}\\_.*';


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources js;

------------------------------------------------------------------------------------------------------------------------

dataset_task \
--include 'crm_gsheets.crm_historical_source_name*' \
--operation ExtractOperation \
--method 'run' \
--start '2020-09-27 00:00:00' \
--end '2020-09-27 00:00:00'

--load s3 to transient table
dataset_task \
--include 'crm_gsheets.crm_historical_source_name*' \
--operation RawIngestOperation \
--method 'run' \
--start '2020-09-27 00:00:00' \
--end '2020-09-27 00:00:00'

--load transient table to raw_vault table
dataset_task \
--include 'crm_gsheets.crm_historical_source_name*' \
--operation ProductionIngestOperation \
--method 'run' \
--start '2020-09-27 00:00:00' \
--end '2020-09-27 00:00:00'

SELECT *
FROM raw_vault_mvp_dev_robin.crm_gsheets.crm_historical_source_name;



SELECT *
FROM data_vault_mvp_dev_robin.crm_gsheets_snapshots.crm_historical_source_name_snapshot;
SELECT get_ddl('table', 'scratch.robinpatel.se_room_type_rooms_and_rates');



SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.jobs_list; -- 2020-03-24 18:35:15.002716000

airflow backfill --start_date '2020-03-24 03:00:00' --end_date '2020-03-24 03:00:00' --task_regex '.*' hygiene_snapshots__sfmc__jobs_list__daily_at_03h00
airflow backfill --start_date '2020-09-28 03:00:00' --end_date '2020-09-28 03:00:00' --task_regex '.*' -m hygiene_snapshots__sfmc__jobs_list__daily_at_03h00


SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.jobs_sources js; -- 2020-07-27 15:00:21.040868000

airflow backfill --start_date '2020-07-27 03:00:00' --end_date '2020-07-27 03:00:00' --task_regex '.*' hygiene_snapshots__sfmc__jobs_sources__daily_at_03h00
airflow backfill --start_date '2020-09-28 03:00:00' --end_date '2020-09-28 03:00:00' --task_regex '.*' -m hygiene_snapshots__sfmc__jobs_sources__daily_at_03h00


------------------------------------------------------------------------------------------------------------------------

SELECT MIN(event_tstamp)
FROM hygiene_vault_mvp.sfmc.events_sends;
SELECT MIN(event_tstamp)
FROM hygiene_vault_mvp.sfmc.events_opens_plus_inferred;
SELECT MIN(event_tstamp)
FROM hygiene_vault_mvp.sfmc.events_clicks
;
CREATE OR REPLACE TABLE scratch.robinpatel.events_sends CLONE hygiene_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TABLE scratch.robinpatel.events_opens_plus_inferred CLONE hygiene_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TABLE scratch.robinpatel.events_clicks CLONE hygiene_vault_mvp.sfmc.events_clicks;

CREATE OR REPLACE TABLE scratch.robinpatel.events_sends_snapshot CLONE hygiene_snapshot_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends CLONE hygiene_snapshot_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TABLE scratch.robinpatel.events_opens_plus_inferred_snapshot CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TABLE scratch.robinpatel.events_clicks_snapshot CLONE hygiene_snapshot_vault_mvp.sfmc.events_clicks;

DELETE
FROM hygiene_vault_mvp.sfmc.events_sends
WHERE event_tstamp < '2018-01-01';
DELETE
FROM hygiene_vault_mvp.sfmc.events_opens_plus_inferred
WHERE event_tstamp < '2018-01-01';
DELETE
FROM hygiene_vault_mvp.sfmc.events_clicks
WHERE event_tstamp < '2018-01-01';

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM data_vault_mvp_dev_robin.crm_gsheets_snapshots.crm_historical_source_name_snapshot;

SELECT DISTINCT
       es.shiro_user_id,
       es.send_id,
       es.subscriber_key,
       chsns.dataset_source AS data_source_name
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends es
         INNER JOIN data_vault_mvp_dev_robin.crm_gsheets_snapshots.crm_historical_source_name_snapshot chsns
                    ON es.send_id = chsns.send_id AND es.list_id = chsns.list_id;


SELECT DISTINCT
       es.send_id,
       es.list_id,
       js.data_source_name
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends es
         INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources js
                    ON es.send_id = js.send_id AND es.subscriber_key = js.subscriber_key;


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources js;


self_describing_task --include 'dv/dwh/email/email_segments.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data.crm_email_segments__step05__model_data;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.crm_email_segments;

DROP TABLE data_vault_mvp_dev_robin.dwh.crm_email_segments;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot;

SELECT min(hras.view_date)
FROM data_vault_mvp.dwh.hotel_room_availability_snapshot hras;

SELECT MIN(loaded_at)
FROM data_vault_mvp.crm_gsheets_snapshots.crm_email_name_mapping_snapshot cenms; --2020-10-01 00:04:06.303213000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.sfmc.jobs_sources; -- 2020-10-01 10:09:39.476000000

airflow backfill --start_date '2020-09-30 04:00:00' --end_date '2020-09-30 04:00:00' --task_regex '.*' dwh__crm_email_segments__daily_at_04h00

SELECT es.event_hash,
       es.event_date,
       es.event_tstamp,
       es.data_source_key,
       es.shiro_user_id,
       es.client_id,
       es.send_id,
       es.subscriber_key,
       es.email_address,
       es.subscriber_id,
       es.list_id,
       es.event_date__o,
       es.event_type,
       es.batch_id,
       es.triggered_send_external_key
FROM hygiene_snapshot_vault_mvp.sfmc.events_sends es;

SELECT eopi.event_hash,
       eopi.event_date,
       eopi.event_tstamp,
       eopi.data_source_key,
       eopi.shiro_user_id,
       eopi.client_id,
       eopi.send_id,
       eopi.subscriber_key,
       eopi.email_address,
       eopi.subscriber_id,
       eopi.list_id,
       eopi.event_date__o,
       eopi.event_type,
       eopi.batch_id,
       eopi.triggered_send_external_key,
       eopi.ip_address,
       eopi.country,
       eopi.region,
       eopi.city,
       eopi.latitude,
       eopi.longitude,
       eopi.metrocode,
       eopi.area_code,
       eopi.browser,
       eopi.email_client,
       eopi.operating_system,
       eopi.device
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eopi;

SELECT ec.event_hash,
       ec.event_date,
       ec.event_tstamp,
       ec.data_source_key,
       ec.shiro_user_id,
       ec.client_id,
       ec.send_id,
       ec.subscriber_key,
       ec.email_address,
       ec.subscriber_id,
       ec.list_id,
       ec.event_date__o,
       ec.event_type,
       ec.send_url_id,
       ec.url_id,
       ec.url,
       ec.alias,
       ec.batch_id,
       ec.triggered_send_external_key,
       ec.ip_address,
       ec.country,
       ec.region,
       ec.city,
       ec.latitude,
       ec.longitude,
       ec.metrocode,
       ec.area_code,
       ec.browser,
       ec.email_client,
       ec.operating_system,
       ec.device
FROM hygiene_snapshot_vault_mvp.sfmc.events_clicks ec;


SELECT jl.send_id,
       jl.scheduled_date,
       jl.scheduled_tstmap,
       jl.sent_date,
       jl.sent_tstamp,
       jl.email_name,
       jl.is_email_name_remapped,
       jl.mapped_crm_date,
       jl.mapped_territory,
       jl.mapped_objective,
       jl.mapped_platform,
       jl.mapped_campaign,
       jl.client_id,
       jl.from_name,
       jl.from_email,
       jl.sched_time,
       jl.sent_time,
       jl.subject,
       jl.email_name__o,
       jl.triggered_send_external_key,
       jl.send_definition_external_key,
       jl.job_status,
       jl.preview_url,
       jl.is_multipart,
       jl.additional
FROM hygiene_snapshot_vault_mvp.sfmc.jobs_list jl;



SELECT es.send_id,
       es.list_id,
       es.data_source_name,
       es.segment,
       es.mapping_type
FROM data_vault_mvp.dwh.crm_email_segments es;

self_describing_task --include 'se/data_pii/crm/create_se_data_pii_objects_crm.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00' --upstream

SELECT *
FROM data_vault_mvp_dev_robin.dwh.crm_email_segments;

WITH sends AS (
    SELECT s.email_segment_key,
           count(*) AS sends
    FROM se_dev_robin.data_pii.crm_events_sends s
    GROUP BY 1
),
     opens AS (
         SELECT o.email_segment_key,
                count(*) AS opens
         FROM se_dev_robin.data_pii.crm_events_opens o
         GROUP BY 1
     ),
     clicks AS (
         SELECT c.email_segment_key,
                count(*) AS clicks
         FROM se_dev_robin.data_pii.crm_events_clicks c
         GROUP BY 1
     )

SELECT cjl.send_id,
       cjl.email_name,
       cjl.mapped_crm_date,
       cjl.mapped_territory,
       cjl.mapped_objective,
       cjl.mapped_platform,
       cjl.mapped_campaign,
       ces.data_source_name,
       ces.segment,
       es.sends,
       eo.opens,
       ec.clicks
FROM se_dev_robin.data_pii.crm_jobs_list cjl
         LEFT JOIN se_dev_robin.data_pii.crm_email_segments ces ON cjl.send_id = ces.send_id
         LEFT JOIN sends es ON ces.email_segment_key = es.email_segment_key
         LEFT JOIN opens eo ON ces.email_segment_key = eo.email_segment_key
         LEFT JOIN clicks ec ON ces.email_segment_key = ec.email_segment_key
WHERE cjl.send_id = 1160337;


SELECT cjl.send_id,
       cjl.email_name,
       cjl.mapped_crm_date,
       cjl.mapped_territory,
       cjl.mapped_objective,
       cjl.mapped_platform,
       cjl.mapped_campaign,
       ces.data_source_name,
       ces.segment,
       count(*) AS sends
FROM se_dev_robin.data_pii.crm_events_sends c
         LEFT JOIN se_dev_robin.data_pii.crm_email_segments ces ON ces.email_segment_key = c.email_segment_key
         LEFT JOIN se_dev_robin.data_pii.crm_jobs_list cjl ON cjl.send_id = ces.send_id
WHERE c.send_id = 1160337
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;


------------------------------------------------------------------------------------------------------------------------
--for model
SELECT ces.email_segment_key,
       ces.event_date,
       count(*)
FROM se.data_pii.crm_events_sends ces
GROUP BY 1, 2;

SELECT ceo.email_segment_key,
       ceo.event_date,
       count(*) AS opens
FROM se.data_pii.crm_events_opens ceo
GROUP BY 1, 2;

SELECT cec.email_segment_key,
       cec.event_date,
       count(*) AS clicks
FROM se.data_pii.crm_events_clicks cec
GROUP BY 1, 2;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data_pii.crm_email_segments es
         CROSS JOIN se.data.se_calendar sc
WHERE sc.date_value >= '2019-01-01'
  AND sc.date_value <= CURRENT_DATE


WITH sends AS (
    SELECT ces.email_segment_key,
           ces.event_date,
           count(*) AS sends
    FROM se.data_pii.crm_events_sends ces
    WHERE ces.event_date >= '2018-01-01'
    GROUP BY 1, 2
),
     opens AS (
         SELECT ceo.email_segment_key,
                ceo.event_date,
                count(*) AS opens
         FROM se.data_pii.crm_events_opens ceo
         WHERE ceo.event_date >= '2018-01-01'
         GROUP BY 1, 2
     ),
     clicks AS (

         SELECT cec.email_segment_key,
                cec.event_date,
                count(*) AS clicks
         FROM se.data_pii.crm_events_clicks cec
         WHERE cec.event_date >= '2018-01-01'
         GROUP BY 1, 2
     )

SELECT COALESCE(s.email_segment_key, o.email_segment_key, c.email_segment_key) AS email_segment_key,
       COALESCE(s.event_date, o.event_date, c.event_date)                      AS event_date,
       s.sends,
       o.opens,
       c.clicks
FROM sends s
         FULL OUTER JOIN opens o ON s.email_segment_key = o.email_segment_key
    AND s.event_date = o.event_date
         FULL OUTER JOIN clicks c ON COALESCE(s.email_segment_key, o.email_segment_key) = c.email_segment_key
    AND COALESCE(s.event_date, o.event_date) = c.event_date;


SELECT *
FROM se.data_pii.crm_jobs_list cjl
         LEFT JOIN se.data_pii.crm_email_segments ces ON cjl.send_id = ces.send_id
WHERE ces.send_id = 1147187;

SELECT *
FROM se.data_pii.crm_email_segments ces
WHERE ces.send_id = 1147187;

SELECT *
FROM raw_vault_mvp.crm_gsheets.crm_historical_source_name chsn
WHERE send_id = 1147179;

SELECT *
FROM select get_ddl('table', 'se.data_pii.crm_jobs_list');

airflow backfill --start_date '2020-04-07 00:00:00' --end_date '2020-04-07 00:00:00' --task_regex '.*' incoming__cms_mysql__booking__hourly

SELECT SUM(sb.margin_gross_of_toms_gbp_constant_currency) AS total_margin
FROM se.data.se_booking sb
         LEFT JOIN se.data.dim_sale ds ON sb.sale_id = ds.se_sale_id
WHERE UPPER(sb.booking_status) = 'COMPLETE'
  AND ds.product_configuration = 'Hotel'
  AND to_date(sb.booking_completed_date) >= '2019-01-01'
  AND to_date(sb.booking_completed_date) <= '2019-12-31' airflow backfill --start_date '2020-10-01 04:00:00' --end_date '2020-10-01 04:00:00' --task_regex '.*' dwh__crm_email_segments__daily_at_04h00


CREATE TABLE scratch.robinpatel.promotion
(
    id           BIGINT PRIMARY KEY,
    version      BIGINT       NOT NULL,
    active       BIT          NOT NULL,
    date_created DATETIME     NOT NULL,
    end          DATETIME     NOT NULL,
    last_updated DATETIME     NOT NULL,
    name         VARCHAR(255) NULL,
    start        DATETIME     NOT NULL
);

