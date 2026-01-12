USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATELDEV35777;


select *
from data_vault_mvp_dev_carmen.reconciliation.key_metrics_rec
limit 10;

select *
from scratch.robinpateldev35777.key_metrics_rec
limit 10;

CREATE TABLE scratch.robinpateldev35777.key_metrics_rec CLONE data_vault_mvp_dev_carmen.reconciliation.key_metrics_rec;

SELECT EVENT_DATE,
       SPVS_CMS_REPORTS
FROM KEY_METRICS_REC
WHERE EVENT_DATE >= '2019-01-01'
ORDER BY 1

SELECT COUNT(*)
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.MONGO__EVENTS_COLLECTION
WHERE EVENT_DATE::DATE = '2018-04-07'; -- 1724712

SELECT COUNT(*)
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.SNOWPLOW__ATOMIC_EVENTS
WHERE EVENT_DATE::DATE = '2018-04-07';
-- 1333413


--mongo events
SELECT USER_ID,
       EVENT_DATE,
       TERRITORY,
       SALE_ID,
       PAGE_URL
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.MONGO__EVENTS_COLLECTION
WHERE EVENT_DATE::DATE = '2018-04-07'
ORDER BY PAGE_URL;

--snowplow vanilla
SELECT USER_ID,
       EVENT_DATE,
       TERRITORY,
       SALE_ID,
       PAGE_URL
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.SNOWPLOW__ATOMIC_EVENTS
WHERE EVENT_DATE::DATE = '2018-04-07'
ORDER BY PAGE_URL;

SELECT *
FROM SNOWPLOW.atomic.events
WHERE ETL_TSTAMP::DATE = '2019-11-12'
LIMIT 10;

------------------------------------------------------------------------------------------------------------------------
--check distinct users

select url_string,
       event_date,
       users
from (
         select 'mongo'::varchar                as source,
                URL_STRING,
                date_trunc('day', EVENT_TSTAMP) as event_date,
                count(distinct user_id)         as users
         from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection
         group by 1, 2, 3
         union all
         select 'snowplow'::varchar                 as source,
                affiliate_url_string                as URL_STRING,
                date_trunc('day', COLLECTOR_TSTAMP) as event_date,
                count(distinct user_id)             as users
         from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
         group by 1, 2, 3
     )
where event_date = '2019-04-14'
;

--identified that there's a clear discrepancy between spvs on the 14th of apr 2019.
set date_variable = '2019-04-14';

------------------------------------------------------------------------------------------------------------------------
--users by url_string
WITH grain AS (
    SELECT DISTINCT EVENT_TSTAMP::DATE AS event_date,
                    URL_STRING
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection

    UNION

    SELECT DISTINCT COLLECTOR_TSTAMP::DATE,
                    affiliate_url_string
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
)
   , mongo as (
    select 'mongo'::varchar        as source,
           URL_STRING,
           EVENT_TSTAMP::DATE      as event_date,
           count(distinct user_id) as users,
           count(1)                as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)
   , snowplow as (
    select 'snowplow'::varchar     as source,
           affiliate_url_string    as URL_STRING,
           COLLECTOR_TSTAMP::DATE  as event_date,
           count(distinct user_id) as users,
           count(1)                as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)

SELECT g.event_date,
       g.URL_STRING,
       COALESCE(m.users, 0)                        AS mongo_users,
       COALESCE(s.users, 0)                        AS snowplow_users,
       COALESCE(m.users, 0) - COALESCE(s.users, 0) AS mongo_snowplow_diff,
       (m.users / NULLIF(s.users, 0) - 1) * 100    AS mongo_snowplow_users_variance

--        COALESCE(m.spvs, 0)                      AS mongo_spvs,
--        COALESCE(s.spvs, 0)                      AS snowplow_spvs,
--        COALESCE(m.spvs, 0) - NULLIF(s.spvs, 0) AS mongo_snowplow_diff,
--        m.spvs / NULLIF(s.spvs, 0) - 1 AS mongo_snowplow_spvs_variance


FROM grain g
         LEFT JOIN mongo m ON g.event_date = m.event_date AND g.URL_STRING = m.URL_STRING
         LEFT JOIN snowplow s ON g.event_date = s.event_date AND g.URL_STRING = s.URL_STRING
where g.event_date = $date_variable
ORDER BY ABS(COALESCE(m.users, 0) - COALESCE(s.users, 0))
             *
         ABS(COALESCE(m.users / NULLIF(s.users, 0) - 1, 1)) DESC;

------------------------------------------------------------------------------------------------------------------------
--spvs by url_string
WITH grain AS (
    SELECT DISTINCT EVENT_TSTAMP::DATE AS event_date,
                    URL_STRING
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection

    UNION

    SELECT DISTINCT COLLECTOR_TSTAMP::DATE,
                    affiliate_url_string
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
)
   , mongo as (
    select 'mongo'::varchar   as source,
           URL_STRING,
           EVENT_TSTAMP::DATE as event_date,
           count(1)           as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)
   , snowplow as (
    select 'snowplow'::varchar    as source,
           affiliate_url_string   as URL_STRING,
           COLLECTOR_TSTAMP::DATE as event_date,
           count(1)               as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)

SELECT g.event_date,
       g.URL_STRING,
       COALESCE(m.spvs, 0)                       AS mongo_spvs,
       COALESCE(s.spvs, 0)                       AS snowplow_spvs,
       COALESCE(m.spvs, 0) - COALESCE(s.spvs, 0) AS mongo_snowplow_diff,
       (m.spvs / NULLIF(s.spvs, 0) - 1) * 100    AS mongo_snowplow_spvs_variance

--        COALESCE(m.spvs, 0)                      AS mongo_spvs,
--        COALESCE(s.spvs, 0)                      AS snowplow_spvs,
--        COALESCE(m.spvs, 0) - NULLIF(s.spvs, 0) AS mongo_snowplow_diff,
--        m.spvs / NULLIF(s.spvs, 0) - 1 AS mongo_snowplow_spvs_variance


FROM grain g
         LEFT JOIN mongo m ON g.event_date = m.event_date AND g.URL_STRING = m.URL_STRING
         LEFT JOIN snowplow s ON g.event_date = s.event_date AND g.URL_STRING = s.URL_STRING
where g.event_date = $date_variable
ORDER BY ABS(COALESCE(m.spvs, 0) - COALESCE(s.spvs, 0))
             *
         ABS(COALESCE(m.spvs / NULLIF(s.spvs, 0) - 1, 1)) DESC;

------------------------------------------------------------------------------------------------------------------------
--users by url path
WITH grain AS (
    SELECT DISTINCT EVENT_TSTAMP::DATE AS event_date,
                    PAGE_URL
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection

    UNION

    SELECT DISTINCT COLLECTOR_TSTAMP::DATE,
                    PAGE_URL
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
)
   , mongo as (
    select 'mongo'::varchar        as source,
           PAGE_URL,
           EVENT_TSTAMP::DATE      as event_date,
           count(distinct user_id) as users
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)
   , snowplow as (
    select 'snowplow'::varchar     as source,
           affiliate_url_string    as PAGE_URL,
           COLLECTOR_TSTAMP::DATE  as event_date,
           count(distinct user_id) as users
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)

SELECT g.event_date,
       g.PAGE_URL,
       COALESCE(m.users, 0)                        AS mongo_users,
       COALESCE(s.users, 0)                        AS snowplow_users,
       COALESCE(m.users, 0) - COALESCE(s.users, 0) AS mongo_snowplow_diff,
       (m.users / NULLIF(s.users, 0) - 1) * 100    AS mongo_snowplow_users_variance

--        COALESCE(m.spvs, 0)                      AS mongo_spvs,
--        COALESCE(s.spvs, 0)                      AS snowplow_spvs,
--        COALESCE(m.spvs, 0) - NULLIF(s.spvs, 0) AS mongo_snowplow_diff,
--        m.spvs / NULLIF(s.spvs, 0) - 1 AS mongo_snowplow_spvs_variance


FROM grain g
         LEFT JOIN mongo m ON g.event_date = m.event_date AND g.PAGE_URL = m.PAGE_URL
         LEFT JOIN snowplow s ON g.event_date = s.event_date AND g.PAGE_URL = s.PAGE_URL
where g.event_date = $date_variable
ORDER BY ABS(COALESCE(m.users, 0) - COALESCE(s.users, 0))
             *
         ABS(COALESCE(m.users / NULLIF(s.users, 0) - 1, 1)) DESC;

------------------------------------------------------------------------------------------------------------------------
--spvs by page url
WITH grain AS (
    SELECT DISTINCT EVENT_TSTAMP::DATE AS event_date,
                    PAGE_URL
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection

    UNION

    SELECT DISTINCT COLLECTOR_TSTAMP::DATE,
                    PAGE_URL
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
)
   , mongo as (
    select 'mongo'::varchar   as source,
           PAGE_URL,
           EVENT_TSTAMP::DATE as event_date,
           count(1)           as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)
   , snowplow as (
    select 'snowplow'::varchar    as source,
           PAGE_URL,
           COLLECTOR_TSTAMP::DATE as event_date,
           count(1)               as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
    where IS_SALE_PAGE = 1                   -- is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)

SELECT g.event_date,
       g.PAGE_URL,
       COALESCE(m.spvs, 0)                       AS mongo_spvs,
       COALESCE(s.spvs, 0)                       AS snowplow_spvs,
       COALESCE(m.spvs, 0) - COALESCE(s.spvs, 0) AS mongo_snowplow_diff,
       (m.spvs / NULLIF(s.spvs, 0) - 1) * 100    AS mongo_snowplow_spvs_variance


FROM grain g
         LEFT JOIN mongo m ON g.event_date = m.event_date AND g.PAGE_URL = m.PAGE_URL
         LEFT JOIN snowplow s ON g.event_date = s.event_date AND g.PAGE_URL = s.PAGE_URL
where g.event_date = $date_variable
ORDER BY ABS(COALESCE(m.spvs, 0) - COALESCE(s.spvs, 0))
             *
         ABS(COALESCE(m.spvs / NULLIF(s.spvs, 0) - 1, 1)) DESC;

------------------------------------------------------------------------------------------------------------------------

--spvs by sale id
WITH grain AS (
    SELECT DISTINCT EVENT_TSTAMP::DATE AS event_date,
                    SALE_ID
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection

    UNION

    SELECT DISTINCT COLLECTOR_TSTAMP::DATE,
                    SALE_ID
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
)
   , mongo as (
    select 'mongo'::varchar   as source,
           SALE_ID,
           EVENT_TSTAMP::DATE as event_date,
           count(1)           as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.mongo__events_collection
    where IS_SALE_PAGE = 1                   --is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)
   , snowplow as (
    select 'snowplow'::varchar    as source,
           SALE_ID,
           COLLECTOR_TSTAMP::DATE as event_date,
           count(1)               as spvs
    from data_vault_mvp_dev_carmen.key_metrics_rec_stg.snowplow__atomic_events
    where IS_SALE_PAGE = 1                   -- is spv
      AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
    group by 1, 2, 3
)

SELECT g.event_date,
       g.SALE_ID,
       COALESCE(m.spvs, 0)                       AS mongo_spvs,
       COALESCE(s.spvs, 0)                       AS snowplow_spvs,
       COALESCE(m.spvs, 0) - COALESCE(s.spvs, 0) AS mongo_snowplow_diff,
       (m.spvs / NULLIF(s.spvs, 0) - 1) * 100    AS mongo_snowplow_spvs_variance


FROM grain g
         LEFT JOIN mongo m ON g.event_date = m.event_date AND g.SALE_ID = m.SALE_ID
         LEFT JOIN snowplow s ON g.event_date = s.event_date AND g.SALE_ID = s.SALE_ID
where g.event_date = $date_variable
ORDER BY ABS(COALESCE(m.spvs, 0) - COALESCE(s.spvs, 0))
             *
         ABS(COALESCE(m.spvs / NULLIF(s.spvs, 0) - 1, 1)) DESC
;

------------------------------------------------------------------------------------------------------------------------

set url_variable ='https://www.secretescapes.com/traumhafter-yachturlaub-in-kroatien-trogir-split-hvar-mljet-dubrovnik-korcula-brac/sale';
set url_variable ='https://www.secretescapes.de/nur-heute-hideaway-im-inselparadies-seasense-boutique-hotel-and-spa-belle-mare-mauritius/sale';
set sale_id_variable ='91948';

SELECT count(*)                as spvs,
       count(distinct USER_ID) AS users
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.MONGO__EVENTS_COLLECTION
WHERE PAGE_URL = $url_variable
  AND EVENT_TSTAMP::DATE = $date_variable
  AND IS_SALE_PAGE = 1 -- is spv
  AND PAGE_HOST_BRAND = 'Secret Escapes'-- is se core
;

SELECT *
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.MONGO__EVENTS_COLLECTION
WHERE PAGE_URL = $url_variable
  AND EVENT_TSTAMP::DATE = $date_variable
  AND IS_SALE_PAGE = 1 -- is spv
  AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
;

SELECT DATE_TRUNC(hour, event_tstamp) as hour,
       count(1)                       as spvs
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.MONGO__EVENTS_COLLECTION
WHERE PAGE_URL = $url_variable
  AND EVENT_TSTAMP::DATE = $date_variable
  AND IS_SALE_PAGE = 1                   -- is spv
  AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
group by 1;
;

SELECT *
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.MONGO__EVENTS_COLLECTION
WHERE SALE_ID = $sale_id_variable
  AND EVENT_TSTAMP::DATE = $date_variable
  AND IS_SALE_PAGE = 1 -- is spv
  AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
;

SELECT EVENT_TSTAMP::DATE,
       COUNT(1) as SPVS
FROM DATA_VAULT_MVP_DEV_CARMEN.KEY_METRICS_REC_STG.MONGO__EVENTS_COLLECTION
WHERE PAGE_HOST_TERRITORY != TERRITORY
  AND IS_SALE_PAGE = 1                   -- is spv
  AND PAGE_HOST_BRAND = 'Secret Escapes' -- is se core
  AND referrer IS DISTINCT FROM 'app_referrer'
GROUP BY 1
ORDER BY 1;

