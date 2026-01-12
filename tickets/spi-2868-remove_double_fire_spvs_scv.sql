USE WAREHOUSE pipe_2xlarge;
--backup
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs_20220908 CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

--clone local, used this as opportunity to create persisted table.
CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
(
    -- (lineage) metadata for the current job
    schedule_tstamp   TIMESTAMP,
    run_tstamp        TIMESTAMP,
    operation_id      VARCHAR,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,

    event_hash        VARCHAR,
    touch_id          VARCHAR,
    event_tstamp      TIMESTAMP,
    se_sale_id        VARCHAR,
    event_category    VARCHAR,
    event_subcategory VARCHAR,
    page_url          VARCHAR
)
    CLUSTER BY (event_tstamp::DATE)
;

INSERT INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
SELECT
    mts.schedule_tstamp,
    mts.run_tstamp,
    mts.operation_id,
    mts.created_at,
    mts.updated_at,
    mts.event_hash,
    mts.touch_id,
    mts.event_tstamp,
    mts.se_sale_id,
    mts.event_category,
    mts.event_subcategory,
    mts.page_url
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts;

-- run 4 delete scripts without incremental filter

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
    USING (
         WITH touches_in_tests AS (
             --identify all sessions that have any event that includes the redirect test parameter
             SELECT DISTINCT
                 sts.touch_id
             FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
             WHERE sts.page_url REGEXP '.*gce_rbf=\\d' -- events that are within the test
--              AND sts.updated_at >= TIMESTAMPADD('day', -1, '2022-09-07 03:00:00'::TIMESTAMP)
         ),
              calc_dedupe_parameters AS (
                  SELECT
                      s.event_hash,
                      s.touch_id,
                      s.event_tstamp,
                      --remove the param from url for partitioning
                      REGEXP_REPLACE(s.page_url, '[&|\\?]gce_rbf=\\d')                       AS stripped_url,
                      --used to compare if two consecutive events are for the same url without the test param
                      LAG(s.page_url) OVER (PARTITION BY s.touch_id ORDER BY s.event_tstamp) AS prev_url,
                      LAG(s.event_tstamp) OVER (PARTITION BY s.touch_id
                          ORDER BY s.event_tstamp)                                           AS prev_event_tstamp,
                      -- investigated differences in time between double fires,
                      -- found an average variance of <1 second, put additional
                      -- +1 second as buffer in case of slow load times.
                      -- work out if the difference between 2 events is within 2 seconds
                      -- if its less than 2 seconds use the earlier of the two for both events
                      -- to allow for partitioning.
                      -- check that the previous event is also for the same url
                      IFF(TIMEDIFF(SECONDS, prev_event_tstamp, s.event_tstamp) <= 2
                              AND stripped_url = prev_url
                              AND s.page_url IS DISTINCT FROM prev_url,
                          prev_event_tstamp,
                          s.event_tstamp)                                                    AS deduped_tstamp,
                      s.se_sale_id
                  FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
                  WHERE s.touch_id IN (
                      SELECT
                          tt.touch_id
                      FROM touches_in_tests tt
                  )
              )
         SELECT
             cdp.event_hash
         FROM calc_dedupe_parameters cdp
             -- dedupe to return the earliest event (in a double fire) which doesn't contain the
             -- test url parm. The result of this will feed into a delete function so it will
             -- delete the initial event, the one without the url param.
             QUALIFY ROW_NUMBER() OVER (PARTITION BY
                 cdp.touch_id,
                 cdp.deduped_tstamp,
                 cdp.se_sale_id,
                 cdp.stripped_url
                 ORDER BY cdp.event_tstamp DESC) != 1
     ) AS batch
WHERE target.event_hash = batch.event_hash;
-- query id: 01a6d4d0-3201-d71d-0000-02ddccfa4d76
-- 6,403,247 rows deleted

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
    USING (
         WITH touches_in_tests AS (
             --identify all sessions that have any event that includes the redirect test parameter
             SELECT DISTINCT
                 sts.touch_id
             FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
             WHERE sts.page_url REGEXP '.*gce_pbf=\\d' -- events that are within the test
--                AND sts.updated_at >= TIMESTAMPADD('day', -1, '2022-09-07 03:00:00'::TIMESTAMP)
         ),
              calc_dedupe_parameters AS (
                  SELECT
                      s.event_hash,
                      s.touch_id,
                      s.event_tstamp,
                      --remove the param from url for partitioning
                      REGEXP_REPLACE(s.page_url, '[&|\\?]gce_pbf=\\d')                       AS stripped_url,
                      --used to compare if two consecutive events are for the same url without the test param
                      LAG(s.page_url) OVER (PARTITION BY s.touch_id ORDER BY s.event_tstamp) AS prev_url,
                      LAG(s.event_tstamp) OVER (PARTITION BY s.touch_id
                          ORDER BY s.event_tstamp)                                           AS prev_event_tstamp,
                      -- investigated differences in time between double fires,
                      -- found an average variance of <1 second, put additional
                      -- +1 second as buffer in case of slow load times.
                      -- work out if the difference between 2 events is within 2 seconds
                      -- if its less than 2 seconds use the earlier of the two for both events
                      -- to allow for partitioning.
                      -- check that the previous event is also for the same url
                      IFF(TIMEDIFF(SECONDS, prev_event_tstamp, s.event_tstamp) <= 2
                              AND stripped_url = prev_url
                              AND s.page_url IS DISTINCT FROM prev_url,
                          prev_event_tstamp,
                          s.event_tstamp)                                                    AS deduped_tstamp,
                      s.se_sale_id
                  FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
                  WHERE s.touch_id IN (
                      SELECT
                          tt.touch_id
                      FROM touches_in_tests tt
                  )
              )
         SELECT
             cdp.event_hash
         FROM calc_dedupe_parameters cdp
             -- dedupe to return the earliest event (in a double fire) which doesn't contain the
             -- test url parm. The result of this will feed into a delete function so it will
             -- delete the initial event, the one without the url param.
             QUALIFY ROW_NUMBER() OVER (PARTITION BY
                 cdp.touch_id,
                 cdp.deduped_tstamp,
                 cdp.se_sale_id,
                 cdp.stripped_url
                 ORDER BY cdp.event_tstamp DESC) != 1
     ) AS batch
WHERE target.event_hash = batch.event_hash;
-- query id: 01a6d4d1-3201-d7a0-0000-02ddccfa6f86
-- 2,332,332 rows deleted

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
    USING (
         WITH touches_in_tests AS (
             --identify all sessions that have any event that includes the redirect test parameter
             SELECT DISTINCT
                 sts.touch_id
             FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
             WHERE sts.page_url REGEXP '.*gce_perbfee=\\d' -- events that are within the test
--                AND sts.updated_at >= TIMESTAMPADD('day', -1, '2022-09-07 03:00:00'::TIMESTAMP)
         ),
              calc_dedupe_parameters AS (
                  SELECT
                      s.event_hash,
                      s.touch_id,
                      s.event_tstamp,
                      --remove the param from url for partitioning
                      REGEXP_REPLACE(s.page_url, '[&|\\?]gce_perbfee=\\d')                   AS stripped_url,
                      --used to compare if two consecutive events are for the same url without the test param
                      LAG(s.page_url) OVER (PARTITION BY s.touch_id ORDER BY s.event_tstamp) AS prev_url,
                      LAG(s.event_tstamp) OVER (PARTITION BY s.touch_id
                          ORDER BY s.event_tstamp)                                           AS prev_event_tstamp,
                      -- investigated differences in time between double fires,
                      -- found an average variance of <1 second, put additional
                      -- +1 second as buffer in case of slow load times.
                      -- work out if the difference between 2 events is within 2 seconds
                      -- if its less than 2 seconds use the earlier of the two for both events
                      -- to allow for partitioning.
                      -- check that the previous event is also for the same url
                      IFF(TIMEDIFF(SECONDS, prev_event_tstamp, s.event_tstamp) <= 2
                              AND stripped_url = prev_url
                              AND s.page_url IS DISTINCT FROM prev_url,
                          prev_event_tstamp,
                          s.event_tstamp)                                                    AS deduped_tstamp,
                      s.se_sale_id
                  FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
                  WHERE s.touch_id IN (
                      SELECT
                          tt.touch_id
                      FROM touches_in_tests tt
                  )
              )
         SELECT
             cdp.event_hash
         FROM calc_dedupe_parameters cdp
             -- dedupe to return the earliest event (in a double fire) which doesn't contain the
             -- test url parm. The result of this will feed into a delete function so it will
             -- delete the initial event, the one without the url param.
             QUALIFY ROW_NUMBER() OVER (PARTITION BY
                 cdp.touch_id,
                 cdp.deduped_tstamp,
                 cdp.se_sale_id,
                 cdp.stripped_url
                 ORDER BY cdp.event_tstamp DESC) != 1
     ) AS batch
WHERE target.event_hash = batch.event_hash;
-- query id: 01a6d4d3-3201-d7a0-0000-02ddccfa95ba
-- 1,558,029 rows deleted

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
    USING (
         WITH touches_in_tests AS (
             --identify all sessions that have any event that includes the redirect test parameter
             SELECT DISTINCT
                 sts.touch_id
             FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
             WHERE sts.page_url REGEXP '.*gce_hpt=\\d' -- events that are within the test
--                AND sts.updated_at >= TIMESTAMPADD('day', -1, '2022-09-07 03:00:00'::TIMESTAMP)
         ),
              calc_dedupe_parameters AS (
                  SELECT
                      s.event_hash,
                      s.touch_id,
                      s.event_tstamp,
                      --remove the param from url for partitioning
                      REGEXP_REPLACE(s.page_url, '[&|\\?]gce_hpt=\\d')                       AS stripped_url,
                      --used to compare if two consecutive events are for the same url without the test param
                      LAG(s.page_url) OVER (PARTITION BY s.touch_id ORDER BY s.event_tstamp) AS prev_url,
                      LAG(s.event_tstamp) OVER (PARTITION BY s.touch_id
                          ORDER BY s.event_tstamp)                                           AS prev_event_tstamp,
                      -- investigated differences in time between double fires,
                      -- found an average variance of <1 second, put additional
                      -- +1 second as buffer in case of slow load times.
                      -- work out if the difference between 2 events is within 2 seconds
                      -- if its less than 2 seconds use the earlier of the two for both events
                      -- to allow for partitioning.
                      -- check that the previous event is also for the same url
                      IFF(TIMEDIFF(SECONDS, prev_event_tstamp, s.event_tstamp) <= 2
                              AND stripped_url = prev_url
                              AND s.page_url IS DISTINCT FROM prev_url,
                          prev_event_tstamp,
                          s.event_tstamp)                                                    AS deduped_tstamp,
                      s.se_sale_id
                  FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
                  WHERE s.touch_id IN (
                      SELECT
                          tt.touch_id
                      FROM touches_in_tests tt
                  )
              )
         SELECT
             cdp.event_hash
         FROM calc_dedupe_parameters cdp
             -- dedupe to return the earliest event (in a double fire) which doesn't contain the
             -- test url parm. The result of this will feed into a delete function so it will
             -- delete the initial event, the one without the url param.
             QUALIFY ROW_NUMBER() OVER (PARTITION BY
                 cdp.touch_id,
                 cdp.deduped_tstamp,
                 cdp.se_sale_id,
                 cdp.stripped_url
                 ORDER BY cdp.event_tstamp DESC) != 1
     ) AS batch
WHERE target.event_hash = batch.event_hash;
-- query id: 01a6d4d4-3201-d7a0-0000-02ddccfa9aca
-- 322,296 rows deleted

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;

GRANT SELECT ON TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs TO ROLE data_team_basic;
