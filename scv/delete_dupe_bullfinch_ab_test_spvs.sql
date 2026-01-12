CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

SELECT MIN(updated_at)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs
WHERE page_url LIKE '%ff_bullfinch%'; --2021-11-23 04:17:16.626000000

USE WAREHOUSE pipe_xlarge;

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
    USING (
         WITH touches_in_tests AS (
             --identify all sessions that have any event that includes the redirect test parameter
             SELECT DISTINCT sts.touch_id
             FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
             WHERE sts.page_url REGEXP '.*ff_bullfinch=\\d' -- events that are within the test
               AND sts.updated_at >= '2021-11-23'
         ),
              calc_dedupe_parameters AS (
                  SELECT s.event_hash,
                         s.touch_id,
                         s.event_tstamp,
                         --remove the param from url for partitioning
                         REGEXP_REPLACE(s.page_url, '[&|\\?]ff_bullfinch=\\d')                  AS stripped_url,
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
                      SELECT tt.touch_id
                      FROM touches_in_tests tt
                  )
              )
         SELECT cdp.event_hash
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
------------------------------------------------------------------------------------------------------------------------
--top level

--dev
SELECT event_tstamp::DATE AS date,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
WHERE event_tstamp::DATE >= '2021-11-23'
GROUP BY 1;

--prod
SELECT event_tstamp::DATE AS date,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs
WHERE event_tstamp::DATE >= '2021-11-23'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--mobile spvs
--dev
SELECT sts.event_tstamp::DATE AS date,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON sts.touch_id = mtba.touch_id
WHERE sts.event_tstamp::DATE >= '2021-11-23'
  AND mtba.touch_experience IN ('mobile web',
                                'mobile wrap android',
                                'mobile wrap ios',
                                'native app android',
                                'native app ios',
                                'tablet web')
GROUP BY 1;

--prod
SELECT sts.event_tstamp::DATE AS date,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba ON sts.touch_id = mtba.touch_id
WHERE sts.event_tstamp::DATE >= '2021-11-23'
  AND mtba.touch_experience IN ('mobile web',
                                'mobile wrap android',
                                'mobile wrap ios',
                                'native app android',
                                'native app ios',
                                'tablet web')
GROUP BY 1;


SELECT fb.margin_gross_of_toms_gbp_constant_currency FROM se.data.fact_booking fb;


SELECT * FROM se.data.se_user_attributes sua;page