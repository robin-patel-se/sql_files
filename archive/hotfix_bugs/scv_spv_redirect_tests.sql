SELECT sts.event_hash,
       sts.touch_id,
       sts.event_tstamp,
       stba.attributed_user_id,
       sts.page_url
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= current_date - 3
  AND stba.stitched_identity_type = 'se_user_id'
  AND sts.touch_id = '0191b1a992d7922b3078abe45fb70b17838072e8a174fa7da30b87f7eecf3f7c';

SELECT sts.event_hash,
       sts.touch_id,
       sts.event_tstamp,
       stba.attributed_user_id,
       sts.page_url
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= current_date - 1
  AND stba.stitched_identity_type = 'se_user_id'
  AND sts.touch_id = '0191b1a992d7922b3078abe45fb70b17838072e8a174fa7da30b87f7eecf3f7c';


SELECT sts.event_hash,
       sts.touch_id,
       sts.event_tstamp,
       REGEXP_REPLACE(sts.page_url, '[&|\\\?]gce_perbfee=\\\d') AS stripped_url,
       LAG(stripped_url) OVER (PARTITION BY sts.touch_id
           ORDER BY sts.event_tstamp)                           AS prev_stripped_url,
       LAG(sts.event_tstamp) OVER (PARTITION BY sts.touch_id
           ORDER BY sts.event_tstamp)                           AS prev_event_tstamp,
       -- investigated differences in time between double fires,
       -- found an average variance of <1 second, put additional
       -- +1 second as buffer in case of slow load times.
       -- work out if the difference between 2 events is within 2 seconds
       -- if its less than 2 seconds use the earlier of the two for both events
       -- to allow for partitioning
       TIMEDIFF(SECONDS, prev_event_tstamp, sts.event_tstamp),
       IFF(TIMEDIFF(SECONDS, prev_event_tstamp, sts.event_tstamp) <= 2
               AND stripped_url = prev_stripped_url,
           prev_event_tstamp,
           sts.event_tstamp)                                    AS deduped_tstamp,
       stba.attributed_user_id,
       sts.page_url,
       sts.se_sale_id

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= current_date - 1
  AND stba.stitched_identity_type = 'se_user_id'
  AND sts.touch_id = '0191b1a992d7922b3078abe45fb70b17838072e8a174fa7da30b87f7eecf3f7c';

SELECT sts.touch_id,
       sts.event_tstamp,
       stba.attributed_user_id,
       sts.page_url
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= current_date - 1
  AND stba.stitched_identity_type = 'se_user_id'
  AND sts.touch_id = '2bfcf0b21ecc2b360b3a9c16eeda94ac976196fc7914576c9d8d470d60501755';

SELECT sts.touch_id,
       sts.event_tstamp,
       stba.attributed_user_id,
       sts.page_url
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= current_date - 1
  AND stba.stitched_identity_type = 'se_user_id'
  AND sts.touch_id = '2bfcf0b21ecc2b360b3a9c16eeda94ac976196fc7914576c9d8d470d60501755';

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM se.data.scv_touched_spvs sts
WHERE sts.touch_id = '004b65a1a197c91a4089c52e74694c0dad88b5ebdebd604fa313d1602080a68f';


SELECT stf.event_hash,
       stf.touch_id,
       stf.event_tstamp,
       LAG(stf.event_tstamp) OVER (PARTITION BY stf.touch_id ORDER BY stf.event_tstamp) AS prev_event_tstamp,
       TIMEDIFF(SECONDS, prev_event_tstamp, stf.event_tstamp)                           AS seconds_diff,
       IFF(seconds_diff <= 2, TRUE, FALSE)                                              AS time_diff,
       stf.se_sale_id,
       stf.page_url,
       REGEXP_SUBSTR(stf.page_url, '(gce_perbfee=\\\d)', 1, 1, 'e')                     AS test_param,
       REGEXP_REPLACE(stf.page_url, '[&|\\\?]gce_perbfee=\\\d')                         AS stripped_url
FROM scratch.robinpatel.spv_test_fix stf


------------------------------------------------------------------------------------------------------------------------

SELECT sts.touch_id, sts.event_tstamp, sts.page_url, sts.se_sale_id
FROM se.data.scv_touched_spvs sts
WHERE sts.page_url LIKE '%gce_perbfee=%';

WITH touches_in_tests AS (
    --identify all sessions that have any event that includes the redirect test parameter
    SELECT DISTINCT sts.touch_id
    FROM se.data.scv_touched_spvs sts
    WHERE sts.page_url REGEXP '.*gce_perbfee=\\\d' -- events that are within the test
    LIMIT 50 -- TODO remove, just for testing. Select 50 sessions that have the gce param
--     AND t.updated_at >= TIMESTAMPADD('day', -1, '{schedule_tstamp}'::TIMESTAMP) --TODO uncomment in production
),
     calc_dedupe_parameters AS (
         SELECT s.event_hash,
                s.touch_id,
                s.event_tstamp,
                -- investigated differences in time between double fires, found an average variance of <1 second, put additional
                -- +1 second as buffer in case of slow load times.
                LAG(s.event_tstamp) OVER (PARTITION BY s.touch_id
                    ORDER BY s.event_tstamp)                           AS prev_event_tstamp,
                -- work out if the difference between 2 events is within 2 seconds
                IFF(TIMEDIFF(SECONDS, prev_event_tstamp, s.event_tstamp) <= 2,
                    prev_event_tstamp,
                    s.event_tstamp)                                    AS deduped_tstamp,
                s.se_sale_id,
                --remove the param from url for partitioning
                REGEXP_REPLACE(s.page_url, '[&|\\\?]gce_perbfee=\\\d') AS stripped_url
         FROM se.data.scv_touched_spvs s
         WHERE s.touch_id IN (
             SELECT tt.touch_id
             FROM touches_in_tests tt
         )
     )
SELECT cdp.event_hash
--        cdp.touch_id,
--        cdp.event_tstamp,
--        cdp.prev_event_tstamp,
--        cdp.deduped_tstamp,
--        cdp.se_sale_id,
--        cdp.page_url,
--        cdp.stripped_url
FROM calc_dedupe_parameters cdp
     -- dedupe to return the earliest event (in a double fire) which doesn't contain the
     -- test url parm. The result of this will feed into a delete function so it will
     -- delete the initial event, the one without the url param.
    QUALIFY ROW_NUMBER()
                    OVER (PARTITION BY
                        cdp.touch_id,
                        cdp.deduped_tstamp,
                        cdp.se_sale_id,
                        cdp.stripped_url
                        ORDER BY cdp.event_tstamp) = 1
;



CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= current_date - 2;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification es
WHERE es.event_tstamp >= current_date - 2;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;


self_describing_task --include 'dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py'  --method 'run' --start '2020-09-21 00:00:00' --end '2020-09-21 00:00:00'


    --identify all sessions that have any event that includes the redirect test parameter
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
WHERE sts.page_url REGEXP '.*gce_perbfee=\\d' -- events that are within the test
  AND sts.updated_at >= TIMESTAMPADD('day', -1, '2020-09-20 03:00:00'::TIMESTAMP);

SELECT *
FROM se.data.ratepay_clearing rc
WHERE rc.booking_id = 'A1365556';

SELECT *
FROM se.data.fact_complete_booking fcb;

SELECT *
FROM raw_vault_mvp.sfsc.account a
WHERE LEFT(id, 15) = '001w000001DVHw3';

SELECT hotel_code
FROM se.data.se_sale_attributes ssa;


SELECT *,
       ssa.hotel_code AS sf_account_id
FROM se.data.master_se_booking_list msbl
         LEFT JOIN se.data.se_sale_attributes ssa ON msbl.saleid = ssa.se_sale_id;

--prod
SELECT sts.event_tstamp::DATE,
       count(*)
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp::DATE > current_date - 4
GROUP BY 1;

SELECT sts.event_tstamp::DATE,
       count(*)
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE > current_date - 4
AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1;

--dev
SELECT sts.event_tstamp::DATE,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
WHERE sts.event_tstamp::DATE > current_date - 4
GROUP BY 1;

