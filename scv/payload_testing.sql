SELECT sts.event_tstamp::DATE,
       ds.product_configuration,
       COUNT(*)                                                         AS all_spvs,
       SUM(IFF(ses.useragent = 'ApacheBench/2.3', 1, 0))                AS apache_user_agent_spvs,
       SUM(IFF(ses.useragent IS DISTINCT FROM 'ApacheBench/2.3', 1, 0)) AS non_apache_user_agent_spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         INNER JOIN se.data_pii.scv_event_stream ses ON sts.event_hash = ses.event_hash
WHERE sts.event_tstamp >= CURRENT_DATE - 10
  AND ds.product_configuration IN ('3PP', 'IHP - static')
GROUP BY 1, 2;


--all the requests i made would have the user-agent ApacheBench/2.3
SELECT sts.*,
       ses.useragent
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         INNER JOIN se.data_pii.scv_event_stream ses ON sts.event_hash = ses.event_hash
WHERE sts.event_tstamp >= CURRENT_DATE - 1
  AND ds.product_configuration IN ('3PP', 'IHP - static')
LIMIT 1000;

USE WAREHOUSE pipe_2xlarge;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_id = 'c49f2ac2a1150c69935011292851ba72693a819006998c96c24caf86fe0b412c'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 1;


SELECT *
FROM se.data_pii.scv_session_events_link ssel
         INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = 'c49f2ac2a1150c69935011292851ba72693a819006998c96c24caf86fe0b412c'
  AND ssel.event_tstamp >= CURRENT_DATE - 1;



self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2021-06-16 00:00:00' --end '2021-06-16 00:00:00'


SELECT
    --aggregate values
    t.touch_id,
    t.attributed_user_id                                                       AS attributed_user_id,
    t.stitched_identity_type                                                   AS stitched_identity_type,
    IFF(MAX(e.se_user_id) IS NOT NULL, TRUE, FALSE)                            AS touch_logged_in, --if the user has logged in at any time during this session
    MIN(e.event_tstamp)                                                        AS touch_start_tstamp,
    MAX(e.event_tstamp)                                                        AS touch_end_tstamp,
    TIMEDIFF(SECONDS, MIN(e.event_tstamp),
             MAX(e.event_tstamp))                                              AS touch_duration_seconds,
    MAX(e.is_internal_ip_address_event)                                        AS is_internal_ip_address,
    MAX(IFF(SPLIT_PART(ua.email, '@', -1) = 'secretescapes.com', TRUE, FALSE)) AS is_se_email_domain_user,
    IFF(is_internal_ip_address OR is_se_email_domain_user, TRUE, FALSE)        AS is_se_internal_touch,
    SUM(IFF(e.useragent = 'ApacheBench/2.3', 1, 0)) > 0                        AS page_load_testing,
    COUNT(*)                                                                   AS touch_event_count,
    SUM(
            CASE
                WHEN
                    (e.event_name IN ('transaction_item', 'transaction')
                        OR
                        --server side se
                     e.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM 'booking confirmed'
                        OR
                        --server side tb
                     e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM 'transaction complete'
                        ) THEN 1
                ELSE 0 END
        ) > 0
                                                                               AS touch_has_booking
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
         LEFT JOIN  data_vault_mvp_dev_robin.dwh.user_attributes ua ON TRY_TO_NUMBER(t.attributed_user_id) = ua.shiro_user_id
WHERE t.updated_at >= TIMESTAMPADD('day', -1, '2021-06-15 03:00:00'::TIMESTAMP)
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE scratch.robinpatel.load_testing_touch_ids AS (
    SELECT DISTINCT
           ssel.touch_id
    FROM se.data_pii.scv_session_events_link ssel
             INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
    WHERE ssel.event_tstamp >= CURRENT_DATE - 2
      AND ses.useragent = 'ApacheBench/2.3'
);

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
SET mtba.touch_hostname_territory = 'SE TECH',
    mtba.useragent                = 'ApacheBench/2.3'
FROM scratch.robinpatel.load_testing_touch_ids batch
WHERE mtba.touch_id = batch.touch_id;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtba
SET mtba.touch_affiliate_territory = 'SE TECH'
FROM scratch.robinpatel.load_testing_touch_ids batch
WHERE mtba.touch_id = batch.touch_id;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_id IN
      ('99268e6be630a5ceb089d8708435ef28c65884031ca6960c35961d5d90c11d92',
       'c49f2ac2a1150c69935011292851ba72693a819006998c96c24caf86fe0b412c',
       '0bce156fc1c4f0f1d7d1109c27854bd0ecda7e5dc8c0c62c5a1c489f98964adb',
       '49d8da82826183b0c16d5c3b6d896214fd51df31639a0efe3e528a60f734c0f9',
       'f8c423d15f69bbf6c07bf1d1ef7191477c2f440d932a2bb59977e2c64c0fcbd4',
       'ea86bf201bb8bf88cf0479951b1934e3d1acb5bb09910ae10382d901c5102faa',
       '240a10ca8ce2a0bd5c5a16b3faf4323692da4e2ed76b9f187aa1712bfba546b1',
       'e0aea9d2badfeb11a16b8e22f273329a74c053d3a354900db65960a45ef9bc60'
          );

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtba
WHERE mtba.touch_id IN
      ('99268e6be630a5ceb089d8708435ef28c65884031ca6960c35961d5d90c11d92',
       'c49f2ac2a1150c69935011292851ba72693a819006998c96c24caf86fe0b412c',
       '0bce156fc1c4f0f1d7d1109c27854bd0ecda7e5dc8c0c62c5a1c489f98964adb',
       '49d8da82826183b0c16d5c3b6d896214fd51df31639a0efe3e528a60f734c0f9',
       'f8c423d15f69bbf6c07bf1d1ef7191477c2f440d932a2bb59977e2c64c0fcbd4',
       'ea86bf201bb8bf88cf0479951b1934e3d1acb5bb09910ae10382d901c5102faa',
       '240a10ca8ce2a0bd5c5a16b3faf4323692da4e2ed76b9f187aa1712bfba546b1',
       'e0aea9d2badfeb11a16b8e22f273329a74c053d3a354900db65960a45ef9bc60'
          );


SELECT sts.event_tstamp::DATE,
       ds.product_configuration,
       COUNT(*)                                                         AS all_spvs,
       SUM(IFF(ses.useragent = 'ApacheBench/2.3', 1, 0))                AS apache_user_agent_spvs,
       SUM(IFF(ses.useragent IS DISTINCT FROM 'ApacheBench/2.3', 1, 0)) AS non_apache_user_agent_spvs,
       SUM(IFF(stmc.touch_affiliate_territory = 'SE TECH', 1, 0))       AS tech_spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         INNER JOIN se.data_pii.scv_event_stream ses ON sts.event_hash = ses.event_hash
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= CURRENT_DATE - 10
  AND ds.product_configuration IN ('3PP', 'IHP - static')
GROUP BY 1, 2;



airflow backfill --start_date '2021-06-17 00:00:00' --end_date '2021-06-18 00:00:00' --task_regex '.*' se__bi__fact_sale_metrics__daily_at_07h00
airflow backfill --start_date '2021-06-17 00:00:00' --end_date '2021-06-18 00:00:00' --task_regex '.*' se__bi__dim_sale__daily_at_07h00