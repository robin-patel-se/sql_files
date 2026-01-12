USE WAREHOUSE pipe_xlarge;

SELECT
    mtba.touch_id,
    mtba.touch_posa_territory,
    mtba.touch_hostname_territory,
    mtba.touch_hostname,
    mtba.touch_experience,
    es.posa_territory,
    es.app_id,
    es.event_name,
    es.br_lang,
    ua.original_affiliate_territory,
    ua.current_affiliate_territory
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mtba.touch_id = es.event_hash AND es.event_tstamp >= CURRENT_DATE - 1
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON TRY_TO_NUMBER(mtba.attributed_user_id) = ua.shiro_user_id
WHERE mtba.touch_experience = 'native app ios'
  AND mtba.touch_start_tstamp >= CURRENT_DATE - 1;


-- check on aggregate the different permutations of the br_lang
SELECT
    es.br_lang,
    CASE
        WHEN es.br_lang IN ('de-DE', 'de', 'de-AT', 'de-CH') THEN 'DE'
        WHEN es.br_lang IN ('it-IT') THEN 'IT'
        WHEN es.br_lang IN ('en-GB') THEN 'UK'
        END AS language_territory,
    ua.current_affiliate_territory,
    es.posa_territory,
    mtmc.touch_affiliate_territory,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mtba.touch_id = es.event_hash AND es.event_tstamp >= CURRENT_DATE - 1
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON TRY_TO_NUMBER(mtba.attributed_user_id) = ua.shiro_user_id
WHERE mtba.touch_experience = 'native app ios'
  AND mtba.touch_start_tstamp >= '2022-09-13'
GROUP BY 1, 2, 3, 4, 5;





SELECT
    es.br_lang,
    es.posa_territory,
    ua.current_affiliate_territory,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mtba.touch_id = es.event_hash AND es.event_tstamp >= CURRENT_DATE - 1
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON TRY_TO_NUMBER(mtba.attributed_user_id) = ua.shiro_user_id
WHERE mtba.touch_experience = 'native app ios'
  AND mtba.touch_start_tstamp >= CURRENT_DATE - 10
GROUP BY 1, 2, 3;



CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker;

--prod
SELECT mtba.touch_hostname_territory,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_experience = 'native app ios'
AND mtba.touch_start_tstamp >= '2022-09-13'
AND mtba.touch_hostname_territory IN ('UK', 'DE', 'IT')
GROUP BY 1;

--dev
SELECT mtba.touch_hostname_territory,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_experience = 'native app ios'
AND mtba.touch_start_tstamp >= '2022-09-13'
AND mtba.touch_hostname_territory IN ('UK', 'DE', 'IT')
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------
--fix affiliate territory
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;


SELECT COUNT(*) FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc WHERE mtmc.touch_hostname_territory = 'DE' AND mtmc.touch_affiliate_territory ='UK';
SELECT COUNT(*) FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20221121 mtmc WHERE mtmc.touch_hostname_territory = 'DE' AND mtmc.touch_affiliate_territory ='UK';