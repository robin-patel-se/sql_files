CREATE TEMPORARY TABLE touch_users AS
    (
        SELECT user_id,
               CASE
                   WHEN has_seen_version_0 = 1 THEN 'control'
                   WHEN has_seen_version_1 = 1 THEN 'var1'
                   WHEN has_seen_version_2 = 1 THEN 'var2'
                   ELSE 'other' END AS test_variants
        FROM (
                 SELECT a.attributed_user_id                                                         AS user_id,
                        max(CASE WHEN a.touch_landing_page LIKE '%gce_perbfee=0%' THEN 1 ELSE 0 END) AS has_seen_version_0,
                        max(CASE WHEN a.touch_landing_page LIKE '%gce_perbfee=1%' THEN 1 ELSE 0 END) AS has_seen_version_1,
                        max(CASE WHEN a.touch_landing_page LIKE '%gce_perbfee=2%' THEN 1 ELSE 0 END) AS has_seen_version_2
                 FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes a
                 WHERE to_date(touch_start_tstamp) BETWEEN '2020-09-17' AND '2020-09-30'
                   AND touch_hostname_territory = 'UK'
                   -- and TOUCH_POSA_TERRITORY = 'UK'
                 GROUP BY 1
                 HAVING has_seen_version_0 + has_seen_version_1 + has_seen_version_2 = 1
             )
    );
USE WAREHOUSE pipe_xlarge;

WITH gce_events AS (
    SELECT PARSE_URL(es.page_url)['parameters']['gce_perbfee']::VARCHAR AS variant,
           es.*,
           mt.attributed_user_id,
           mt.stitched_identity_type,
           mt.touch_id,
           mt.event_index_within_touch
    FROM hygiene_vault_mvp.snowplow.event_stream es
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mt.touch_id = mtmc.touch_id
    WHERE es.event_tstamp::DATE BETWEEN '2020-09-17' AND '2020-09-30'
      AND es.page_urlquery LIKE '%gce_perbfee=%'
      AND mtmc.touch_affiliate_territory = 'UK'
)
SELECT CASE
           WHEN variant = '0' THEN 'control'
           WHEN variant = '1' THEN 'var1'
           WHEN variant = '2' THEN 'var2'
           ELSE 'other' END                  AS test_variants,
       COUNT(DISTINCT ge.attributed_user_id) AS users
FROM gce_events ge
GROUP BY 1;


WITH gce_events AS (
    SELECT PARSE_URL(es.page_url)['parameters']['gce_perbfee']::VARCHAR AS variant,
           es.*,
           mt.attributed_user_id,
           mt.stitched_identity_type,
           mt.touch_id,
           mt.event_index_within_touch
    FROM hygiene_vault_mvp.snowplow.event_stream es
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mt.touch_id = mtmc.touch_id
    WHERE es.event_tstamp::DATE BETWEEN '2020-09-17' AND '2020-09-30'
      AND es.page_urlquery LIKE '%gce_perbfee=%'
      AND mtmc.touch_affiliate_territory = 'UK'
)
SELECT ge.attributed_user_id,
       CASE
           WHEN variant = '0' THEN 'control'
           WHEN variant = '1' THEN 'var1'
           WHEN variant = '2' THEN 'var2'
           ELSE 'other' END                  AS test_variants
FROM gce_events ge;