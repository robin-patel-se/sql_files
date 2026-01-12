SELECT
    MIN(asl.log_date)
FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl; --2021-10-16 06:01:15.000000000
USE WAREHOUSE pipe_2xlarge;

WITH clicks AS (
    --aggregate spvs that are from emails up to grain that can combine
    SELECT
        stba.attributed_user_id,
        TRY_TO_NUMBER(stmc.utm_campaign) AS send_id,
        sts.se_sale_id,
        COUNT(*)                         AS clicks
    FROM se.data.scv_touched_spvs sts
        INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    WHERE sts.event_hash = sts.touch_id --landing page is an spv
      AND stmc.touch_mkt_channel LIKE 'Email%'
      AND TRY_TO_NUMBER(stmc.utm_campaign) IS NOT NULL
      AND sts.event_tstamp >= (
        SELECT
            MIN(asl.log_date)::DATE
        FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl
    )                                   --athena send log data is trimmed
    GROUP BY 1, 2, 3
),
     sale_position AS (
         --compute the sale position in an email send
         SELECT
             a.deal_id                                                                                             AS se_sale_id,
             a.job_id                                                                                              AS send_id,
             a.subscriber_key,
             ROW_NUMBER() OVER (PARTITION BY a.job_id, a.subscriber_key ORDER BY a.section, a.position_in_section) AS sale_position
         FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log a
     ),
     position_and_clicks AS (
         SELECT
             sp.se_sale_id,
             sp.send_id,
             sp.subscriber_key,
             sp.sale_position,
             COALESCE(c.clicks, 0) AS clicks
         FROM sale_position sp
             LEFT JOIN clicks c ON sp.se_sale_id = c.se_sale_id
             AND sp.send_id = c.send_id
             AND sp.subscriber_key = c.attributed_user_id
     )
SELECT
    pc.sale_position,
    SUM(pc.clicks) AS clicks
FROM position_and_clicks pc
GROUP BY 1--, 2;


WITH clicks AS (
    --aggregate spvs that are from emails up to grain that can combine
    SELECT
        stba.attributed_user_id,
        TRY_TO_NUMBER(stmc.utm_campaign) AS send_id,
        sts.se_sale_id,
        COUNT(*)                         AS clicks
    FROM se.data.scv_touched_spvs sts
        INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    WHERE sts.event_hash = sts.touch_id --landing page is an spv
      AND stmc.touch_mkt_channel LIKE 'Email%'
      AND TRY_TO_NUMBER(stmc.utm_campaign) IS NOT NULL
      AND sts.event_tstamp >= (
        SELECT
            MIN(asl.log_date)::DATE
        FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl
    )                                   --athena send log data is trimmed
    GROUP BY 1, 2, 3
),
     sale_position AS (
         --compute the sale position in an email send
         SELECT
             a.deal_id                                                                                             AS se_sale_id,
             a.job_id                                                                                              AS send_id,
             a.subscriber_key,
             ROW_NUMBER() OVER (PARTITION BY a.job_id, a.subscriber_key ORDER BY a.section, a.position_in_section) AS sale_position
         FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log a
     ),
     position_and_clicks AS (
         SELECT
             sp.se_sale_id,
             sp.send_id,
             sp.subscriber_key,
             sp.sale_position,
             COALESCE(c.clicks, 0) AS clicks
         FROM sale_position sp
             LEFT JOIN  clicks c ON sp.se_sale_id = c.se_sale_id
             AND sp.send_id = c.send_id
             AND sp.subscriber_key = c.attributed_user_id
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON sp.subscriber_key = ua.shiro_user_id
             AND SPLIT_PART(ua.email, '@', -1) = 'gmail.com'
             AND ua.current_affiliate_territory = 'DE'
     )
SELECT
    pc.sale_position,
    SUM(pc.clicks) AS clicks
FROM position_and_clicks pc
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------

USE WAREHOUSE pipe_2xlarge;

WITH clicks AS (
    --aggregate spvs that are from emails up to grain that can combine
    SELECT
        stba.attributed_user_id,
        TRY_TO_NUMBER(stmc.utm_campaign) AS campaign_id,
        sts.se_sale_id,
        COUNT(*)                         AS clicks
    FROM se.data.scv_touched_spvs sts
        INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    WHERE sts.event_hash = sts.touch_id --landing page is an spv
      AND stmc.touch_mkt_channel LIKE 'Email%'
      AND TRY_TO_NUMBER(stmc.utm_campaign) IS NOT NULL
      AND sts.event_tstamp >= (
        SELECT
            MIN(asl.request_time)::DATE
        FROM latest_vault.iterable.email_send_log asl
    )                                   --athena send log data is trimmed
    GROUP BY 1, 2, 3
),
     sale_position AS (
         --compute the sale position in an email send
         SELECT
             esl.campaign_id,
             esl.user_id,
             element_sids.index + 1                          AS sale_position,
             SPLIT_PART(element_sids.value::VARCHAR, '-', 1) AS se_sale_id
         FROM latest_vault.iterable.email_send_log esl,
              LATERAL FLATTEN(INPUT => shown_deal_ids, OUTER => TRUE) element_sids
     ),
     position_and_clicks AS (
         SELECT
             sp.se_sale_id,
             sp.campaign_id,
             sp.user_id,
             sp.sale_position,
             COALESCE(c.clicks, 0) AS clicks
         FROM sale_position sp
             LEFT JOIN clicks c ON sp.se_sale_id = c.se_sale_id
             AND sp.campaign_id = c.campaign_id
             AND sp.user_id = c.attributed_user_id
     )
SELECT
    pc.sale_position,
    SUM(pc.clicks) AS clicks
FROM position_and_clicks pc
GROUP BY 1;
