SELECT
    ces.shiro_user_id,
    ces.campaign_id,
    c.name                                                       AS email_name,
    c.mapped_objective,
    c.mapped_platform,
    c.mapped_campaign,
    SPLIT_PART(c.name, '_', 6)                                   AS mapped_segment,
    CASE
        WHEN LOWER(c.name) LIKE '%pureathena%' THEN 'Athena'
        WHEN LOWER(c.name) LIKE '%sunday%best%' THEN 'Sunday Best'
        WHEN (LOWER(c.name) LIKE '%price%drop%'
            OR LOWER(c.name) LIKE '%pd%'
            OR LOWER(c.name) LIKE '%ets%'
            OR LOWER(c.name) LIKE '%flash%'
            OR LOWER(c.name) LIKE '%upgrade%') THEN 'Promo'
        WHEN LOWER(c.name) LIKE '%incremental%' THEN 'Incremental'
        WHEN (LOWER(c.name) LIKE '%new%improved%'
            OR LOWER(c.name) LIKE '%newthisweek') THEN 'New & Improved'
        WHEN LOWER(c.name) LIKE '%abandon%' THEN 'Abandoned Browse'
        WHEN LOWER(c.name) LIKE '%wishlist%' THEN 'Wishlist'
        WHEN LOWER(c.name) LIKE '%media%' THEN 'Media'
        WHEN LOWER(c.name) LIKE '%athena%' THEN 'Other Athena'
        ELSE 'Other'
        END                                                      AS email_theme,
    IFNULL(ces.message_id, 'NA')                                 AS message_id,
    ces.event_date                                               AS send_date,
    ces.event_tstamp                                             AS send_time,
    MAX(IFF(ceo.event_hash IS NOT NULL, 1, 0))                   AS opened,
    MIN(IFF(ceo.event_hash IS NOT NULL, ceo.event_tstamp, NULL)) AS open_time,
    SUM(IFF(ceo.event_hash IS NOT NULL, 1, 0))                   AS opens,
    MAX(IFF(cec.event_hash IS NOT NULL, 1, 0))                   AS clicked,
    MIN(IFF(cec.event_hash IS NOT NULL, cec.event_tstamp, NULL)) AS click_time,
    SUM(IFF(cec.event_hash IS NOT NULL, 1, 0))                   AS clicks,
    MAX(IFF(ceu.event_hash IS NOT NULL, 1, 0))                   AS unsubscribed_email_specific,
    MIN(IFF(ceu.event_hash IS NOT NULL, ceu.event_tstamp, NULL)) AS unsub_time

FROM se.data.crm_events_sends ces
    LEFT JOIN se.data.crm_events_opens ceo ON ces.shiro_user_id = ceo.shiro_user_id
    AND ces.email_id = ceo.email_id
    AND IFNULL(ces.message_id, 'NA') = IFNULL(ceo.message_id, 'NA')
    -- limit to interactions within 1 week of the send
    AND ceo.event_date BETWEEN ces.event_date AND DATEADD(DAY, 36, ces.event_date)
    LEFT JOIN se.data.crm_events_clicks cec ON ces.shiro_user_id = cec.shiro_user_id
    AND ces.email_id = cec.email_id
    AND IFNULL(ces.message_id, 'NA') = IFNULL(cec.message_id, 'NA')
    AND cec.event_date BETWEEN ces.event_date AND DATEADD(DAY, 36, ces.event_date)
    LEFT JOIN se.data.crm_events_unsubscribes ceu ON ces.shiro_user_id = ceu.shiro_user_id
    AND ces.email_id = ceu.email_id
    AND IFNULL(ces.message_id, 'NA') = IFNULL(ceu.message_id, 'NA')
    AND ceu.event_date BETWEEN ces.event_date AND DATEADD(DAY, 36, ces.event_date)
    LEFT JOIN latest_vault.iterable.campaign c ON ces.campaign_id = c.id
WHERE ces.event_date >= DATE_TRUNC(WEEK, DATE('2022-01-01'))
  AND ces.event_date >= DATEADD(DAY, -36, CURRENT_DATE)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_2xlarge;

WITH opens AS (
    SELECT
        ceo.shiro_user_id,
        ceo.email_id,
        ceo.message_id,
        MAX(IFF(ceo.event_hash IS NOT NULL, 1, 0))                   AS opened,
        MIN(IFF(ceo.event_hash IS NOT NULL, ceo.event_tstamp, NULL)) AS open_time,
        SUM(IFF(ceo.event_hash IS NOT NULL, 1, 0))                   AS opens
    FROM se.data.crm_events_opens ceo
        -- if regenerative
--         INNER JOIN se.data.crm_events_sends ces ON ceo.shiro_user_id = ces.shiro_user_id
--         AND ceo.email_id = ces.email_id
--         AND IFNULL(ceo.message_id, 'NA') = IFNULL(ces.message_id, 'NA')
        -- end if regenerative


    WHERE ceo.event_tstamp >= '2023-01-01'
      -- if incremental
      AND ceo.event_date >= DATEADD(DAY, -36, CURRENT_DATE)
      -- end if incremental

      -- if regenerative
--         AND ceo.event_tstamp BETWEEN ces.event_tstamp AND DATEADD(DAY, 36, ces.event_tstamp)
      -- end if regenerative

    GROUP BY 1, 2, 3
),
     clicks AS (
         SELECT
             cec.shiro_user_id,
             cec.email_id,
             cec.message_id,
             MAX(IFF(cec.event_hash IS NOT NULL, 1, 0))                   AS clicked,
             MIN(IFF(cec.event_hash IS NOT NULL, cec.event_tstamp, NULL)) AS click_time,
             SUM(IFF(cec.event_hash IS NOT NULL, 1, 0))                   AS clicks
         FROM se.data.crm_events_clicks cec
             -- if regenerative
--              INNER JOIN se.data.crm_events_sends ces ON cec.shiro_user_id = ces.shiro_user_id
--              AND cec.email_id = ces.email_id
--              AND IFNULL(cec.message_id, 'NA') = IFNULL(ces.message_id, 'NA')
             -- end if regenerative

         WHERE cec.event_tstamp >= '2023-01-01'
           -- if incremental
           AND cec.event_date >= DATEADD(DAY, -36, CURRENT_DATE)
           -- end if incremental

           -- if regenerative
--              AND cec.event_tstamp BETWEEN ces.event_tstamp AND DATEADD(DAY, 36, ces.event_tstamp)
           -- end if regenerative
         GROUP BY 1, 2, 3
     ),
     unsubscribes AS (
         SELECT
             ceu.shiro_user_id,
             ceu.email_id,
             ceu.message_id,
             MAX(IFF(ceu.event_hash IS NOT NULL, 1, 0))                   AS unsubscribed,
             MIN(IFF(ceu.event_hash IS NOT NULL, ceu.event_tstamp, NULL)) AS unsubscribe_time,
             SUM(IFF(ceu.event_hash IS NOT NULL, 1, 0))                   AS unsubscribes
         FROM se.data.crm_events_unsubscribes ceu
             -- if regenerative
--              INNER JOIN se.data.crm_events_sends ces ON ceu.shiro_user_id = ces.shiro_user_id
--              AND ceu.email_id = ces.email_id
--              AND IFNULL(ceu.message_id, 'NA') = IFNULL(ces.message_id, 'NA')
             -- end if regenerative

         WHERE ceu.event_tstamp >= '2023-01-01'
           -- if incremental
           AND ceu.event_date >= DATEADD(DAY, -36, CURRENT_DATE)
           -- end if incremental

           -- if regenerative
--              AND ceu.event_tstamp BETWEEN ces.event_tstamp AND DATEADD(DAY, 36, ces.event_tstamp)
           -- end if regenerative
         GROUP BY 1, 2, 3
     )


SELECT
    ces.shiro_user_id || ces.email_id || COALESCE(ces.message_id, 'NA') AS id,
    ces.shiro_user_id,
    ces.campaign_id,
    cm.email_name                     AS email_name,
    cm.mapped_objective,
    cm.mapped_platform,
    cm.mapped_campaign,
    SPLIT_PART(cm.email_name, '_', 6) AS mapped_segment,
    CASE
        WHEN LOWER(cm.email_name) LIKE '%pureathena%' THEN 'Athena'
        WHEN LOWER(cm.email_name) LIKE '%sunday%best%' THEN 'Sunday Best'
        WHEN (LOWER(cm.email_name) LIKE '%price%drop%'
            OR LOWER(cm.email_name) LIKE '%pd%'
            OR LOWER(cm.email_name) LIKE '%ets%'
            OR LOWER(cm.email_name) LIKE '%flash%'
            OR LOWER(cm.email_name) LIKE '%upgrade%') THEN 'Promo'
        WHEN LOWER(cm.email_name) LIKE '%incremental%' THEN 'Incremental'
        WHEN (LOWER(cm.email_name) LIKE '%new%improved%'
            OR LOWER(cm.email_name) LIKE '%newthisweek') THEN 'New & Improved'
        WHEN LOWER(cm.email_name) LIKE '%abandon%' THEN 'Abandoned Browse'
        WHEN LOWER(cm.email_name) LIKE '%wishlist%' THEN 'Wishlist'
        WHEN LOWER(cm.email_name) LIKE '%media%' THEN 'Media'
        WHEN LOWER(cm.email_name) LIKE '%athena%' THEN 'Other Athena'
        ELSE 'Other'
        END                           AS email_theme,
    IFNULL(ces.message_id, 'NA')      AS message_id,
    ces.event_date                    AS send_date,
    ces.event_tstamp                  AS send_time,
    o.opened,
    o.open_time,
    COALESCE(o.opens, 0)              AS opens,
    c.clicked,
    c.click_time,
    COALESCE(c.clicks, 0)             AS clicks,
    u.unsubscribed,
    u.unsubscribe_time,
    COALESCE(u.unsubscribes, 0)       AS unsubscribes

FROM se.data.crm_events_sends ces
    LEFT JOIN data_vault_mvp.dwh.email_list cm ON ces.email_id = cm.email_id

    LEFT JOIN opens o ON ces.shiro_user_id = o.shiro_user_id
    AND ces.email_id = o.email_id
    AND IFNULL(ces.message_id, 'NA') = IFNULL(o.message_id, 'NA')

    LEFT JOIN clicks c ON ces.shiro_user_id = c.shiro_user_id
    AND ces.email_id = c.email_id
    AND IFNULL(ces.message_id, 'NA') = IFNULL(c.message_id, 'NA')

    LEFT JOIN unsubscribes u ON ces.shiro_user_id = u.shiro_user_id
    AND ces.email_id = u.email_id
    AND IFNULL(ces.message_id, 'NA') = IFNULL(u.message_id, 'NA')

WHERE ces.event_date >= DATE_TRUNC(WEEK, DATE('2022-01-01'))
      -- if incremental
    AND ces.event_date >= DATEADD(DAY, -36, CURRENT_DATE)
-- end if incremental
;

------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM data_vault_mvp.dwh.email_list el