USE WAREHOUSE pipe_xlarge;
SET (week_start, week_end)= ('2020-01-01', '2020-05-07');
--original
WITH wau AS
    (SELECT app_id
          , min(date)                 AS week_start
          , max(date)                 AS week_end
          , count(DISTINCT (user_id)) AS wau
     FROM (SELECT CASE
                      WHEN app_id = 'UK' THEN 'UK'
                      WHEN app_id = 'DE' THEN 'DE'
                      WHEN app_id = 'IT' THEN 'IT'
         END                                AS app_id
                , to_date(collector_tstamp) AS date
                , user_id
           FROM hygiene_vault_mvp.snowplow.event_stream
           WHERE app_id IN ('UK', 'DE', 'IT')
             AND to_date(collector_tstamp) >= $week_start
             AND to_date(collector_tstamp) <= $week_end
             AND user_id IS NOT NULL
             AND useragent NOT LIKE '%mobile_native_v3%'
             AND v_tracker NOT LIKE 'py-%'
             AND v_tracker NOT LIKE 'java-%'
           GROUP BY 1, 2, 3)
     GROUP BY 1)
   , mau AS
    (SELECT app_id
          , min(date)                 AS week_start
          , max(date)                 AS week_end
          , count(DISTINCT (user_id)) AS mau
     FROM (SELECT CASE
                      WHEN app_id = 'UK' THEN 'UK'
                      WHEN app_id = 'DE' THEN 'DE'
                      WHEN app_id = 'IT' THEN 'IT'
         END                                AS app_id
                , to_date(collector_tstamp) AS date
                , user_id
           FROM hygiene_vault_mvp.snowplow.event_stream
           WHERE app_id IN ('UK', 'DE', 'IT')
             AND to_date(collector_tstamp) > (cast($week_end AS DATE) - 28)
             AND to_date(collector_tstamp) <= $week_end
             AND user_id IS NOT NULL
             AND useragent NOT LIKE '%mobile_native_v3%'
             AND v_tracker NOT LIKE 'py-%'
             AND v_tracker NOT LIKE 'java-%'
           GROUP BY 1, 2, 3)
     GROUP BY 1)
   , sessions AS
    (SELECT app_id
          , min(date)         AS week_start
          , max(date)         AS week_end
          , count(session_id) AS sessions
     FROM (SELECT CASE
                      WHEN app_id = 'UK' THEN 'UK'
                      WHEN app_id = 'DE' THEN 'DE'
                      WHEN app_id = 'IT' THEN 'IT'
         END                                AS app_id
                , to_date(collector_tstamp) AS date
                , domain_sessionid          AS session_id
           FROM hygiene_vault_mvp.snowplow.event_stream
           WHERE app_id IN ('UK', 'DE', 'IT')
             AND to_date(collector_tstamp) >= $week_start
             AND to_date(collector_tstamp) <= $week_end
             AND user_id IS NOT NULL
             AND useragent NOT LIKE '%mobile_native_v3%'
             AND v_tracker NOT LIKE 'py-%'
             AND v_tracker NOT LIKE 'java-%'
           GROUP BY 1, 2, 3)
     GROUP BY 1)
   , spv_se AS
    (SELECT app_id
          , min(date)       AS week_start
          , max(date)       AS week_end
          , count(event_id) AS spv_se
     FROM (SELECT CASE
                      WHEN app_id = 'UK' THEN 'UK'
                      WHEN app_id = 'DE' THEN 'DE'
                      WHEN app_id = 'IT' THEN 'IT'
         END                                AS app_id
                , to_date(collector_tstamp) AS date
                , event_id
           FROM hygiene_vault_mvp.snowplow.event_stream
           WHERE app_id IN ('UK', 'DE', 'IT')
             AND to_date(collector_tstamp) >= $week_start
             AND to_date(collector_tstamp) <= $week_end
             AND user_id IS NOT NULL
             AND event = 'page_view'
             AND page_url LIKE '%/sale%'
             AND page_url NOT LIKE '%/sale/book%'
             AND page_url NOT LIKE '%/sale/allocationsByDate%'
             AND useragent NOT LIKE '%mobile_native_v3%'
             AND v_tracker NOT LIKE 'py-%'
             AND v_tracker NOT LIKE 'java-%'
           GROUP BY 1, 2, 3)
     GROUP BY 1)
   , spv_tb AS
    (SELECT app_id
          , min(date)       AS week_start
          , max(date)       AS week_end
          , count(event_id) AS spv_tb
     FROM (SELECT CASE
                      WHEN contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR = 'GB' THEN 'UK'
                      WHEN contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR = 'DE' THEN 'DE'
                      WHEN contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR = 'IT' THEN 'IT'
         END                                AS app_id
                , to_date(collector_tstamp) AS date
                , event_id
           FROM hygiene_vault_mvp.snowplow.event_stream
           WHERE contexts_com_secretescapes_environment_context_1[0]['device_platform']::VARCHAR NOT LIKE 'IOS_APP_V3'
             AND contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR = 'Travelbird Platform'
             AND to_date(collector_tstamp) >= $week_start
             AND to_date(collector_tstamp) <= $week_end
             AND user_id IS NOT NULL
             AND event_name = 'page_view'
             AND contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
             AND v_tracker LIKE 'py-%'       -- from TB
             AND v_tracker NOT LIKE 'java-%' -- not from SE S2S tracker
           GROUP BY 1, 2, 3
          )
     GROUP BY 1)

   , temp_res_1 AS
    (SELECT w.*, s.sessions
     FROM wau w
              INNER JOIN sessions s
                         ON w.app_id = s.app_id)

   , temp_res_2 AS
    (SELECT t.*, e.spv_se
     FROM temp_res_1 t
              INNER JOIN spv_se e
                         ON t.app_id = e.app_id)

   , temp_res_3 AS
    (SELECT t.*, e.spv_tb
     FROM temp_res_2 t
              LEFT JOIN spv_tb e
                        ON t.app_id = e.app_id)

SELECT t.app_id
     , t.week_start
     , t.week_end
     , t.wau
     , t.sessions
     , coalesce(t.spv_se + t.spv_tb, t.spv_se, t.spv_tb) AS spv
     , m.mau
FROM temp_res_3 t
         INNER JOIN mau m
                    ON t.app_id = m.app_id;

------------------------------------------------------------------------------------------------------------------------

SET (week_start, week_end)= ('2020-01-01', '2020-05-07');
--refactored
WITH mau AS (
    SELECT $week_start,
           $week_end,
           CASE WHEN b.touch_posa_territory = 'GB' THEN 'UK' ELSE b.touch_posa_territory END                    AS territory,
           count(DISTINCT CASE WHEN b.stitched_identity_type = 'se_user_id' THEN b.attributed_user_id_hash END) AS mau
    FROM se.data.scv_touch_basic_attributes b
    WHERE b.touch_experience != 'native app'
      AND b.touch_posa_territory IN ('DE', 'UK', 'IT', 'GB')
      AND b.touch_start_tstamp >= DATEADD(DAY, -28, $week_end)
      AND b.touch_start_tstamp <= $week_end
    GROUP BY 1, 2, 3
)
SELECT $week_start                                                                                          AS week_start,
       $week_end                                                                                            AS week_end,
       CASE WHEN b.touch_posa_territory = 'GB' THEN 'UK' ELSE b.touch_posa_territory END                    AS territory,
       m.mau,
       count(DISTINCT CASE WHEN b.stitched_identity_type = 'se_user_id' THEN b.touch_id END)                AS sessions,
       count(DISTINCT s.event_hash)                                                                         AS spvs,
       count(DISTINCT CASE WHEN b.stitched_identity_type = 'se_user_id' THEN b.attributed_user_id_hash END) AS wau
FROM se.data.scv_touch_basic_attributes b
         LEFT JOIN se.data.scv_touched_spvs s ON b.touch_id = s.touch_id
         LEFT JOIN mau m ON CASE WHEN b.touch_posa_territory = 'GB' THEN 'UK' ELSE b.touch_posa_territory END = m.territory
WHERE b.touch_experience != 'native app'
  AND b.touch_posa_territory IN ('DE', 'UK', 'IT', 'GB')
  AND b.touch_start_tstamp >= $week_start
  AND b.touch_start_tstamp <= $week_end
GROUP BY 1, 2, 3, 4;

------------------------------------------------------------------------------------------------------------------------
SET (week_start, week_end)= ('2020-01-01', '2020-05-07');
USE WAREHOUSE pipe_xlarge;

--ORIGINAL MAU
SELECT app_id
     , min(date)                 AS week_start
     , max(date)                 AS week_end
     , count(DISTINCT (user_id)) AS mau
FROM (SELECT CASE
                 WHEN app_id = 'UK' THEN 'UK'
                 WHEN app_id = 'DE' THEN 'DE'
                 WHEN app_id = 'IT' THEN 'IT'
    END                                AS app_id
           , to_date(collector_tstamp) AS date
           , user_id
      FROM hygiene_vault_mvp.snowplow.event_stream
      WHERE app_id IN ('UK', 'DE', 'IT')
        AND to_date(collector_tstamp) > (cast($week_end AS DATE) - 28)
        AND to_date(collector_tstamp) <= $week_end
        AND user_id IS NOT NULL
        AND useragent NOT LIKE '%mobile_native_v3%'
        AND v_tracker NOT LIKE 'py-%'
        AND v_tracker NOT LIKE 'java-%'
      GROUP BY 1, 2, 3)
GROUP BY 1;

--refactored original mau
SELECT app_id,
       $week_start,
       $week_end,
       count(DISTINCT user_id) AS mau
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE app_id IN ('UK', 'DE', 'IT')
  AND to_date(collector_tstamp) > DATEADD(DAY, -28, $week_end)
  AND to_date(collector_tstamp) <= $week_end
  AND user_id IS NOT NULL
  AND useragent NOT LIKE '%mobile_native_v3%'
--   AND v_tracker NOT LIKE 'py-%'
--   AND v_tracker NOT LIKE 'java-%'
GROUP BY 1, 2, 3;

--refactored scv mau
SELECT $week_start,
       $week_end,
       CASE WHEN b.touch_posa_territory = 'GB' THEN 'UK' ELSE b.touch_posa_territory END                    AS territory,
       count(DISTINCT CASE WHEN b.stitched_identity_type = 'se_user_id' THEN b.attributed_user_id_hash END) AS mau
FROM se.data.scv_touch_basic_attributes b
WHERE b.touch_experience != 'native app'
  AND b.touch_posa_territory IN ('DE', 'UK', 'IT', 'GB')
  AND b.touch_start_tstamp >= DATEADD(DAY, -28, $week_end)
  AND b.touch_start_tstamp <= $week_end
GROUP BY 1, 2, 3



SELECT app_id,
       contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR AS posa_territory,
       page_urlhost,
       count(DISTINCT domain_sessionid)
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE app_id IN ('UK')
  AND to_date(collector_tstamp) > (cast($week_end AS DATE) - 28)
  AND to_date(collector_tstamp) <= $week_end
  AND user_id IS NOT NULL
  AND useragent NOT LIKE '%mobile_native_v3%'
  AND v_tracker NOT LIKE 'py-%'
  AND v_tracker NOT LIKE 'java-%'
GROUP BY 1, 2, 3;


SELECT app_id,
       to_date(collector_tstamp) AS date,
       domain_sessionid          AS session_id
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE app_id IN ('UK', 'DE', 'IT')
  AND to_date(collector_tstamp) >= $week_start
  AND to_date(collector_tstamp) <= $week_end
  AND user_id IS NOT NULL
  AND useragent NOT LIKE '%mobile_native_v3%'
  AND v_tracker NOT LIKE 'py-%'
  AND v_tracker NOT LIKE 'java-%'
GROUP BY 1, 2, 3;

SELECT *
FROM raw_vault_mvp.sfmc.push_status
WHERE TRY_TO_NUMBER(user_id) IS NULL;


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
ALTER SESSION SET week_start = 5; --set start of week to Friday
SET (from_date, to_date)= ('2020-01-01', '2020-05-07');
SELECT date_trunc(WEEK, b.touch_start_tstamp)                                                               AS week_start,
       dateadd(DAY, 6, week_start)                                                                          AS week_end,
       b.touch_hostname_territory,
       count(DISTINCT b.touch_id)                                                                           AS sessions,
       count(DISTINCT s.event_hash)                                                                         AS spvs,
       count(DISTINCT CASE WHEN b.stitched_identity_type = 'se_user_id' THEN b.attributed_user_id_hash END) AS wau
FROM se.data.scv_touch_basic_attributes b
         LEFT JOIN se.data.scv_touched_spvs s ON b.touch_id = s.touch_id
WHERE b.touch_experience != 'native app'
  AND b.touch_hostname_territory IN ('UK', 'IT', 'DE')
  AND b.touch_start_tstamp >= $from_date
  AND b.touch_start_tstamp <= $to_date
GROUP BY 1, 2, 3
ORDER BY 1, 3;

------------------------------------------------------------------------------------------------------------------------
ALTER SESSION SET week_start = 5; --set start of week to Friday
SET (from_date, to_date)= ('2020-01-01', '2020-05-07');
WITH grain AS ( --create a weekly grain
    SELECT DISTINCT date_trunc(WEEK, b.touch_start_tstamp) AS week_start,
                    dateadd(DAY, 6, week_start)            AS week_end
    FROM se.data.scv_touch_basic_attributes b
    WHERE b.touch_start_tstamp >= $from_date
      AND b.touch_start_tstamp <= $to_date
),
     mau AS ( --calculate monthly active users, users that are active within 28 days of the week end date
         SELECT g.week_start,
                g.week_end,
                t.touch_affiliate_territory,
                count(DISTINCT b.attributed_user_id_hash) AS mau
         FROM grain g
                  LEFT JOIN se.data.scv_touch_basic_attributes b
                            ON DATEADD(DAY, -28, g.week_end) <= b.touch_start_tstamp::DATE
                                AND g.week_end >= b.touch_start_tstamp::DATE
                  LEFT JOIN se.data.scv_touch_marketing_channel t ON b.touch_id = t.touch_id
         WHERE t.touch_affiliate_territory IN ('UK', 'IT', 'DE')
           AND b.touch_experience != 'native app'
           AND b.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2, 3
     )
SELECT g.week_start::DATE                        AS week_start,
       g.week_end::DATE                          AS week_end,
       t.touch_affiliate_territory,
       count(DISTINCT b.attributed_user_id_hash) AS wau,
       count(DISTINCT b.touch_id)                AS sessions,
       count(DISTINCT s.event_hash)              AS spvs,
       m.mau
FROM grain g
         LEFT JOIN se.data.scv_touch_basic_attributes b
                   ON g.week_start <= b.touch_start_tstamp::DATE
                       AND g.week_end >= b.touch_start_tstamp::DATE
         LEFT JOIN se.data.scv_touch_marketing_channel t ON b.touch_id = t.touch_id
         LEFT JOIN se.data.scv_touched_spvs s ON b.touch_id = s.touch_id
         LEFT JOIN mau m ON g.week_start = m.week_start AND t.touch_affiliate_territory = m.touch_affiliate_territory
WHERE t.touch_affiliate_territory IN ('UK', 'IT', 'DE')
  AND b.touch_experience != 'native app'
  AND b.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 7
ORDER BY 1, 3
;


SET (from_date, to_date)= ('2020-05-01', '2020-05-07');
SELECT $from_date                                AS from_date,
       $to_date                                  AS to_date,
       t.touch_affiliate_territory,
       count(DISTINCT b.attributed_user_id_hash) AS active_users,
       count(DISTINCT b.touch_id)                AS sessions,
       count(DISTINCT s.event_hash)              AS spvs

FROM se.data.scv_touch_basic_attributes b
         LEFT JOIN se.data.scv_touched_spvs s ON b.touch_id = s.touch_id
         LEFT JOIN se.data.scv_touch_marketing_channel t ON b.touch_id = t.touch_id
WHERE t.touch_affiliate_territory IN ('UK', 'IT', 'DE')
  AND b.touch_experience != 'native app'
  AND b.stitched_identity_type = 'se_user_id'
  AND b.touch_start_tstamp::DATE >= $from_date
  AND b.touch_start_tstamp::DATE <= $to_date

GROUP BY 1, 2, 3;
