WITH spvs AS (
    SELECT touch_id,
           COUNT(DISTINCT event_hash) AS spvs
    FROM se.data.scv_touched_spvs
    WHERE event_tstamp >= '2021-01-01'
      AND event_tstamp < '2021-02-01'
    GROUP BY 1
),

     bookings AS (
         SELECT tt.touch_id,
                COUNT(DISTINCT tt.booking_id)    AS bookings,
                SUM(fcb.margin_gross_of_toms_cc) AS margin
         FROM se.data.scv_touched_transactions tt
                  JOIN se.data.fact_complete_booking fcb ON fcb.booking_id = tt.booking_id
         WHERE event_tstamp >= '2021-01-01'
           AND event_tstamp < '2021-02-01'
         GROUP BY 1
     ),


     sessions AS (
         SELECT tba.touch_id,
                spv.spvs     AS spvs,
                bkg.bookings AS bookings,
                bkg.margin   AS margin
         FROM se.data_pii.scv_touch_basic_attributes tba
                  LEFT JOIN spvs spv ON spv.touch_id = tba.touch_id
                  LEFT JOIN bookings bkg ON bkg.touch_id = tba.touch_id

         WHERE tba.touch_start_tstamp >= '2021-01-01'
           AND tba.touch_start_tstamp < '2021-02-01'
     )

SELECT tba.touch_landing_page                                                                                            AS landing_page,
       tba.touch_landing_pagepath                                                                                        AS landing_page_path,
       tba.touch_referrer_url,
--tba.TOUCH_REFERRER_URL as Referrer_Query,
       CASE
           WHEN CHARINDEX('&', tba.touch_referrer_url) = 0 THEN
               SUBSTRING(tba.touch_referrer_url, CHARINDEX('query=', tba.touch_referrer_url) + 6,
                         (CHARINDEX('query=', tba.touch_referrer_url) + 6))
           ELSE
               SUBSTRING(tba.touch_referrer_url, CHARINDEX('query=', tba.touch_referrer_url) + 6,
                         CHARINDEX('&', tba.touch_referrer_url) - (CHARINDEX('query=', tba.touch_referrer_url) + 6)) END AS query,
       COUNT(tba.touch_id)                                                                                               AS sessions,
       SUM(s.spvs)                                                                                                       AS spvs,
       SUM(s.bookings)                                                                                                   AS bookings,
       SUM(s.margin)                                                                                                     AS margin,
       CAST(SUM(s.spvs) / COUNT(tba.touch_id) AS dec(10, 2))                                                             AS spv_per_sess,
       CAST(SUM(s.margin) / COUNT(tba.touch_id) AS dec(10, 2))                                                           AS gpv,
       CAST(AVG(tba.touch_duration_seconds) / 60 AS dec(10, 2))                                                          AS avg_sess_lgt,
       CAST(SUM(s.margin) / SUM(s.bookings) AS dec(10, 2))                                                               AS aov

FROM se.data_pii.scv_touch_basic_attributes tba
         JOIN sessions s ON s.touch_id = tba.touch_id

WHERE tba.touch_start_tstamp >= '2021-01-01'
  AND tba.touch_start_tstamp < '2021-02-01'
  AND tba.touch_hostname_territory = 'UK'
  AND tba.touch_referrer_url LIKE '%search?query%'

GROUP BY 1, 2, 3;

SELECT * FROm data_vault_mvp.dwh.tb_booking tb;