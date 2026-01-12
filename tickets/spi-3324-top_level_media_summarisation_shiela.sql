USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 10
  AND stmc.referrer_hostname = 'mp.secretescapes.com';

-- mp traffic currently looks like direct traffic

SELECT
    DATE_TRUNC('month', stba.touch_start_tstamp) AS month,
    COUNT(*)                                     AS sessions
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2022-01-01'
  AND stmc.referrer_hostname = 'mp.secretescapes.com'
GROUP BY 1;



WITH bookings AS (
    SELECT
        DATE_TRUNC('month', stt.event_tstamp)               AS month,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings,
        SUM(fcb.gross_revenue_gbp_constant_currency)        AS gross_revenue,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.scv_touch_marketing_channel stmc
        INNER JOIN se.data.scv_touched_transactions stt ON stmc.touch_id = stt.touch_id
        INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stt.event_tstamp >= '2022-01-01'
      AND stmc.referrer_hostname = 'mp.secretescapes.com'
    GROUP BY 1
),
     spvs AS (
         SELECT
             DATE_TRUNC('month', sts.event_tstamp) AS month,
             COUNT(DISTINCT sts.event_hash)        AS spvs
         FROM se.data.scv_touch_marketing_channel stmc
             INNER JOIN se.data.scv_touched_spvs sts ON stmc.touch_id = sts.touch_id
         WHERE sts.event_tstamp >= '2022-01-01'
           AND stmc.referrer_hostname = 'mp.secretescapes.com'
         GROUP BY 1
     ),
     sessions AS (
         SELECT
             DATE_TRUNC('month', stba.touch_start_tstamp) AS month,
             COUNT(DISTINCT stmc.touch_id)                AS sessions
         FROM se.data.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         WHERE stba.touch_start_tstamp >= '2022-01-01'
           AND stmc.referrer_hostname = 'mp.secretescapes.com'
         GROUP BY 1
     )

SELECT COALESCE(s.month, ss.month, b.month) AS month,
       s.sessions,
       ss.spvs,
       b.bookings,
       b.gross_revenue,
       b.margin
FROM sessions s
FULL OUTER JOIN spvs ss ON s.month = ss.month
FULL OUTER JOIN bookings b ON COALESCE(s.month, ss.month) = b.month
;

