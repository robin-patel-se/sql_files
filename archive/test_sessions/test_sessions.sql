SELECT regexp_substr(stba.touch_landing_page, '[/|&]gce_(.*)=', 1, 1, 'e') AS test,
       stba.touch_landing_pagepath,
       stba.touch_landing_page

FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_landing_pagepath LIKE '%gce_%'


WITH test_sessions AS (
    --sessions that have a param in the query string that is prefixed 'gce_'
    SELECT stba.touch_id,
           stba.attributed_user_id,
           stba.touch_start_tstamp,
           stba.touch_landing_pagepath,
           stba.touch_landing_page,
           PARSE_URL(stba.touch_landing_page)['parameters'] AS parameters,
           params.path                                      AS test,
           params.value::VARCHAR                            AS test_group

    FROM se.data_pii.scv_touch_basic_attributes stba,
         LATERAL FLATTEN(INPUT => PARSE_URL(stba.touch_landing_page)['parameters'], OUTER => TRUE) params
    WHERE stba.touch_landing_page LIKE '%gce_%'
      AND params.key LIKE 'gce_bfeeuk%'
      AND stba.touch_start_tstamp::DATE >= current_date - 10
      AND stba.touch_hostname = 'www.secretescapes.com'
)
SELECT ts.test,
       ts.test_group,
       COUNT(*) AS sessions
FROM test_sessions ts
GROUP BY 1, 2;


SELECT *
FROM se.data.fact_complete_booking fcb
         LEFT JOIN se.data.se_booking sb ON fcb.booking_id = sb.booking_id
WHERE fcb.booking_completed_date >= CURRENT_DATE - 10
  AND fcb.booking_fee_net_rate_gbp > 0
  AND sb.territory = 'UK';

------------------------------------------------------------------------------------------------------------------------
--cian's original code
SELECT e.user_id,
       MAX(CASE WHEN e.page_urlquery LIKE '%gce_bfeeuk=0%' THEN 1 ELSE 0 END) AS has_seen_version_0,
       MAX(CASE WHEN e.page_urlquery LIKE '%gce_bfeeuk=1%' THEN 1 ELSE 0 END) AS has_seen_version_1
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= '2019-06-13'
  AND e.collector_tstamp <= '2019-07-03'
  AND e.page_urlquery LIKE '%gce_bfeeuk=%'
  AND e.app_id = 'UK' -- < identified that this is flawed
  AND e.page_urlhost = 'www.secretescapes.com'
  AND e.br_type != 'Robot'
  AND e.user_ipaddress != '89.197.56.3'
GROUP BY e.user_id


------------------------------------------------------------------------------------------------------------------------
--sessions that have a param in the query string that is prefixed 'gce_perbfee' on the staging environment
SELECT stba.touch_id,
       stba.attributed_user_id,
       stba.touch_start_tstamp,
       stba.touch_landing_pagepath,
       stba.touch_landing_page,
       PARSE_URL(stba.touch_landing_page)['parameters'] AS parameters,
       params.path                                      AS test,
       params.value::VARCHAR                            AS test_group

FROM se.data_pii.scv_touch_basic_attributes stba,
     LATERAL FLATTEN(INPUT
                     => PARSE_URL(stba.touch_landing_page)['parameters'], OUTER => TRUE) params
WHERE stba.touch_landing_page LIKE '%gce_%'
  AND params.key LIKE 'gce_perbfee%'
  AND stba.touch_start_tstamp::DATE >= current_date - 10
  AND stba.touch_hostname = 'booking-fees.fs-staging.escapes.tech' --TODO replace this when test goes live
  ;

--sessions and bookings
WITH test_sessions AS (
    --sessions that have a param in the query string that is prefixed 'gce_'
    SELECT stba.touch_id,
           stba.attributed_user_id,
           stba.touch_start_tstamp,
           stba.touch_landing_pagepath,
           stba.touch_landing_page,
           PARSE_URL(stba.touch_landing_page)['parameters'] AS parameters,
           params.path                                      AS test,
           params.value::VARCHAR                            AS test_group

    FROM se.data_pii.scv_touch_basic_attributes stba,
         LATERAL FLATTEN(INPUT
                         => PARSE_URL(stba.touch_landing_page)['parameters'], OUTER => TRUE) params
    WHERE stba.touch_landing_page LIKE '%gce_%'
      AND params.key LIKE 'gce_perbfee%'
      AND stba.touch_start_tstamp::DATE >= current_date - 10
      AND stba.touch_hostname = 'booking-fees.fs-staging.escapes.tech' --TODO replace this when test goes live
)
SELECT ts.touch_id,
       ts.attributed_user_id,
       ts.touch_start_tstamp,
       ts.touch_landing_pagepath,
       ts.touch_landing_page,
       ts.parameters,
       ts.test,
       ts.test_group,
       stt.event_tstamp AS booking_timestamp,
       stt.booking_id,
       fcb.customer_total_price_gbp,
       fcb.margin_gross_of_toms_gbp,
       fcb.booking_fee_net_rate_gbp
FROM test_sessions ts
         LEFT JOIN se.data.scv_touched_transactions stt ON ts.touch_id = stt.touch_id
         LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id;

------------------------------------------------------------------------------------------------------------------------
--aggregated view of sessions and bookings with cvr and financials
WITH test_sessions AS (
    --sessions that have a param in the query string that is prefixed 'gce_'
    SELECT stba.touch_id,
           stba.attributed_user_id,
           stba.touch_start_tstamp,
           stba.touch_landing_pagepath,
           stba.touch_landing_page,
           PARSE_URL(stba.touch_landing_page)['parameters'] AS parameters,
           params.path                                      AS test,
           params.value::VARCHAR                            AS test_group

    FROM se.data_pii.scv_touch_basic_attributes stba,
         LATERAL FLATTEN(INPUT
                         => PARSE_URL(stba.touch_landing_page)['parameters'], OUTER => TRUE) params
    WHERE stba.touch_landing_page LIKE '%gce_%'
      AND params.key LIKE 'gce_perbfee%'
      AND stba.touch_start_tstamp::DATE >= current_date - 10
      AND stba.touch_hostname = 'booking-fees.fs-staging.escapes.tech' --TODO replace this when test goes live
),
     sessions_with_bookings AS (
         SELECT ts.touch_id,
                ts.attributed_user_id,
                ts.touch_start_tstamp,
                ts.touch_landing_pagepath,
                ts.touch_landing_page,
                ts.parameters,
                ts.test,
                ts.test_group,
                stt.event_tstamp AS booking_timestamp,
                stt.booking_id,
                fcb.customer_total_price_gbp,
                fcb.margin_gross_of_toms_gbp,
                fcb.booking_fee_net_rate_gbp
         FROM test_sessions ts
                  LEFT JOIN se.data.scv_touched_transactions stt ON ts.touch_id = stt.touch_id
                  LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
     )
SELECT swb.test_group,
       COUNT(DISTINCT swb.booking_id)                                                AS bookings,
       COUNT(DISTINCT swb.touch_id)                                                  AS sessions,
       COUNT(DISTINCT IFF(swb.booking_id IS NOT NULL, swb.attributed_user_id, NULL)) AS users_with_a_booking,
       COUNT(DISTINCT swb.attributed_user_id)                                        AS users,
       bookings / users                                                              AS original_cvr,
       bookings / sessions                                                           AS session_cvr,
       users_with_a_booking / users                                                  AS user_cvr,
       SUM(swb.customer_total_price_gbp)                                             AS customer_total_price_gbp,
       SUM(swb.margin_gross_of_toms_gbp)                                             AS margin_gross_of_toms_gbp,
       SUM(swb.booking_fee_net_rate_gbp)                                             AS booking_fee_net_rate_gbp
FROM sessions_with_bookings swb
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------

--sessions that have a param in the query string that is prefixed 'gce_perbfee' on the staging environment
SELECT stba.*,
       PARSE_URL(stba.touch_landing_page)['parameters'] AS parameters,
       params.path                                      AS test,
       params.value::VARCHAR                            AS test_group

FROM se.data_pii.scv_touch_basic_attributes stba,
     LATERAL FLATTEN(INPUT
                     => PARSE_URL(stba.touch_landing_page)['parameters'], OUTER => TRUE) params
WHERE stba.touch_landing_page LIKE '%gce_%'
  AND params.key LIKE 'gce_perbfee%'
  AND stba.touch_start_tstamp::DATE >= current_date - 10
--TODO replace this when test goes live

