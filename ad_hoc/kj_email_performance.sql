SET (campaign_date, days_add)= ('2021-06-25', 2);

WITH crm_data AS (

    WITH send_ids AS (
        SELECT column1 AS send_id
        FROM (VALUES ('1227552'), -- INSERT RELEVANT CRM CAMPAIGN IDs HERE
                     ('1227558')
                 )
    ),

         opens AS (
             SELECT shiro_user_id::VARCHAR        AS shiro_user_id,
                    send_id,
                    COUNT(DISTINCT shiro_user_id) AS unique_email_opens,
                    COUNT(*)                      AS email_opens
             FROM se.data_pii.crm_events_opens
             WHERE send_id IN (
                 SELECT send_id
                 FROM send_ids
             )
               AND event_date::DATE >= $campaign_date::DATE
               AND event_date <= DATEADD('day', $days_add, $campaign_date::DATE) -- between '2021-05-13' and '2021-05-14'
             GROUP BY 1, 2
         )
            ,
         email_sends AS (
             SELECT send_id,
                    shiro_user_id::VARCHAR AS shiro_user_id,
                    COUNT(*)               AS email_sends
             FROM se.data_pii.crm_events_sends
             WHERE send_id IN ('1227552', '1227558')                             -- INSERT RELEVANT CRM CAMPAIGN ID HERE
               AND event_date::DATE >= $campaign_date::DATE
               AND event_date <= DATEADD('day', $days_add, $campaign_date::DATE) -- between '2021-05-13' and '2021-05-14'
             GROUP BY 1, 2
         ),
         clicks AS (
             SELECT send_id,
                    shiro_user_id::VARCHAR        AS shiro_user_id,
                    COUNT(DISTINCT shiro_user_id) AS unique_email_clicks,
                    COUNT(*)                      AS email_clicks
             FROM se.data_pii.crm_events_clicks
             WHERE send_id IN (
                 SELECT send_id
                 FROM send_ids
             )
               AND event_date::DATE >= $campaign_date::DATE
               AND event_date <= DATEADD('day', $days_add, $campaign_date::DATE) -- between '2021-05-13' and '2021-05-14'
             GROUP BY 1, 2
         )

    SELECT s.shiro_user_id,
           s.send_id,
           SUM(s.email_sends)         AS sends,
           SUM(o.unique_email_opens)  AS unique_opens,
           SUM(o.email_opens)         AS opens,
           SUM(c.unique_email_clicks) AS unique_clicks,
           SUM(c.email_clicks)        AS clicks
    FROM email_sends s
             LEFT JOIN opens o ON s.send_id = o.send_id AND s.shiro_user_id = o.shiro_user_id
             LEFT JOIN clicks c ON c.send_id = o.send_id AND c.shiro_user_id = o.shiro_user_id

    GROUP BY 1, 2
),

     site_data AS (
         WITH spvs AS (
             SELECT touch_id,
                    COUNT(*) AS spvs
             FROM se.data.scv_touched_spvs
             WHERE event_tstamp::DATE >= $campaign_date::DATE
               AND event_tstamp::DATE <=
                   DATEADD('day', $days_add, $campaign_date::DATE) -- between '2021-05-13' and '2021-05-14'
             GROUP BY 1
         ),

              bookings AS (
                  SELECT tt.touch_id,
                         COUNT(DISTINCT tt.booking_id)                AS bookings,
                         SUM(fcb.margin_gross_of_toms_cc)             AS margin,
                         SUM(fcb.gross_booking_value_gbp)             AS gross_booking_value,
                         SUM(fcb.gross_revenue_gbp_constant_currency) AS gross_revenue

                  FROM se.data.scv_touched_transactions tt
                           JOIN se.data.fact_complete_booking fcb ON fcb.booking_id = tt.booking_id
                  WHERE event_tstamp::DATE >= $campaign_date::DATE
                    AND event_tstamp::DATE <=
                        DATEADD('day', $days_add, $campaign_date::DATE) -- between '2021-05-13' and '2021-05-14'
                  GROUP BY 1
              ),

              sessions AS (
                  SELECT tba.touch_id,
                         spv.spvs                AS spvs,
                         bkg.bookings            AS bookings,
                         bkg.margin              AS margin,
                         bkg.gross_booking_value AS gross_booking_value,
                         bkg.gross_revenue       AS gross_revenue

                  FROM se.data_pii.scv_touch_basic_attributes tba
                           LEFT JOIN spvs spv ON spv.touch_id = tba.touch_id
                           LEFT JOIN bookings bkg ON bkg.touch_id = tba.touch_id
                  WHERE tba.touch_start_tstamp::DATE >= $campaign_date::DATE
                    AND tba.touch_start_tstamp::DATE <=
                        DATEADD('day', $days_add, $campaign_date::DATE) -- between '2021-05-13' and '2021-05-14'
              )

         SELECT tba.attributed_user_id,
                COUNT(DISTINCT tba.touch_id)                   AS sessions,
                SUM(s.spvs)                                    AS spvs,
                SUM(s.bookings)                                AS bookings,
                SUM(s.margin)                                  AS margin,
                ROUND(SUM(s.gross_booking_value), 2)           AS gross_booking_value,
                ROUND(SUM(s.gross_revenue), 2)                 AS gross_revenue,
                ROUND(AVG(tba.touch_duration_seconds) / 60, 2) AS avg_sess_lgt


         FROM se.data_pii.scv_touch_basic_attributes tba
                  JOIN sessions s ON s.touch_id = tba.touch_id


         WHERE tba.touch_start_tstamp::DATE >= $campaign_date::DATE
           AND tba.touch_start_tstamp::DATE <= DATEADD('day', $days_add, $campaign_date::DATE)
           AND tba.touch_hostname_territory = 'UK'
         GROUP BY 1
     )


SELECT c.send_id,
       SUM(c.sends)                         AS sends,
       SUM(c.unique_opens)                  AS unique_opens,
       SUM(c.opens)                         AS opens,
       SUM(c.unique_clicks)                 AS unique_clicks,
       SUM(c.clicks)                        AS clicks,
       SUM(s.sessions)                      AS sessions,
       SUM(s.spvs)                          AS spvs,
       SUM(s.bookings)                      AS bookings,
       ROUND(SUM(s.margin), 2)              AS margin,
       ROUND(SUM(s.gross_booking_value), 2) AS gross_bk_value,
       ROUND(SUM(s.gross_revenue), 2)       AS gross_rev,
       ROUND(AVG(avg_sess_lgt), 2)          AS avg_ses_lgt

FROM crm_data c
         LEFT JOIN site_data s ON s.attributed_user_id = c.shiro_user_id
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------
SET (campaign_date, days_add)= ('2021-06-25', 2);

WITH input_sends AS (
    -- purpose of this query is to output a single list of send_ids
    -- this then filters down into subsequent CTES
    SELECT column1 AS send_id
    FROM (VALUES ('1227552'), -- INSERT RELEVANT CRM CAMPAIGN IDs HERE
                 ('1227558')
             )
),
     sends AS (
         SELECT es.send_id,
                cjl.email_name,
                cjl.mapped_objective,
                cjl.mapped_territory,
                COUNT(*) AS email_sends
         FROM se.data_pii.crm_events_sends es
                  INNER JOIN se.data.crm_jobs_list cjl ON es.send_id = cjl.send_id
         WHERE es.send_id IN (
             SELECT DISTINCT send_id
             FROM input_sends
         )
           AND es.event_date::DATE >= $campaign_date::DATE
           AND es.event_date <= DATEADD('day', $days_add, $campaign_date::DATE)
         GROUP BY 1, 2, 3, 4
     ),
     opens AS (
         SELECT send_id,
                COUNT(DISTINCT shiro_user_id) AS unique_email_opens,
                COUNT(*)                      AS email_opens
         FROM se.data_pii.crm_events_opens
         WHERE send_id IN (
             SELECT DISTINCT send_id
             FROM input_sends
         )
           AND event_date::DATE >= $campaign_date::DATE
           AND event_date <= DATEADD('day', $days_add, $campaign_date::DATE)
         GROUP BY 1
     ),
     clicks AS (
         SELECT send_id,
                COUNT(DISTINCT shiro_user_id) AS unique_email_clicks,
                COUNT(*)                      AS email_clicks
         FROM se.data_pii.crm_events_clicks
         WHERE send_id IN (
             SELECT DISTINCT send_id
             FROM input_sends
         )
           AND event_date::DATE >= $campaign_date::DATE
           AND event_date <= DATEADD('day', $days_add, $campaign_date::DATE)
         GROUP BY 1
     ),
     unsubs AS (
         SELECT send_id,
                COUNT(*) AS email_unsubs
         FROM se.data_pii.crm_events_unsubscribes ceu
         WHERE send_id IN (
             SELECT DISTINCT send_id
             FROM input_sends
         )
           AND event_date::DATE >= $campaign_date::DATE
           AND event_date <= DATEADD('day', $days_add, $campaign_date::DATE)
         GROUP BY 1
     ),
     spvs AS (
         SELECT stmc.utm_campaign AS send_id,
                COUNT(*)          AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
         WHERE event_tstamp::DATE >= $campaign_date::DATE
           AND event_tstamp::DATE <= DATEADD('day', $days_add, $campaign_date::DATE)
           AND TRY_TO_NUMBER(stmc.utm_campaign) IN (
             SELECT DISTINCT send_id
             FROM input_sends
         )
         GROUP BY 1
     ),
     bookings AS (
         SELECT stmc.utm_campaign                                 AS send_id,
                COUNT(DISTINCT tt.booking_id)                     AS bookings,
                SUM(IFF(fcb.travel_type = 'Domestic', 1, 0))      AS domestic_bookings,
                SUM(IFF(fcb.travel_type = 'International', 1, 0)) AS international_bookings,
                SUM(fcb.margin_gross_of_toms_cc)                  AS margin,
                SUM(fcb.gross_booking_value_gbp)                  AS gross_booking_value,
                SUM(fcb.gross_revenue_gbp_constant_currency)      AS gross_revenue
         FROM se.data.scv_touched_transactions tt
                  INNER JOIN se.data.fact_complete_booking fcb ON fcb.booking_id = tt.booking_id
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON tt.touch_id = stmc.touch_id
         WHERE event_tstamp::DATE >= $campaign_date::DATE
           AND event_tstamp::DATE <= DATEADD('day', $days_add, $campaign_date::DATE)
           AND TRY_TO_NUMBER(stmc.utm_campaign) IN (
             SELECT DISTINCT send_id
             FROM input_sends
         )
         GROUP BY 1
     ),
     sessions AS (
         SELECT stmc.utm_campaign AS send_id,
                COUNT(*)          AS sessions
         FROM se.data_pii.scv_touch_basic_attributes tba
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON tba.touch_id = stmc.touch_id
         WHERE tba.touch_start_tstamp::DATE >= $campaign_date::DATE
           AND tba.touch_start_tstamp::DATE <=
               DATEADD('day', $days_add, $campaign_date::DATE)
           AND TRY_TO_NUMBER(stmc.utm_campaign) IN (
             SELECT DISTINCT send_id
             FROM input_sends
         )
         GROUP BY 1
     )
SELECT ins.send_id,
       s.email_name,
       s.mapped_objective,
       s.mapped_territory,
       $campaign_date::DATE                            AS data_start,
       DATEADD('day', $days_add, $campaign_date::DATE) AS data_end,
       s.email_sends,
       o.unique_email_opens,
       o.email_opens,
       c.unique_email_clicks,
       c.email_clicks,
       uns.email_unsubs,
       ss.sessions,
       spvs.spvs,
       b.bookings,
       b.domestic_bookings,
       b.international_bookings,
       b.margin,
       b.gross_booking_value,
       b.gross_revenue
FROM input_sends ins
         LEFT JOIN sends s ON ins.send_id = s.send_id
         LEFT JOIN opens o ON ins.send_id = o.send_id
         LEFT JOIN clicks c ON ins.send_id = c.send_id
         LEFT JOIN unsubs uns ON ins.send_id = uns.send_id
         LEFT JOIN sessions ss ON ins.send_id = ss.send_id
         LEFT JOIN spvs ON ins.send_id = spvs.send_id
         LEFT JOIN bookings b ON ins.send_id = b.send_id;



SELECT TRY_TO_NUMBER(j.send_id) AS send_id
FROM se.data.crm_jobs_list j
WHERE j.sent_date >= $campaign_date
  AND j.mapped_territory IN ('UK')
  AND j.mapped_objective IN ('PARTNER')


SELECT mts.event_tstamp::DATE, COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE mts.event_category = 'web redirect'
GROUP BY 1;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE mts.event_category = 'web redirect';


