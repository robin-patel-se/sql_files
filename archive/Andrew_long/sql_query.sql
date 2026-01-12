WITH
// Identify the users (from a known email send) who fall in to the groups we are looking to analyse
user_ids AS (
    SELECT DISTINCT
           s.shiro_user_id
         , CASE
               WHEN e.data_source_name IN ('SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A', 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_B')
                   THEN 'UK Active'
               WHEN e.data_source_name IN ('SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_C', 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_D')
                   THEN 'UK Generic'
               END AS member_group
    FROM se.data.crm_events_sends s
             JOIN se.data.crm_email_segments e ON e.email_segment_key = s.email_segment_key
    WHERE s.send_id IN (1196070, 1196071)
),
// Get all SEND_IDs of "core" emails from the last week
send_ids AS (
    SELECT DISTINCT
           s.send_id
         , s.shiro_user_id
         , s.event_date
    FROM se.data.crm_events_sends s
             JOIN se.data.crm_jobs_list c
                  ON c.send_id = s.send_id
                      AND c.sent_date = s.event_date
         --RP: this join will only return send events for sends that occurred on the date that the email was sent,
         --this may seem logical however due to batching, there may be instances where the send date on the email
         --might be different to that of the send events
    WHERE c.mapped_objective = 'CORE'
      AND s.event_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE
),
// For the sends identified above, get the opens
opens AS (
    SELECT DISTINCT
           send_id
         , shiro_user_id
    FROM se.data.crm_events_opens
    WHERE send_id IN (
        SELECT DISTINCT send_id
        FROM send_ids
    )
),
// For the sends identified above, get the clicks
clicks AS (
    SELECT DISTINCT
           send_id
         , shiro_user_id
    FROM se.data.crm_events_clicks
    WHERE send_id IN (
        SELECT DISTINCT send_id
        FROM send_ids
    )
),
// Join users, sends, opens and clicks together to understand email performance for the different groups
crm_data AS (
    SELECT COALESCE(u.member_group, 'Other') AS member_group
         , COUNT(DISTINCT u.shiro_user_id)   AS members_in_group
         , COUNT(DISTINCT s.shiro_user_id)   AS members_sent_email
         , COUNT(s.*)                        AS core_sends
         , core_sends / members_sent_email   AS weekly_core_sends_per_member
         , COUNT(o.*)                        AS unique_opens
         , unique_opens / core_sends         AS open_rate
         , COUNT(c.*)                        AS unique_clicks
         , unique_clicks / unique_opens      AS unique_ctor
    FROM send_ids s
             LEFT JOIN user_ids u
                       ON s.shiro_user_id = u.shiro_user_id
             LEFT JOIN opens o
                       ON s.send_id = o.send_id
                           AND s.shiro_user_id = o.shiro_user_id
             LEFT JOIN clicks c
                       ON s.send_id = c.send_id
                           AND s.shiro_user_id = c.shiro_user_id
    GROUP BY 1
    ORDER BY 1
),
// Getting bookings data for the last week, split in to groups
booking_data AS (
    SELECT COALESCE(u.member_group, 'Other')                      AS member_group
         , COUNT(DISTINCT b.booking_id)                           AS unique_weekly_completed_bookings
         , COUNT(DISTINCT b.shiro_user_id)                        AS unique_weekly_bookers
         , SUM(b.margin_gross_of_toms_gbp)                        AS total_weekly_margin
         , total_weekly_margin / unique_weekly_completed_bookings AS margin_per_booking
         , SUM(b.rooms) / unique_weekly_completed_bookings        AS avg_rooms_per_booking
         , SUM(b.no_nights) / unique_weekly_completed_bookings    AS avg_nights_per_booking
         , total_weekly_margin / SUM(b.rooms * b.no_nights)       AS margin_per_room_per_night
    FROM se.data.fact_booking b
             LEFT JOIN user_ids u
                       ON b.shiro_user_id = u.shiro_user_id
    WHERE b.booking_created_date BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE
      AND b.booking_status = 'COMPLETE'
    GROUP BY 1
    ORDER BY 1
)
// Join CRM and BOOKING data
SELECT a.*
     , b.*
FROM crm_data a
         LEFT JOIN booking_data b
                   ON a.member_group = b.member_group