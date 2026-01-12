WITH event_bounces_hygiene AS (
    SELECT
        -- (lineage) original metadata columns from previous step
        es.loaded_at                                                 AS row_loaded_at,
        es.filename                                                  AS row_filename,
        es.file_row_number                                           AS row_file_row_number,
        -- hygiene columns
        --create a event hash to ease dedupe in snapshot step. Also allow
        --unique identifier for events, include event type so we can ensure
        --no conflicts between different event types
        SHA2(
                    COALESCE(es.send_id::VARCHAR, '') ||
                    COALESCE(es.subscriber_key::VARCHAR, '') ||
                    COALESCE(es.subscriber_id::VARCHAR, '') ||
                    COALESCE(es.list_id::VARCHAR, '') ||
                    COALESCE(es.event_date, '1970-01-01 00:00:00') ||
                    COALESCE(es.event_type, '') || --in case we want to combine event types
                    COALESCE(es.batch_id::VARCHAR, '')
            )                                                        AS event_hash,
        COALESCE(TRY_TO_NUMBER(es.subscriber_key), ua.shiro_user_id) AS shiro_user_id,
        TRY_TO_TIMESTAMP(es.event_date::VARCHAR)                     AS event_tstamp,
        es.bounce_category
    FROM raw_vault_mvp.sfmc.events_bounces es
        LEFT JOIN data_vault_mvp.dwh.user_attributes ua ON es.email_address = ua.email
        AND TRY_TO_NUMBER(es.subscriber_key) IS NULL
),
     event_bounces_hygiene_snapshot AS (
         SELECT ebh.shiro_user_id,
                ebh.event_tstamp,
                ebh.bounce_category
         FROM event_bounces_hygiene ebh
             QUALIFY ROW_NUMBER() OVER (
                 PARTITION BY event_hash
                 ORDER BY
                     row_loaded_at DESC,
                     row_filename DESC,
                     row_file_row_number DESC) = 1
     ),
     held_users AS (
         SELECT ub.shiro_user_id,
                SUM(IFF(ub.bounce_category IN ('Hard bounce', 'Soft bounce'), 1, 0))                  AS total_bounces,
                MIN(IFF(ub.bounce_category IN ('Hard bounce', 'Soft bounce'), ub.event_tstamp, NULL)) AS first_bounce_tstamp
         FROM event_bounces_hygiene_snapshot ub
         GROUP BY 1
         HAVING total_bounces >= 3
            AND first_bounce_tstamp < CURRENT_DATE() - 15
     ),
     bookings AS (
         SELECT u.shiro_user_id,
                COUNT(DISTINCT booking_id) AS lifetime_bookings
         FROM data_vault_mvp.dwh.user_attributes u
             LEFT JOIN data_vault_mvp.dwh.fact_booking fb ON u.shiro_user_id = fb.shiro_user_id AND fb.booking_status_type IN ('live', 'cancelled')
         GROUP BY 1
     )
SELECT ura.shiro_user_id,
       b.lifetime_bookings,
       ua.signup_tstamp,
       ura.last_session_end_tstamp,
       ura.last_email_open_tstamp,
       ua.membership_account_status,
       h.total_bounces,
       h.first_bounce_tstamp,
       GREATEST(ua.signup_tstamp,
                IFNULL(ura.last_session_end_tstamp, '1970-01-01'),
                IFNULL(ura.last_email_open_tstamp, '1970-01-01')) AS crm_last_activity_tstamp
FROM data_vault_mvp.dwh.user_recent_activities ura
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON ura.shiro_user_id = ua.shiro_user_id
    INNER JOIN bookings b ON ura.shiro_user_id = b.shiro_user_id
    INNER JOIN held_users h ON ura.shiro_user_id = h.shiro_user_id
WHERE DATEDIFF(MONTH, crm_last_activity_tstamp, CURRENT_DATE) > 60
  AND ua.membership_account_status IS DISTINCT FROM 'DELETED'
  AND ua.current_affiliate_territory IS DISTINCT FROM 'US'
  AND COALESCE(b.lifetime_bookings, 0) = 0;



USE WAREHOUSE pipe_xlarge;

SELECT MIN(last_session_end_tstamp)
FROM data_vault_mvp.dwh.user_recent_activities ura;

--query outputs 1.9M, some of which are probably already deleted
--when we strip out deleted 493K





