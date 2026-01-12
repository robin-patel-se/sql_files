USE WAREHOUSE pipe_xlarge;

SELECT ua.date,
       COUNT(CASE WHEN ua.emails_1d > 0 THEN 1 END)                                                        AS email_active_1d,
       COUNT(CASE WHEN ua.emails_7d > 0 THEN 1 END)                                                        AS email_active_7d,
       COUNT(CASE WHEN ua.emails_14d > 0 THEN 1 END)                                                       AS email_active_14d,
       COUNT(CASE WHEN ua.emails_30d > 0 THEN 1 END)                                                       AS email_active_30d,
       COUNT(CASE WHEN ua.emails_90d > 0 THEN 1 END)                                                       AS email_active_90d,

       COUNT(CASE WHEN u.subscriberkey IS NOT NULL AND ua.emails_1d > 0 THEN 1 END)                        AS added_back_email_active_1d,
       COUNT(CASE WHEN u.subscriberkey IS NOT NULL AND ua.emails_7d > 0 THEN 1 END)                        AS added_back_email_active_7d,
       COUNT(CASE WHEN u.subscriberkey IS NOT NULL AND ua.emails_14d > 0 THEN 1 END)                       AS added_back_email_active_14d,
       COUNT(CASE WHEN u.subscriberkey IS NOT NULL AND ua.emails_30d > 0 THEN 1 END)                       AS added_back_email_active_30d,
       COUNT(CASE WHEN u.subscriberkey IS NOT NULL AND ua.emails_90d > 0 THEN 1 END)                       AS added_back_email_active_90d,

       COUNT(CASE WHEN ua.web_sessions_1d > 0 OR ua.app_sessions_1d > 0 OR ua.emails_1d > 0 THEN 1 END)    AS active_1d,
       COUNT(CASE WHEN ua.web_sessions_7d > 0 OR ua.app_sessions_7d > 0 OR ua.emails_7d > 0 THEN 1 END)    AS active_7d,
       COUNT(CASE WHEN ua.web_sessions_14d > 0 OR ua.app_sessions_14d > 0 OR ua.emails_14d > 0 THEN 1 END) AS active_14d,
       COUNT(CASE WHEN ua.web_sessions_30d > 0 OR ua.app_sessions_30d > 0 OR ua.emails_30d > 0 THEN 1 END) AS active_30d,
       COUNT(CASE WHEN ua.web_sessions_90d > 0 OR ua.app_sessions_90d > 0 OR ua.emails_90d > 0 THEN 1 END) AS active_90d,

       COUNT(CASE
                 WHEN u.subscriberkey IS NOT NULL AND (ua.web_sessions_1d > 0 OR ua.app_sessions_1d > 0 OR ua.emails_1d > 0)
                     THEN 1 END)                                                                           AS added_back_users_active_1d,
       COUNT(CASE
                 WHEN u.subscriberkey IS NOT NULL AND (ua.web_sessions_7d > 0 OR ua.app_sessions_7d > 0 OR ua.emails_7d > 0)
                     THEN 1 END)                                                                           AS added_back_users_active_7d,
       COUNT(CASE
                 WHEN u.subscriberkey IS NOT NULL AND (ua.web_sessions_14d > 0 OR ua.app_sessions_14d > 0 OR ua.emails_14d > 0)
                     THEN 1 END)                                                                           AS added_back_users_active_14d,
       COUNT(CASE
                 WHEN u.subscriberkey IS NOT NULL AND (ua.web_sessions_30d > 0 OR ua.app_sessions_30d > 0 OR ua.emails_30d > 0)
                     THEN 1 END)                                                                           AS added_back_users_active_30d,
       COUNT(CASE
                 WHEN u.subscriberkey IS NOT NULL AND (ua.web_sessions_90d > 0 OR ua.app_sessions_90d > 0 OR ua.emails_90d > 0)
                     THEN 1 END)                                                                           AS added_back_users_active_90d
FROM se.data.user_activity ua
         LEFT JOIN collab.marketing.adhoc_user_c19_back_in_20200608_import u
                   ON ua.shiro_user_id = TRY_TO_NUMBER(u.subscriberkey)
WHERE ua.date >= CURRENT_DATE - 14
GROUP BY 1
ORDER BY 1;

SELECT date, sum(ue.sends), sum(ue.opens), sum(ue.clicks)
FROM se.data.user_emails ue
         INNER JOIN collab.marketing.adhoc_user_c19_back_in_20200608_import u ON ue.user_id = TRY_TO_NUMBER(u.subscriberkey)
WHERE ue.date >= CURRENT_DATE - 14
GROUP BY 1
ORDER BY 1;

------------------------------------------------------------------------------------------------------------------------
--check what emails were sent to users before added back in
SELECT es.client_id,
       es.send_id,
       es.subscriber_key,
       es.email_address,
       es.subscriber_id,
       es.list_id,
       es.event_date,
       es.event_type,
       es.batch_id,
       es.triggered_send_external_key,
       es.extract_metadata,
       jl.email_name
FROM raw_vault_mvp.sfmc.events_sends es
         LEFT JOIN raw_vault_mvp.sfmc.jobs_list jl ON es.send_id = jl.send_id
         INNER JOIN collab.marketing.adhoc_user_c19_back_in_20200608_import u ON es.subscriber_key = u.subscriberkey
WHERE event_date >= '2020-06-01'
  AND es.event_date < '2020-06-08';


CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.dwh.hotel_room_inventory
(

    -- (lineage) metadata for the current job
    schedule_tstamp    TIMESTAMP,
    run_tstamp         TIMESTAMP,
    operation_id       VARCHAR,
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP,

    view_date          DATE,
    mari_hotel_id      INT,
    cms_hotel_id       INT,
    hotel_name         VARCHAR,
    hotel_code         VARCHAR,
    room_type_id       INT,
    room_type_name     VARCHAR,
    room_type_code     VARCHAR,
    inventory_date     DATE,
    inventory_day      VARCHAR,
    no_total_rooms     INT,
    no_available_rooms INT,
    no_booked_rooms    INT,
    no_closedout_rooms INT,

    CONSTRAINT pk_1 PRIMARY KEY (view_date, mari_hotel_id, room_type_id, inventory_date)
)
    CLUSTER BY (view_date)
;

DROP TABLE data_vault_mvp_dev_robin.dwh.hotel_room_inventory;


SELECT jl.email_name,
       count(*)
FROM raw_vault_mvp.sfmc.events_sends es
         LEFT JOIN raw_vault_mvp.sfmc.jobs_list jl ON es.send_id = jl.send_id
         INNER JOIN collab.marketing.adhoc_user_c19_back_in_20200608_import u ON es.subscriber_key = u.subscriberkey
WHERE event_date >= '2020-06-01'
  AND es.event_date < '2020-06-08'
GROUP BY 1
