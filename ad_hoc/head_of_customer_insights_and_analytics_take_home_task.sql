CREATE SCHEMA collab.head_of_customer_insights_and_analytics_task;

GRANT USAGE ON SCHEMA collab.head_of_customer_insights_and_analytics_task TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA collab.head_of_customer_insights_and_analytics_task TO ROLE personal_role__robinpatel;

CREATE OR REPLACE TRANSIENT TABLE collab.head_of_customer_insights_and_analytics_task.user_attributes COPY GRANTS AS (
    SELECT SHA2(ua.shiro_user_id)                                                AS shiro_user_id,
           ua.current_affiliate_territory,
           ua.email_opt_in_status = 'daily'                                      AS daily_opt_in,
           ua.email_opt_in_status = 'weekly'                                     AS weekly_opt_in,
           IFF(ua.email_opt_in_status IS DISTINCT FROM 'opted out', TRUE, FALSE) AS subscribed
    FROM data_vault_mvp.dwh.user_attributes ua sample (30000 rows)
    WHERE ua.signup_tstamp >= '2020-01-01'
);

CREATE OR REPLACE TRANSIENT TABLE collab.head_of_customer_insights_and_analytics_task.user_recent_activities COPY GRANTS AS (
    SELECT SHA2(ura.shiro_user_id) AS shiro_user_id,
           ura.last_email_open_tstamp,
           ura.last_email_click_tstamp,
           ura.last_session_end_tstamp
    FROM data_vault_mvp.dwh.user_recent_activities ura
        INNER JOIN collab.head_of_customer_insights_and_analytics_task.user_attributes ul ON SHA2(ura.shiro_user_id) = ul.shiro_user_id
);

CREATE OR REPLACE TRANSIENT TABLE collab.head_of_customer_insights_and_analytics_task.fact_booking COPY GRANTS AS (
    SELECT fb.booking_id,
           SHA2(fb.shiro_user_id) AS shiro_user_id,
           fb.margin_gross_of_toms_gbp_constant_currency,
           fb.booking_completed_date,
           fb.booking_status_type,
           fb.cancellation_date
    FROM data_vault_mvp.dwh.fact_booking fb
        INNER JOIN collab.head_of_customer_insights_and_analytics_task.user_attributes ul ON SHA2(fb.shiro_user_id) = ul.shiro_user_id
    WHERE fb.booking_status_type IS DISTINCT FROM 'abandoned'
);



GRANT SELECT ON TABLE collab.head_of_customer_insights_and_analytics_task.user_attributes TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.head_of_customer_insights_and_analytics_task.user_recent_activities TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.head_of_customer_insights_and_analytics_task.fact_booking TO ROLE personal_role__gianniraftis;



SELECT *
FROM collab.head_of_customer_insights_and_analytics_task.user_attributes;
SELECT *
FROM collab.head_of_customer_insights_and_analytics_task.user_recent_activities;
SELECT *
FROM collab.head_of_customer_insights_and_analytics_task.fact_booking;