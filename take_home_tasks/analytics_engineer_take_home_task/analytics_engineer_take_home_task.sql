CREATE SCHEMA collab.analytics_engineer_task;

GRANT USAGE ON SCHEMA collab.analytics_engineer_task TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA collab.analytics_engineer_task TO ROLE personal_role__kirstengrieve;
GRANT USAGE ON SCHEMA collab.analytics_engineer_task TO ROLE personal_role__robinpatel;

GRANT SELECT ON TABLE collab.analytics_engineer_task.user_attributes TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.analytics_engineer_task.fact_user_transactions TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.analytics_engineer_task.fact_user_activity TO ROLE personal_role__gianniraftis;

GRANT SELECT ON TABLE collab.analytics_engineer_task.user_attributes TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON TABLE collab.analytics_engineer_task.fact_user_transactions TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON TABLE collab.analytics_engineer_task.fact_user_activity TO ROLE personal_role__kirstengrieve;


USE WAREHOUSE pipe_xlarge;

-- CREATE OR REPLACE TRANSIENT TABLE collab.analytics_engineer_task.user_attributes COPY GRANTS AS (
--     SELECT
--         SHA2(sua.shiro_user_id)                      AS user_id,
--         sua.signup_tstamp,
--         sua.current_affiliate_territory              AS territory,
--         sua.membership_account_status,
--         sua.member_original_affiliate_classification AS acquisition_type,
--         sua.email_opt_in_status
--     FROM se.data.se_user_attributes sua sample (10000 rows)
--     WHERE sua.signup_tstamp >= '2021-01-01'
-- );
--
-- CREATE OR REPLACE TRANSIENT TABLE collab.analytics_engineer_task.fact_user_transactions COPY GRANTS AS (
--     SELECT
--         SHA2(fcb.shiro_user_id)                         AS user_id,
--         DATE_TRUNC('month', fcb.booking_completed_date) AS event_month,
--         COUNT(DISTINCT fcb.booking_id)                  AS bookings,
--         SUM(fcb.margin_gross_of_toms_gbp)               AS margin_gbp
--     FROM se.data.fact_complete_booking fcb
--         INNER JOIN collab.analytics_engineer_task.user_attributes ua ON SHA2(fcb.shiro_user_id) = ua.user_id
--     GROUP BY 1, 2
--     HAVING bookings > 0
-- );
--
-- CREATE OR REPLACE TRANSIENT TABLE collab.analytics_engineer_task.fact_user_activity COPY GRANTS AS (
--     SELECT
--         stba.attributed_user_id_hash               AS user_id,
--         DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS event_month,
--         COUNT(DISTINCT stba.touch_id)              AS sessions,
--         COUNT(DISTINCT sts.event_hash)             AS product_page_views
--     FROM se.data.scv_touch_basic_attributes stba
--         LEFT JOIN  se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
--         INNER JOIN collab.analytics_engineer_task.user_attributes ua ON stba.attributed_user_id_hash = ua.user_id
--     GROUP BY 1, 2
--     HAVING sessions > 0
--         OR product_page_views > 0
-- );

SELECT *
FROM collab.analytics_engineer_task.user_attributes;
SELECT *
FROM collab.analytics_engineer_task.fact_user_transactions;
SELECT *
FROM collab.analytics_engineer_task.fact_user_activity;


-- file checks

SELECT count(*)
FROM collab.analytics_engineer_task.user_attributes; -- 10000

SELECT count(*)
FROM collab.analytics_engineer_task.fact_user_transactions; -- 435

SELECT count(*)
FROM collab.analytics_engineer_task.fact_user_activity; -- 16868

SELECT count(*)
FROM collab.analytics_engineer_task.fact_user_transactions fut
    INNER JOIN collab.analytics_engineer_task.user_attributes ua ON fut.user_id = ua.user_id; -- 435


SELECT count(*)
FROM collab.analytics_engineer_task.fact_user_activity ut
    INNER JOIN collab.analytics_engineer_task.user_attributes ua ON ut.user_id = ua.user_id; -- 16868




-- 21 june 2022
/*
    Checked the existing tables in collab, all of the tables have user id's that reference.
    Downloaded the files again and reuploaded to drive
*/


SELECT * FROM se.data.scv_touch_basic_attributes stba;