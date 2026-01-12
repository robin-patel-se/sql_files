CREATE OR REPLACE VIEW collab.crm.unsubbed_profile_attributes COPY GRANTS AS
(
WITH distinct_users AS (
    -- get distinct users based on their first input unsub event
    SELECT
        bd.c1 AS shiro_user_id,
        bd.c2 AS unsub_tstamp,
        u.signup_tstamp
    FROM collab.crm.ben_deavin_list_user_ids_20220726 bd
        INNER JOIN data_vault_mvp.dwh.user_attributes u ON bd.c1 = u.shiro_user_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c2) = 1 -- first unsub event
),
     profile_history AS (
         --model history profile changes using the hygiene table
         SELECT
             p.row_loaded_at::DATE AS event_date,
             ua.shiro_user_id,
             ua.signup_tstamp,
             p.receive_sales_reminders,
             p.receive_weekly_offers,
             p.receive_hand_picked_offers
         FROM hygiene_vault_mvp.cms_mysql.profile p
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON p.id = ua.profile_id
             INNER JOIN distinct_users du ON ua.shiro_user_id = du.shiro_user_id
             QUALIFY ROW_NUMBER() OVER (PARTITION BY p.id, p.row_loaded_at::DATE ORDER BY p.row_loaded_at DESC) = 1
     ),
     grain AS (
         -- create a grain of user profile on any given date
         SELECT
             s.date_value AS view_date,
             du.signup_tstamp,
             du.shiro_user_id
         FROM distinct_users du
             --history only kept from 16th dec 2019
             LEFT JOIN se.data.se_calendar s ON GREATEST('2019-12-16', du.signup_tstamp::DATE) <= s.date_value
             AND CURRENT_DATE >= s.date_value
     ),
     model_user_profile_history AS (
-- persist profile attributes over grain
         SELECT
             g.view_date,
             g.shiro_user_id,
             g.signup_tstamp,
             ph.event_date IS NOT NULL                                                                                                                                   AS profile_updated,
             LAST_VALUE(ph.receive_sales_reminders)
                        IGNORE NULLS OVER (PARTITION BY g.shiro_user_id ORDER BY g.view_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           AS receive_sales_reminders,
             LAST_VALUE(ph.receive_weekly_offers) IGNORE NULLS OVER (PARTITION BY g.shiro_user_id ORDER BY g.view_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receive_weekly_offers,
             LAST_VALUE(ph.receive_hand_picked_offers)
                        IGNORE NULLS OVER (PARTITION BY g.shiro_user_id ORDER BY g.view_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           AS receive_hand_picked_offers
         FROM grain g
             LEFT JOIN profile_history ph ON g.shiro_user_id = ph.shiro_user_id AND g.view_date = ph.event_date
     )
SELECT
    du.shiro_user_id,
    cd.email                                       AS email_address,
    du.unsub_tstamp,
    du.signup_tstamp,
    sd.receive_sales_reminders                     AS receive_sales_reminders_same_day,
    db.receive_sales_reminders                     AS receive_sales_reminders_day_before,
    da.receive_sales_reminders                     AS receive_sales_reminders_day_after,
    IFF(cd.email_receive_sales_reminders, 1, 0)    AS receive_sales_reminders_current,
    sd.receive_weekly_offers                       AS receive_weekly_offers_same_day,
    db.receive_weekly_offers                       AS receive_weekly_offers_day_before,
    da.receive_weekly_offers                       AS receive_weekly_offers_day_after,
    IFF(cd.email_receive_weekly_offers, 1, 0)      AS receive_weekly_offers_current,
    sd.receive_hand_picked_offers                  AS receive_hand_picked_offers_same_day,
    db.receive_hand_picked_offers                  AS receive_hand_picked_offers_day_before,
    da.receive_hand_picked_offers                  AS receive_hand_picked_offers_day_after,
    IFF(cd.email_receive_hand_picked_offers, 1, 0) AS receive_hand_picked_offers_current
FROM distinct_users du
    LEFT JOIN model_user_profile_history sd ON du.shiro_user_id = sd.shiro_user_id AND du.unsub_tstamp::DATE = sd.view_date
    LEFT JOIN model_user_profile_history db ON du.shiro_user_id = db.shiro_user_id AND du.unsub_tstamp::DATE = db.view_date + 1
    LEFT JOIN model_user_profile_history da ON du.shiro_user_id = da.shiro_user_id AND du.unsub_tstamp::DATE = da.view_date - 1
    LEFT JOIN data_vault_mvp.dwh.user_attributes cd ON du.shiro_user_id = cd.shiro_user_id
    )
;

SELECT *
FROM collab.crm.unsubbed_profile_attributes;

GRANT SELECT ON TABLE collab.crm.unsubbed_profile_attributes TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.crm.unsubbed_profile_attributes TO ROLE personal_role__bendeavin;
GRANT SELECT ON TABLE collab.crm.unsubbed_profile_attributes TO ROLE personal_role__kostaschaveles;


WITH distinct_users AS (
    -- get distinct users based on their first input unsub event
    SELECT
        bd.c1 AS shiro_user_id,
        bd.c2 AS unsub_tstamp,
        u.signup_tstamp
    FROM collab.crm.ben_deavin_list_user_ids_20220726 bd
        INNER JOIN data_vault_mvp.dwh.user_attributes u ON bd.c1 = u.shiro_user_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c2) = 1 -- first unsub event
),
     profile_history AS (
         --model history profile changes using the hygiene table
         SELECT
             p.row_loaded_at::DATE AS event_date,
             ua.shiro_user_id,
             ua.signup_tstamp,
             p.receive_sales_reminders,
             p.receive_weekly_offers,
             p.receive_hand_picked_offers
         FROM hygiene_vault_mvp.cms_mysql.profile p
             INNER JOIN data_vault_mvp.dwh.user_attributes ua ON p.id = ua.profile_id
             INNER JOIN distinct_users du ON ua.shiro_user_id = du.shiro_user_id
             QUALIFY ROW_NUMBER() OVER (PARTITION BY p.id, p.row_loaded_at::DATE ORDER BY p.row_loaded_at DESC) = 1
     ),
     grain AS (
         -- create a grain of user profile on any given date
         SELECT
             s.date_value AS view_date,
             du.signup_tstamp,
             du.shiro_user_id
         FROM distinct_users du
             --history only kept from 16th dec 2019
             LEFT JOIN se.data.se_calendar s ON GREATEST('2019-12-16', du.signup_tstamp::DATE) <= s.date_value
             AND CURRENT_DATE >= s.date_value
     )
-- persist profile attributes over grain
SELECT
    g.view_date,
    g.shiro_user_id,
    g.signup_tstamp,
    ph.event_date IS NOT NULL                                                                                                                                   AS profile_updated,
    LAST_VALUE(ph.receive_sales_reminders)
               IGNORE NULLS OVER (PARTITION BY g.shiro_user_id ORDER BY g.view_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           AS receive_sales_reminders,
    LAST_VALUE(ph.receive_weekly_offers) IGNORE NULLS OVER (PARTITION BY g.shiro_user_id ORDER BY g.view_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receive_weekly_offers,
    LAST_VALUE(ph.receive_hand_picked_offers)
               IGNORE NULLS OVER (PARTITION BY g.shiro_user_id ORDER BY g.view_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           AS receive_hand_picked_offers
FROM grain g
    LEFT JOIN profile_history ph ON g.shiro_user_id = ph.shiro_user_id AND g.view_date = ph.event_date
;

--no option for third party at sign up
-- could either be daily or weekly

-- daily, weekly, third party
-- 111
-- 010
-- 000

WITH distinct_users AS (
    SELECT
        bd.c1 AS shiro_user_id,
        bd.c2 AS unsub_tstamp,
        u.signup_tstamp
    FROM collab.crm.ben_deavin_list_user_ids_20220726 bd
        INNER JOIN data_vault_mvp.dwh.user_attributes u ON bd.c1 = u.shiro_user_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c2) = 1 -- first unsub event
)
SELECT *
FROM se.data.crm_events_unsubscribes ceu
    INNER JOIN distinct_users b ON ceu.shiro_user_id = b.shiro_user_id
    AND ceu.event_tstamp >= b.unsub_tstamp
    AND ceu.unsub_source IS DISTINCT FROM 'Workflow';
-- 22 users unsubbed


SELECT *
FROM se.data.crm_events_unsubscribes ceu
WHERE ceu.unsub_source = 'Workflow';


SELECT
    COUNT(*)
FROM se.data.crm_events_unsubscribes ceu
WHERE ceu.unsub_source = 'Workflow';


SELECT
    ceu.event_tstamp::DATE AS date,
    COUNT(DISTINCT ceu.shiro_user_id)
FROM se.data.crm_events_unsubscribes ceu
WHERE ceu.unsub_source = 'Workflow'
GROUP BY 1;


SELECT *
FROM se.data_pii.crm_jobs_list cjl
WHERE cjl.list_ids::VARCHAR LIKE '%1710941%';



WITH distinct_users AS (
    -- get distinct users based on their first input unsub event
    SELECT
        bd.c1 AS shiro_user_id,
        bd.c2 AS unsub_tstamp,
        u.signup_tstamp
    FROM collab.crm.ben_deavin_list_user_ids_20220726 bd
        INNER JOIN data_vault_mvp.dwh.user_attributes u ON bd.c1 = u.shiro_user_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c2) = 1 -- first unsub event
)
     --model history profile changes using the hygiene table
SELECT
    p.row_loaded_at AS event_date,
    ua.shiro_user_id,
    ua.signup_tstamp,
    p.receive_sales_reminders,
    p.receive_weekly_offers,
    p.receive_hand_picked_offers
FROM hygiene_vault_mvp.cms_mysql.profile p
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON p.id = ua.profile_id
    INNER JOIN distinct_users du ON ua.shiro_user_id = du.shiro_user_id
WHERE ua.shiro_user_id = '77912176'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.id, p.row_loaded_at::DATE ORDER BY p.row_loaded_at DESC) = 1



SELECT
    bd.c1 AS shiro_user_id,
    bd.c2 AS unsub_tstamp,
    u.signup_tstamp
FROM collab.crm.ben_deavin_list_user_ids_20220726 bd
    INNER JOIN data_vault_mvp.dwh.user_attributes u ON bd.c1 = u.shiro_user_id
WHERE c1 = '77912176';

SELECT *
FROM se.data.crm_events_unsubscribes ceu
WHERE ceu.shiro_user_id = '77912176'

SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.shiro_user_id = 77912176;

