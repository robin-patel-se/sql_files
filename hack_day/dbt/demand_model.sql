------------------------------------------------------------------------------------------------------------------------
--sends
SELECT *
FROM dbt_dev_robin.bi.dv_bi_02_sends;

------------------------------------------------------------------------------------------------------------------------
--opens
SELECT *
FROM dbt_dev_robin.bi.dv_bi_03_opens;

------------------------------------------------------------------------------------------------------------------------
--clicks
SELECT *
FROM dbt_dev_robin.bi.dv_bi_04_clicks;

------------------------------------------------------------------------------------------------------------------------
--mau
SELECT *
FROM dbt_dev_robin.bi.dv_bi_05_mau;


------------------------------------------------------------------------------------------------------------------------
--wau
SELECT *
FROM dbt_dev_robin.bi.dv_bi_06_wau;


------------------------------------------------------------------------------------------------------------------------
--sign ups
SELECT *
FROM dbt_dev_robin.bi.dv_bi_07_sign_ups;

------------------------------------------------------------------------------------------------------------------------
--session grain
SELECT *
FROM dbt_dev_robin.bi.dv_bi_08_01_session_grain;

------------------------------------------------------------------------------------------------------------------------
--event grain
SELECT *
FROM dbt_dev_robin.bi.dv_bi_08_02_event_grain;

------------------------------------------------------------------------------------------------------------------------
--grain
--stack all event types ontop of one another

WITH stack AS (
    SELECT COALESCE(s.id, o.id, c.id, m.id, w.id, su.id)                                                                                                                   AS id,
           COALESCE(s.member_recency_status, o.member_recency_status, c.member_recency_status, m.member_recency_status, w.member_recency_status, su.member_recency_status) AS member_recency_status,
           COALESCE(s.current_affiliate_territory, o.current_affiliate_territory, c.current_affiliate_territory, m.current_affiliate_territory, w.current_affiliate_territory,
                    su.current_affiliate_territory)                                                                                                                        AS current_affiliate_territory,
           COALESCE(s.original_affiliate_territory, o.original_affiliate_territory, c.original_affiliate_territory, m.original_affiliate_territory, w.original_affiliate_territory,
                    su.original_affiliate_territory)                                                                                                                       AS original_affiliate_territory,
           COALESCE(s.send_date, o.open_date, c.click_date, m.date, w.date, su.signup_date)                                                                                AS date,
           s.sends,
           o.opens,
           o.unique_opens,
           c.clicks,
           c.unique_clicks,
           m.mau,
           m.app_mau,
           m.web_mau,
           m.email_mau,
           w.wau,
           w.app_wau,
           w.web_wau,
           w.email_wau,
           su.signups
    FROM dbt_dev_robin.bi.dv_bi_02_sends s
        FULL OUTER JOIN dbt_dev_robin.bi.dv_bi_03_opens o ON s.id = o.id
        FULL OUTER JOIN dbt_dev_robin.bi.dv_bi_04_clicks c ON COALESCE(s.id, o.id) = c.id
        FULL OUTER JOIN dbt_dev_robin.bi.dv_bi_05_mau m ON COALESCE(s.id, o.id, c.id) = m.id
        FULL OUTER JOIN dbt_dev_robin.bi.dv_bi_06_wau w ON COALESCE(s.id, o.id, c.id, m.id) = w.id
        FULL OUTER JOIN dbt_dev_robin.bi.dv_bi_07_sign_ups su ON COALESCE(s.id, o.id, c.id, m.id, w.id) = su.id
)
SELECT s.id,
       s.member_recency_status,
       s.current_affiliate_territory,
       s.original_affiliate_territory,
       s.date,
       sc.day_name,
       sc.year,
       sc.se_year,
       sc.se_week,
       sc.month,
       sc.month_name,
       sc.day_of_month,
       sc.day_of_week,
       sc.week_start,
       sc.yesterday,
       sc.yesterday_last_week,
       sc.this_week,
       sc.this_week_wtd,
       sc.last_week,
       sc.last_week_wtd,
       sc.this_month,
       sc.this_month_mtd,
       sc.last_month,
       sc.last_month_mtd,
       s.sends,
       s.opens,
       s.unique_opens,
       s.clicks,
       s.unique_clicks,
       s.mau,
       s.app_mau,
       s.web_mau,
       s.email_mau,
       s.wau,
       s.app_wau,
       s.web_wau,
       s.email_wau,
       s.signups
FROM stack s
    LEFT JOIN data_vault_mvp.dwh.se_calendar sc ON s.date = sc.date_value
;


DROP SCHEMA dbt_dev_robin.bi;

SELECT *
FROM hygiene_snapshot_vault_mvp.tableau_gsheets.tableau_channel_costs;

SELECT *
FROM dbt_dev_robin.bi.dv_bi_02_grain db02g;

SELECT *
FROM dbt_dev_robin.bi.dv_bi_04_event_grain db04eg
WHERE member_recency_status = NULL;

SELECT sg.id,
       sg.member_recency_status,
       sg.current_affiliate_territory,
       sg.original_affiliate_territory,
       sg.date,
       sg.channel,
       sg.touch_experience,
       sg.platform,
       sg.posa_category,
       sg.sessions,
       sg.users,
       eg.id,
       eg.member_recency_status,
       eg.current_affiliate_territory,
       eg.original_affiliate_territory,
       eg.product_configuration,
       eg.travel_type,
       eg.bookings,
       eg.margin_gbp_constant_currency,
       eg.no_nights,
       eg.rooms,
       eg.spvs,
       eg.sessions,
       eg.users
FROM dbt_dev_robin.bi.dv_bi_03_session_grain sg
    LEFT JOIN dbt_dev_robin.bi.dv_bi_04_event_grain eg
              ON sg.date = eg.date AND
                 sg.member_recency_status = eg.member_recency_status AND
                 sg.current_affiliate_territory = eg.current_affiliate_territory AND
                 sg.original_affiliate_territory = eg.original_affiliate_territory AND
                 sg.date = eg.date AND
                 sg.channel = eg.channel AND
                 sg.touch_experience = eg.touch_experience AND
                 sg.platform = eg.platform AND
                 sg.posa_category = eg.posa_category;


SELECT DISTINCT member_recency_status
FROM dbt_dev_robin.bi.dv_bi_02_grain db02g;
SELECT DISTINCT member_recency_status
FROM dbt_dev_robin.bi.dv_bi_03_session_grain db03sg;

SELECT COUNT(*)``
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date = CURRENT_DATE - 1;
SELECT SUM(db04eg.bookings)
FROM dbt_dev_robin.bi.dv_bi_04_event_grain db04eg;


------------------------------------------------------------------------------------------------------------------------

SELECT * FROM se.bi.daily_spv_weight dsw