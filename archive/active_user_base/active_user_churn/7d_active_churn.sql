USE WAREHOUSE pipe_large;


WITH current_7d_active AS (
    SELECT shiro_user_id,
           web_sessions_7d,
           app_sessions_7d,
           emails_7d
    FROM se.data.user_activity
    WHERE date = DATEADD(DAY, -2, CURRENT_DATE) --todo change to -1 once the data is up to date
      AND (web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0)
),
     prev_shiro_user_idious_7d_active AS (
         SELECT shiro_user_id,
                web_sessions_7d,
                app_sessions_7d,
                emails_7d
         FROM se.data.user_activity
         WHERE date = DATEADD(DAY, -9, CURRENT_DATE) --todo change to -8 once the data is up to date
           AND (web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0)
     ),
     activity_status AS (
         SELECT COALESCE(c.shiro_user_id, p.shiro_user_id) AS user_id,
                CASE
                    WHEN c.shiro_user_id IS NULL THEN 'Lost Active User'
                    WHEN p.shiro_user_id IS NULL THEN 'New Active User'
                    ELSE 'Repeat Active User'
                    END                                    AS activity_status
         FROM current_7d_active c
                  FULL JOIN prev_shiro_user_idious_7d_active p ON c.shiro_user_id = p.shiro_user_id
     )
SELECT activity_status,
       count(*)
FROM activity_status
GROUP BY 1
;

--no of 7d active users 2 days ago
SELECT COUNT(*)
FROM se.data.user_activity
WHERE date = DATEADD(DAY, -2, CURRENT_DATE) --todo change to -1 once the data is up to date
  AND (web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0) --7d active user
;
--3732024

--no of 7d active users 9 days ago
SELECT COUNT(*)
FROM se.data.user_activity
WHERE date = DATEADD(DAY, -9, CURRENT_DATE) --todo change to -1 once the data is up to date
  AND (web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0) --7d active user
; --3495535


WITH current_30d_active AS (
    SELECT shiro_user_id,
           web_sessions_30d,
           app_sessions_30d,
           emails_30d
    FROM se.data.user_activity
    WHERE date = DATEADD(DAY, -33, CURRENT_DATE) --todo change to -1 once the data is up to date
      AND (web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0)
),
     prev_shiro_user_idious_30d_active AS (
         SELECT shiro_user_id,
                web_sessions_30d,
                app_sessions_30d,
                emails_30d
         FROM se.data.user_activity
         WHERE date = DATEADD(DAY, -63, CURRENT_DATE) --todo change to -32 once the data is up to date
           AND (web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0)
     ),
     activity_status AS (
         SELECT COALESCE(c.shiro_user_id, p.shiro_user_id) AS user_id,
                CASE
                    WHEN c.shiro_user_id IS NULL THEN 'Lost Active User'
                    WHEN p.shiro_user_id IS NULL THEN 'New Active User'
                    ELSE 'Repeat Active User'
                    END                                    AS activity_status
         FROM current_30d_active c
                  FULL JOIN prev_shiro_user_idious_30d_active p ON c.shiro_user_id = p.shiro_user_id
     )
SELECT activity_status,
       count(*)
FROM activity_status
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
SET lookbackweeks = -12; --number of look back weeks
WITH mondays AS (
    SELECT date_value AS date
    FROM se.data.se_calendar
    WHERE dayofweek(date_value) = 1
      AND date_value >= dateadd(WEEK, $lookbackweeks, date_trunc(WEEK, current_date))
      AND date_value <= CURRENT_DATE
)
   , user_activity AS (
    --active users on Monday of each week
    SELECT date,
           shiro_user_id,
           web_sessions_7d,
           app_sessions_7d,
           emails_7d
    FROM se.data.user_activity
    WHERE DAYOFWEEK(date) = 1
      AND date >= dateadd(WEEK, $lookbackweeks, date_trunc(WEEK, CURRENT_DATE))
      AND (web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0)
    ORDER BY 1
)
   , users AS (
    SELECT shiro_user_id,
           MIN(date) AS min_date
    FROM user_activity
    GROUP BY 1
)
   , grain AS (
    SELECT *
    FROM users u
             LEFT JOIN mondays c ON c.date >= u.min_date
)
   , activity_status AS (
    SELECT g.date,
           g.shiro_user_id,
           u.web_sessions_7d,
           u.app_sessions_7d,
           u.emails_7d,
           u.shiro_user_id                                                    AS curr_suid,
           LAG(curr_suid) OVER (PARTITION BY g.shiro_user_id ORDER BY g.date) AS prev_suid,
           CASE
               WHEN prev_suid IS NULL AND curr_suid IS NULL THEN 'Inactive User'
               WHEN prev_suid IS NULL AND curr_suid IS NOT NULL THEN 'New Active User'
               WHEN prev_suid IS NOT NULL AND curr_suid IS NULL THEN 'Lost Active User'
               WHEN prev_suid IS NOT NULL AND curr_suid IS NOT NULL THEN 'Repeat Active User'
               END                                                            AS activity_status
    FROM grain g
             LEFT JOIN user_activity u ON g.date = u.date AND g.shiro_user_id = u.shiro_user_id
)
SELECT date,
       SUM(CASE WHEN activity_status = 'New Active User' THEN 1 END)    AS new_active_users,
       SUM(CASE WHEN activity_status = 'Repeat Active User' THEN 1 END) AS repeat_active_users,
       SUM(CASE WHEN activity_status = 'Lost Active User' THEN 1 END)   AS lost_active_users,
       new_active_users + repeat_active_users                           AS active_users,
       lost_active_users / active_users                                 AS churn_rate
FROM activity_status
WHERE activity_status != 'Inactive User' -- remove entries where a user has been inactive consistently to avoid double count
GROUP BY 1
HAVING repeat_active_users IS NOT NULL
   AND lost_active_users IS NOT NULL -- remove first week
ORDER BY date
;

-- SELECT c.date,
--        u.shiro_user_id
-- FROM mondays c
--          CROSS JOIN user_active u
-- GROUP BY 1, 2




