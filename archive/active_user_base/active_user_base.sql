USE WAREHOUSE pipe_large;

WITH session_activity AS (
    SELECT attributed_user_id,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_1d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_7d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS sessions_14d
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
    WHERE touch_start_tstamp >= DATEADD(DAY, -30, CURRENT_DATE)
      AND stitched_identity_type = 'se_user_id'
    GROUP BY 1
)

SELECT CURRENT_DATE                                                           AS date,
       COUNT(DISTINCT CASE WHEN sessions_1d > 0 THEN attributed_user_id END)  AS users_1d_web_active,
       COUNT(DISTINCT CASE WHEN sessions_7d > 0 THEN attributed_user_id END)  AS users_7d_web_active,
       COUNT(DISTINCT CASE WHEN sessions_14d > 0 THEN attributed_user_id END) AS users_14d_web_active,
       COUNT(DISTINCT attributed_user_id)                                     AS users_30d_web_active
FROM session_activity
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------


WITH email_activity AS (
    SELECT user_id,
           SUM(CASE WHEN date >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_1d,
           SUM(CASE WHEN date >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_7d,
           SUM(CASE WHEN date >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS emails_14d
    FROM se.data.user_emails
    WHERE date >= DATEADD(DAY, -30, CURRENT_DATE)
      AND opens > 0 --any user with an open
    GROUP BY 1
)

SELECT CURRENT_DATE                                              AS date,
       COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END)  AS users_1d_email_active,
       COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END)  AS users_7d_email_active,
       COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS users_14d_email_active,
       COUNT(DISTINCT user_id)                                   AS users_30d_email_active
FROM email_activity
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------

WITH session_activity AS (
    SELECT attributed_user_id,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_1d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_7d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS sessions_14d,
           COUNT(*)                                                                               AS sessions_30d
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
    WHERE touch_start_tstamp >= DATEADD(DAY, -30, CURRENT_DATE)
      AND stitched_identity_type = 'se_user_id'
    GROUP BY 1
),
     email_activity AS (
         SELECT user_id,
                SUM(CASE WHEN date >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_1d,
                SUM(CASE WHEN date >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_7d,
                SUM(CASE WHEN date >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS emails_14d,
                COUNT(*)                                                                 AS emails_30d
         FROM se.data.user_emails
         WHERE date >= DATEADD(DAY, -30, CURRENT_DATE)
           AND opens > 0 --any user with an open
         GROUP BY 1
     ),
     user_activity AS (
         SELECT id                          AS user_id,
                COALESCE(s.sessions_1d, 0)  AS sessions_1d,
                COALESCE(s.sessions_7d, 0)  AS sessions_7d,
                COALESCE(s.sessions_14d, 0) AS sessions_14d,
                COALESCE(s.sessions_30d, 0) AS sessions_30d,
                COALESCE(e.emails_1d, 0)    AS emails_1d,
                COALESCE(e.emails_7d, 0)    AS emails_7d,
                COALESCE(e.emails_14d, 0)   AS emails_14d,
                COALESCE(e.emails_30d, 0)   AS emails_30d

         FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
             LEFT JOIN session_activity s ON u.id = s.attributed_user_id
             LEFT JOIN email_activity e ON u.id = e.user_id
     )

SELECT CURRENT_DATE                                                                  AS date,

       COUNT(DISTINCT CASE WHEN sessions_1d > 0 THEN user_id END)                    AS users_1d_web_active,
       COUNT(DISTINCT CASE WHEN sessions_7d > 0 THEN user_id END)                    AS users_7d_web_active,
       COUNT(DISTINCT CASE WHEN sessions_14d > 0 THEN user_id END)                   AS users_14d_web_active,
       COUNT(DISTINCT CASE WHEN sessions_30d > 0 THEN user_id END)                   AS users_30d_web_active,

       COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END)                      AS users_1d_email_active,
       COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END)                      AS users_7d_email_active,
       COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END)                     AS users_14d_email_active,
       COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END)                     AS users_30d_email_active,

       COUNT(DISTINCT CASE WHEN sessions_1d > 0 OR emails_1d > 0 THEN user_id END)   AS users_1d_active,
       COUNT(DISTINCT CASE WHEN sessions_7d > 0 OR emails_7d > 0 THEN user_id END)   AS users_7d_active,
       COUNT(DISTINCT CASE WHEN sessions_14d > 0 OR emails_14d > 0 THEN user_id END) AS users_14d_active,
       COUNT(DISTINCT CASE WHEN sessions_30d > 0 OR emails_30d > 0 THEN user_id END) AS users_30d_active
FROM user_activity;

------------------------------------------------------------------------------------------------------------------------

WITH session_activity AS (
    SELECT attributed_user_id,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_1d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_7d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS sessions_14d,
           COUNT(*)                                                                               AS sessions_30d
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
    WHERE touch_start_tstamp >= DATEADD(DAY, -30, CURRENT_DATE)
      AND stitched_identity_type = 'se_user_id'
    GROUP BY 1
),
     email_activity AS (
         SELECT user_id,
                SUM(CASE WHEN date >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_1d,
                SUM(CASE WHEN date >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_7d,
                SUM(CASE WHEN date >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS emails_14d,
                COUNT(*)                                                                 AS emails_30d
         FROM se.data.user_emails
         WHERE date >= DATEADD(DAY, -30, CURRENT_DATE)
           AND opens > 0 --any user with an open
         GROUP BY 1
     ),
     user_activity AS (
         SELECT id                          AS user_id,
                COALESCE(s.sessions_1d, 0)  AS sessions_1d,
                COALESCE(s.sessions_7d, 0)  AS sessions_7d,
                COALESCE(s.sessions_14d, 0) AS sessions_14d,
                COALESCE(s.sessions_30d, 0) AS sessions_30d,
                COALESCE(e.emails_1d, 0)    AS emails_1d,
                COALESCE(e.emails_7d, 0)    AS emails_7d,
                COALESCE(e.emails_14d, 0)   AS emails_14d,
                COALESCE(e.emails_30d, 0)   AS emails_30d

         FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
             LEFT JOIN session_activity s ON u.id = s.attributed_user_id
             LEFT JOIN email_activity e ON u.id = e.user_id
     )

SELECT CURRENT_DATE                                                AS date,
       'web_active'                                                AS platform,
       COUNT(DISTINCT CASE WHEN sessions_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN sessions_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN sessions_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN sessions_30d > 0 THEN user_id END) AS active_30d

FROM user_activity

UNION ALL

SELECT CURRENT_DATE                                              AS date,
       'email_active'                                            AS platform,
       COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END) AS active_30d

FROM user_activity

UNION ALL

SELECT CURRENT_DATE                                                                  AS date,
       'user_active'                                                                 AS platform,
       COUNT(DISTINCT CASE WHEN sessions_1d > 0 OR emails_1d > 0 THEN user_id END)   AS active_1d,
       COUNT(DISTINCT CASE WHEN sessions_7d > 0 OR emails_7d > 0 THEN user_id END)   AS active_7d,
       COUNT(DISTINCT CASE WHEN sessions_14d > 0 OR emails_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN sessions_30d > 0 OR emails_30d > 0 THEN user_id END) AS active_30d

FROM user_activity
;
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base
(
    schedule_tstamp TIMESTAMP,
    run_tstamp      TIMESTAMP,
    operation_id    VARCHAR,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,

    date            DATE,
    platform        VARCHAR,
    territory       VARCHAR,
    booker_segment  VARCHAR,
    active_1d       INT,
    active_7d       INT,
    active_14d      INT,
    active_30d      INT
);

------------------------------------------------------------------------------------------------------------------------

INSERT INTO collab.muse_data_modelling.active_user_base
WITH session_activity AS (
    SELECT attributed_user_id,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_1d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS sessions_7d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS sessions_14d,
           COUNT(*)                                                                               AS sessions_30d
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
    WHERE touch_start_tstamp >= DATEADD(DAY, -30, CURRENT_DATE)::DATE
      AND stitched_identity_type = 'se_user_id'
    GROUP BY 1
),
     email_activity AS (
         SELECT user_id,
                SUM(CASE WHEN date >= DATEADD(DAY, -1, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_1d,
                SUM(CASE WHEN date >= DATEADD(DAY, -7, CURRENT_DATE) THEN 1 ELSE 0 END)  AS emails_7d,
                SUM(CASE WHEN date >= DATEADD(DAY, -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS emails_14d,
                COUNT(*)                                                                 AS emails_30d
         FROM se.data.user_emails
         WHERE date >= DATEADD(DAY, -30, CURRENT_DATE)::DATE
           AND opens > 0 --any user with an open
         GROUP BY 1
     ),
     user_activity AS (
         SELECT id                          AS user_id,
                COALESCE(s.sessions_1d, 0)  AS sessions_1d,
                COALESCE(s.sessions_7d, 0)  AS sessions_7d,
                COALESCE(s.sessions_14d, 0) AS sessions_14d,
                COALESCE(s.sessions_30d, 0) AS sessions_30d,
                COALESCE(e.emails_1d, 0)    AS emails_1d,
                COALESCE(e.emails_7d, 0)    AS emails_7d,
                COALESCE(e.emails_14d, 0)   AS emails_14d,
                COALESCE(e.emails_30d, 0)   AS emails_30d

         FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
             LEFT JOIN session_activity s ON u.id = s.attributed_user_id
             LEFT JOIN email_activity e ON u.id = e.user_id
     )

SELECT CURRENT_DATE                                                AS date,
       'web_active'                                                AS platform,
       COUNT(DISTINCT CASE WHEN sessions_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN sessions_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN sessions_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN sessions_30d > 0 THEN user_id END) AS active_30d

FROM user_activity

UNION ALL

SELECT CURRENT_DATE                                              AS date,
       'email_active'                                            AS platform,
       COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END) AS active_30d

FROM user_activity

UNION ALL

SELECT CURRENT_DATE                                                                  AS date,
       'user_active'                                                                 AS platform,
       COUNT(DISTINCT CASE WHEN sessions_1d > 0 OR emails_1d > 0 THEN user_id END)   AS active_1d,
       COUNT(DISTINCT CASE WHEN sessions_7d > 0 OR emails_7d > 0 THEN user_id END)   AS active_7d,
       COUNT(DISTINCT CASE WHEN sessions_14d > 0 OR emails_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN sessions_30d > 0 OR emails_30d > 0 THEN user_id END) AS active_30d

FROM user_activity;

SELECT DATEADD(DAY, -1, CURRENT_DATE);
------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
SET date_var = DATEADD(DAY, -1, CURRENT_DATE);
INSERT INTO se_dev_robin.data.active_user_base
WITH web_session_activity AS (
    SELECT attributed_user_id,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS sessions_1d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS sessions_7d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS sessions_14d,
           COUNT(*)                                                                            AS sessions_30d
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
    WHERE touch_start_tstamp >= DATEADD(DAY, -30, $date_var)
      AND touch_start_tstamp <= $date_var
      AND stitched_identity_type = 'se_user_id'
      AND touch_experience != 'native app'
    GROUP BY 1
),
     app_session_activity AS (
         SELECT attributed_user_id,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS sessions_1d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS sessions_7d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS sessions_14d,
                COUNT(*)                                                                            AS sessions_30d
         FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
         WHERE touch_start_tstamp >= DATEADD(DAY, -30, $date_var)
           AND touch_start_tstamp <= $date_var
           AND stitched_identity_type = 'se_user_id'
           AND touch_experience = 'native app'
         GROUP BY 1
     ),
     email_activity AS (
         SELECT user_id,
                SUM(CASE WHEN date >= DATEADD(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS emails_1d,
                SUM(CASE WHEN date >= DATEADD(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS emails_7d,
                SUM(CASE WHEN date >= DATEADD(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS emails_14d,
                COUNT(*)                                                              AS emails_30d
         FROM se.data.user_emails
         WHERE date >= DATEADD(DAY, -30, $date_var)
           AND date <= $date_var
           AND opens > 0 --any user with an open
         GROUP BY 1
     ),
     user_activity AS (
         SELECT id                            AS user_id,
                COALESCE(ws.sessions_1d, 0)   AS web_sessions_1d,
                COALESCE(ws.sessions_7d, 0)   AS web_sessions_7d,
                COALESCE(ws.sessions_14d, 0)  AS web_sessions_14d,
                COALESCE(ws.sessions_30d, 0)  AS web_sessions_30d,

                COALESCE(aps.sessions_1d, 0)  AS app_sessions_1d,
                COALESCE(aps.sessions_7d, 0)  AS app_sessions_7d,
                COALESCE(aps.sessions_14d, 0) AS app_sessions_14d,
                COALESCE(aps.sessions_30d, 0) AS app_sessions_30d,

                COALESCE(e.emails_1d, 0)      AS emails_1d,
                COALESCE(e.emails_7d, 0)      AS emails_7d,
                COALESCE(e.emails_14d, 0)     AS emails_14d,
                COALESCE(e.emails_30d, 0)     AS emails_30d

         FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
             LEFT JOIN web_session_activity ws ON u.id = ws.attributed_user_id
             LEFT JOIN app_session_activity aps ON u.id = aps.attributed_user_id
             LEFT JOIN email_activity e ON u.id = e.user_id
     )

SELECT '1970-01-01 00:00:00.000'                                       AS schedule_tstamp,
       '2020-04-01 00:00:00.000'                                       AS run_tstamp,
       'initial backfill'                                              AS operation_id,
       CURRENT_DATE                                                    AS created_at,
       CURRENT_DATE                                                    AS updated_at,

       $date_var                                                       AS date,
       'web_active'                                                    AS platform,
       COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 THEN user_id END) AS active_30d

FROM user_activity

UNION ALL

SELECT '1970-01-01 00:00:00.000'                                       AS schedule_tstamp,
       '2020-04-01 00:00:00.000'                                       AS run_tstamp,
       'initial backfill'                                              AS operation_id,
       CURRENT_DATE                                                    AS created_at,
       CURRENT_DATE                                                    AS updated_at,

       $date_var                                                       AS date,
       'app_active'                                                    AS platform,
       COUNT(DISTINCT CASE WHEN app_sessions_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN app_sessions_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN app_sessions_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN app_sessions_30d > 0 THEN user_id END) AS active_30d

FROM user_activity

UNION ALL

SELECT '1970-01-01 00:00:00.000'                                 AS schedule_tstamp,
       '2020-04-01 00:00:00.000'                                 AS run_tstamp,
       'initial backfill'                                        AS operation_id,
       CURRENT_DATE                                              AS created_at,
       CURRENT_DATE                                              AS updated_at,

       $date_var                                                 AS date,
       'email_active'                                            AS platform,
       COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END) AS active_30d

FROM user_activity

UNION ALL

SELECT '1970-01-01 00:00:00.000'       AS schedule_tstamp,
       '2020-04-01 00:00:00.000'       AS run_tstamp,
       'initial backfill'              AS operation_id,
       CURRENT_DATE                    AS created_at,
       CURRENT_DATE                    AS updated_at,

       $date_var                       AS date,
       'user_active'                   AS platform,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_1d > 0 OR app_sessions_1d > 0 OR emails_1d > 0
                     THEN user_id END) AS active_1d,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0
                     THEN user_id END) AS active_7d,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_14d > 0 OR app_sessions_14d > 0 OR emails_14d > 0
                     THEN user_id END) AS active_14d,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0
                     THEN user_id END) AS active_30d

FROM user_activity;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM collab.muse_data_modelling.active_user_base;

GRANT USAGE ON SCHEMA collab.muse_data_modelling TO ROLE personal_role__alexscottsimons;
GRANT USAGE ON SCHEMA collab.muse_data_modelling TO ROLE personal_role__gianniraftis;

GRANT SELECT ON TABLE collab.muse_data_modelling.active_user_base TO ROLE personal_role__alexscottsimons;
GRANT SELECT ON TABLE collab.muse_data_modelling.active_user_base TO ROLE personal_role__gianniraftis;

USE WAREHOUSE pipe_xlarge;


self_describing_task --include 'dv/active_user_base/active_user_base'  --method 'run' --start '2020-04-06 00:00:00' --end '2020-04-06 00:00:00'

CREATE SCHEMA data_vault_dev_robin.single_customer_view_stg;
CREATE SCHEMA data_vault_mvp_dev_robin.cms_snapshots;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE se_dev_robin.data.user_emails CLONE se.data.user_emails;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

------------------------------------------------------------------------------------------------------------------------
--stored procedure

CREATE OR REPLACE PROCEDURE scratch.robinpatel.backfill_active_users_loop(p_first_run DOUBLE, p_max_runs DOUBLE
                                                                         )
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT
AS
$$
var i;
for (i = P_FIRST_RUN; i < P_MAX_RUNS; i++) {
    var sql_command = `SELECT '''' || TO_CHAR(DATEADD(DAY, -${i}, current_date)) || ''''`;
    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    var res = stmt.execute();
    res.next()
    var date_var = res.getColumnValue(1);
    var sql_command =
        `INSERT INTO se_dev_robin.data.active_user_base
        WITH web_session_activity AS (
            SELECT attributed_user_id,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, ua.date) THEN 1 ELSE 0 END) AS sessions_1d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, ua.date) THEN 1 ELSE 0 END) AS sessions_7d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, ua.date) THEN 1 ELSE 0 END) AS sessions_14d,
                count(*) AS sessions_30d
            FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
            WHERE touch_start_tstamp >= DATEADD(DAY, -30, ua.date)
                AND touch_start_tstamp <= ua.date
                AND stitched_identity_type = 'se_user_id'
                AND touch_experience != 'native app'
            GROUP BY 1
        ),
        app_session_activity AS (
            SELECT attributed_user_id,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, ua.date) THEN 1 ELSE 0 END) AS sessions_1d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, ua.date) THEN 1 ELSE 0 END) AS sessions_7d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, ua.date) THEN 1 ELSE 0 END) AS sessions_14d,
                count(*) AS sessions_30d
            FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
            WHERE touch_start_tstamp >= DATEADD(DAY, -30, ua.date)
                AND touch_start_tstamp <= ua.date
                AND stitched_identity_type = 'se_user_id'
                AND touch_experience = 'native app'
            GROUP BY 1
        ),
        email_activity AS (
            SELECT user_id,
                SUM(CASE WHEN date >= DATEADD(DAY, -1, ua.date) THEN 1 ELSE 0 END) AS emails_1d,
                SUM(CASE WHEN date >= DATEADD(DAY, -7, ua.date) THEN 1 ELSE 0 END) AS emails_7d,
                SUM(CASE WHEN date >= DATEADD(DAY, -14, ua.date) THEN 1 ELSE 0 END) AS emails_14d,
                count(*) AS emails_30d
            FROM se.data.user_emails
            WHERE date >= DATEADD(DAY, -30, ua.date)
                AND date <= ua.date
                AND opens > 0 --any user with an open
            GROUP BY 1
        ),
        user_activity AS (
            SELECT u.id AS user_id,
                t.name AS territory,
                COALESCE(ws.sessions_1d, 0) AS web_sessions_1d,
                COALESCE(ws.sessions_7d, 0) AS web_sessions_7d,
                COALESCE(ws.sessions_14d, 0) AS web_sessions_14d,
                COALESCE(ws.sessions_30d, 0) AS web_sessions_30d,

                COALESCE(aps.sessions_1d, 0) AS app_sessions_1d,
                COALESCE(aps.sessions_7d, 0) AS app_sessions_7d,
                COALESCE(aps.sessions_14d, 0) AS app_sessions_14d,
                COALESCE(aps.sessions_30d, 0) AS app_sessions_30d,

                COALESCE(e.emails_1d, 0) AS emails_1d,
                COALESCE(e.emails_7d, 0) AS emails_7d,
                COALESCE(e.emails_14d, 0) AS emails_14d,
                COALESCE(e.emails_30d, 0) AS emails_30d
            FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
                INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
                LEFT JOIN web_session_activity ws ON u.id = ws.attributed_user_id
                LEFT JOIN app_session_activity aps ON u.id = aps.attributed_user_id
                LEFT JOIN email_activity e ON u.id = e.user_id
        )
        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-06 00:00:00.000'       AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'web_active' AS platform,
            territory,
            COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-06 00:00:00.000'       AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'app_active' AS platform,
            territory,
            COUNT(DISTINCT CASE WHEN app_sessions_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN app_sessions_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN app_sessions_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN app_sessions_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-06 00:00:00.000'       AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'email_active' AS platform,
            territory,
            COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-06 00:00:00.000'       AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'user_active' AS platform,
            territory,
            COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 OR app_sessions_1d > 0 OR emails_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 OR app_sessions_14d > 0 OR emails_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8;`;

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
};
$$;

------------------------------------------------------------------------------------------------------------------------


TRUNCATE se_dev_robin.data.active_user_base;
USE WAREHOUSE pipe_xlarge;
CALL scratch.robinpatel.backfill_active_users_loop(459, 462);



USE WAREHOUSE pipe_xlarge;
CALL scratch.robinpatel.backfill_active_users_loop(278, 459);

SELECT date, territory, COUNT(*)
FROM se_dev_robin.data.active_user_base
GROUP BY 1, 2
HAVING COUNT(*) > 4;

SELECT date, COUNT(*)
FROM se_dev_robin.data.active_user_base
GROUP BY 1;

SELECT *
FROM se_dev_robin.data.active_user_base
WHERE operation_id != 'initial backfill';

SELECT MIN(date)
FROM se_dev_robin.data.active_user_base;



CREATE OR REPLACE TABLE se.data.active_user_base CLONE se_dev_robin.data.active_user_base;

SELECT date,
       SUM(sends) AS sends
FROM se.data.user_emails
WHERE date >= '2019-01-01'
GROUP BY 1
ORDER BY 1;

SELECT date,
       active_1d,
       active_7d,
       active_14d,
       active_30d
FROM se.data.active_user_base
WHERE platform = 'email_active'
ORDER BY 1;


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
SET date_var = DATEADD(DAY, -1, CURRENT_DATE);
WITH web_session_activity AS (
    SELECT attributed_user_id,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS sessions_1d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS sessions_7d,
           SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS sessions_14d,
           COUNT(*)                                                                            AS sessions_30d
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
    WHERE touch_start_tstamp >= DATEADD(DAY, -30, $date_var)
      AND touch_start_tstamp <= $date_var
      AND stitched_identity_type = 'se_user_id'
      AND touch_experience != 'native app'
    GROUP BY 1
),
     app_session_activity AS (
         SELECT attributed_user_id,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS sessions_1d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS sessions_7d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS sessions_14d,
                COUNT(*)                                                                            AS sessions_30d
         FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
         WHERE touch_start_tstamp >= DATEADD(DAY, -30, $date_var)
           AND touch_start_tstamp <= $date_var
           AND stitched_identity_type = 'se_user_id'
           AND touch_experience = 'native app'
         GROUP BY 1
     ),
     email_activity AS (
         SELECT user_id,
                SUM(CASE WHEN date >= DATEADD(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS emails_1d,
                SUM(CASE WHEN date >= DATEADD(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS emails_7d,
                SUM(CASE WHEN date >= DATEADD(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS emails_14d,
                COUNT(*)                                                              AS emails_30d
         FROM se.data.user_emails
         WHERE date >= DATEADD(DAY, -30, $date_var)
           AND date <= $date_var
           AND opens > 0 --any user with an open
         GROUP BY 1
     ),
     user_activity AS (
         SELECT u.id                                   AS user_id,
                t.name                                 AS territory,
                COALESCE(b.booker_segment, 'Prospect') AS booker_segment,
                COALESCE(ws.sessions_1d, 0)            AS web_sessions_1d,
                COALESCE(ws.sessions_7d, 0)            AS web_sessions_7d,
                COALESCE(ws.sessions_14d, 0)           AS web_sessions_14d,
                COALESCE(ws.sessions_30d, 0)           AS web_sessions_30d,

                COALESCE(aps.sessions_1d, 0)           AS app_sessions_1d,
                COALESCE(aps.sessions_7d, 0)           AS app_sessions_7d,
                COALESCE(aps.sessions_14d, 0)          AS app_sessions_14d,
                COALESCE(aps.sessions_30d, 0)          AS app_sessions_30d,

                COALESCE(e.emails_1d, 0)               AS emails_1d,
                COALESCE(e.emails_7d, 0)               AS emails_7d,
                COALESCE(e.emails_14d, 0)              AS emails_14d,
                COALESCE(e.emails_30d, 0)              AS emails_30d

         FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
             LEFT JOIN  se.data.user_segmentation b ON u.id = b.shiro_user_id AND b.date = $date_var
             LEFT JOIN  web_session_activity ws ON u.id = ws.attributed_user_id
             LEFT JOIN  app_session_activity aps ON u.id = aps.attributed_user_id
             LEFT JOIN  email_activity e ON u.id = e.user_id
         WHERE u.date_created <= $date_var
     )

SELECT '1970-01-01 00:00:00.000'                                       AS schedule_tstamp,
       '2020-04-01 00:00:00.000'                                       AS run_tstamp,
       'initial backfill'                                              AS operation_id,
       CURRENT_DATE                                                    AS created_at,
       CURRENT_DATE                                                    AS updated_at,

       $date_var                                                       AS date,
       'web_active'                                                    AS platform,
       territory,
       booker_segment,
       COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 THEN user_id END) AS active_30d

FROM user_activity
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

UNION ALL

SELECT '1970-01-01 00:00:00.000'                                       AS schedule_tstamp,
       '2020-04-01 00:00:00.000'                                       AS run_tstamp,
       'initial backfill'                                              AS operation_id,
       CURRENT_DATE                                                    AS created_at,
       CURRENT_DATE                                                    AS updated_at,

       $date_var                                                       AS date,
       'app_active'                                                    AS platform,
       territory,
       booker_segment,
       COUNT(DISTINCT CASE WHEN app_sessions_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN app_sessions_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN app_sessions_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN app_sessions_30d > 0 THEN user_id END) AS active_30d

FROM user_activity
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

UNION ALL

SELECT '1970-01-01 00:00:00.000'                                 AS schedule_tstamp,
       '2020-04-01 00:00:00.000'                                 AS run_tstamp,
       'initial backfill'                                        AS operation_id,
       CURRENT_DATE                                              AS created_at,
       CURRENT_DATE                                              AS updated_at,

       $date_var                                                 AS date,
       'email_active'                                            AS platform,
       territory,
       booker_segment,
       COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END) AS active_30d

FROM user_activity
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

UNION ALL

SELECT '1970-01-01 00:00:00.000'       AS schedule_tstamp,
       '2020-04-01 00:00:00.000'       AS run_tstamp,
       'initial backfill'              AS operation_id,
       CURRENT_DATE                    AS created_at,
       CURRENT_DATE                    AS updated_at,

       $date_var                       AS date,
       'user_active'                   AS platform,
       territory,
       booker_segment,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_1d > 0 OR app_sessions_1d > 0 OR emails_1d > 0
                     THEN user_id END) AS active_1d,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0
                     THEN user_id END) AS active_7d,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_14d > 0 OR app_sessions_14d > 0 OR emails_14d > 0
                     THEN user_id END) AS active_14d,
       COUNT(DISTINCT
             CASE
                 WHEN web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0
                     THEN user_id END) AS active_30d

FROM user_activity
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base CLONE se.data.active_user_base;
CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base_bkup CLONE se.data.active_user_base;

SELECT MAX(date)
FROM se_dev_robin.data.active_user_base; --2020-04-22

CREATE OR REPLACE PROCEDURE scratch.robinpatel.backfill_active_users_loop(p_first_run DOUBLE, p_max_runs DOUBLE
                                                                         )
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT
AS
$$
var i;
for (i = P_FIRST_RUN; i < P_MAX_RUNS; i++) {
    var sql_command = `SELECT '''' || TO_CHAR(DATEADD(DAY, -${i}, current_date)) || ''''`;
    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    var res = stmt.execute();
    res.next()
    var date_var = res.getColumnValue(1);
    var sql_command =
        `INSERT INTO se_dev_robin.data.active_user_base
        WITH web_session_activity AS (
            SELECT attributed_user_id,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, ua.date) THEN 1 ELSE 0 END) AS sessions_1d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, ua.date) THEN 1 ELSE 0 END) AS sessions_7d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, ua.date) THEN 1 ELSE 0 END) AS sessions_14d,
                count(*) AS sessions_30d
            FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
            WHERE touch_start_tstamp >= DATEADD(DAY, -30, ua.date)
                AND touch_start_tstamp <= ua.date
                AND stitched_identity_type = 'se_user_id'
                AND touch_experience != 'native app'
            GROUP BY 1
        ),
        app_session_activity AS (
            SELECT attributed_user_id,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, ua.date) THEN 1 ELSE 0 END) AS sessions_1d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, ua.date) THEN 1 ELSE 0 END) AS sessions_7d,
                SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, ua.date) THEN 1 ELSE 0 END) AS sessions_14d,
                count(*) AS sessions_30d
            FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
            WHERE touch_start_tstamp >= DATEADD(DAY, -30, ua.date)
                AND touch_start_tstamp <= ua.date
                AND stitched_identity_type = 'se_user_id'
                AND touch_experience = 'native app'
            GROUP BY 1
        ),
        email_activity AS (
            SELECT user_id,
                SUM(CASE WHEN date >= DATEADD(DAY, -1, ua.date) THEN 1 ELSE 0 END) AS emails_1d,
                SUM(CASE WHEN date >= DATEADD(DAY, -7, ua.date) THEN 1 ELSE 0 END) AS emails_7d,
                SUM(CASE WHEN date >= DATEADD(DAY, -14, ua.date) THEN 1 ELSE 0 END) AS emails_14d,
                count(*) AS emails_30d
            FROM se.data.user_emails
            WHERE date >= DATEADD(DAY, -30, ua.date)
                AND date <= ua.date
                AND opens > 0 --any user with an open
            GROUP BY 1
        ),
        user_activity AS (
         SELECT u.id                                   AS user_id,
                t.name                                 AS territory,
                COALESCE(b.booker_segment, 'Prospect') AS booker_segment,
                COALESCE(ws.sessions_1d, 0)            AS web_sessions_1d,
                COALESCE(ws.sessions_7d, 0)            AS web_sessions_7d,
                COALESCE(ws.sessions_14d, 0)           AS web_sessions_14d,
                COALESCE(ws.sessions_30d, 0)           AS web_sessions_30d,

                COALESCE(aps.sessions_1d, 0)           AS app_sessions_1d,
                COALESCE(aps.sessions_7d, 0)           AS app_sessions_7d,
                COALESCE(aps.sessions_14d, 0)          AS app_sessions_14d,
                COALESCE(aps.sessions_30d, 0)          AS app_sessions_30d,

                COALESCE(e.emails_1d, 0)               AS emails_1d,
                COALESCE(e.emails_7d, 0)               AS emails_7d,
                COALESCE(e.emails_14d, 0)              AS emails_14d,
                COALESCE(e.emails_30d, 0)              AS emails_30d
         FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
                INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
                LEFT JOIN se.data.user_segmentation b ON u.id = b.shiro_user_id AND b.date = ua.date
                LEFT JOIN web_session_activity ws ON u.id = ws.attributed_user_id
                LEFT JOIN app_session_activity aps ON u.id = aps.attributed_user_id
                LEFT JOIN email_activity e ON u.id = e.user_id
         WHERE u.date_created <= ua.date
        )
        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-20 00:00:00.000'       AS run_tstamp,
            'stuck job backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'web_active' AS platform,
            territory,
            booker_segment,
            COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8,9

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-20 00:00:00.000'       AS run_tstamp,
            'stuck job backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'app_active' AS platform,
            territory,
            booker_segment,
            COUNT(DISTINCT CASE WHEN app_sessions_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN app_sessions_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN app_sessions_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN app_sessions_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8,9

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-20 00:00:00.000'       AS run_tstamp,
            'stuck job backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'email_active' AS platform,
            territory,
            booker_segment,
            COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8,9

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            '2020-04-20 00:00:00.000'       AS run_tstamp,
            'stuck job backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'user_active' AS platform,
            territory,
            booker_segment,
            COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 OR app_sessions_1d > 0 OR emails_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 OR app_sessions_14d > 0 OR emails_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0 THEN user_id END) AS active_30d
        FROM user_activity
        GROUP BY 1,2,3,4,5,6,7,8,9;`;

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
};
$$;
TRUNCATE se_dev_robin.data.active_user_base;
USE WAREHOUSE pipe_large;
CALL scratch.robinpatel.backfill_active_users_loop(1, 5);

SELECT date, COUNT(*)
FROM se_dev_robin.data.active_user_base
GROUP BY 1;

SELECT date, COUNT(*)
FROM se.data.active_user_base
GROUP BY 1;

CREATE OR REPLACE TABLE se.data.active_user_base CLONE se_dev_robin.data.active_user_base;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE se_dev_robin.data.user_emails CLONE se.data.user_emails;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
self_describing_task --include 'dv/active_user_base/active_user_base'  --method 'run' --start '2020-04-06 00:00:00' --end '2020-04-06 00:00:00'

SELECT *
FROM se_dev_robin.data.active_user_base;

UPDATE se_dev_robin.data.active_user_base AS target
SET target.operation_id = 'backfill failed run';



self_describing_task --include 'dv/active_user_base/active_user_base'  --method 'run' --start '2020-04-06 00:00:00' --end '2020-04-06 00:00:00'

SELECT e.date,
       t.name                     AS territory,
       s.booker_segment,
       SUM(COALESCE(e.sends, 0))  AS sends,
       SUM(COALESCE(e.opens, 0))  AS opens,
       SUM(COALESCE(e.clicks, 0)) AS clicks

FROM se.data.user_emails e
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u ON e.user_id = u.id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
    INNER JOIN se.data.user_segmentation s ON e.user_id = s.shiro_user_id AND e.date = s.date
WHERE e.date >= '2019-03-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2;

SELECT date, SUM(sends)
FROM se.data.user_emails
WHERE date >= '2020-03-01'
GROUP BY 1;


WITH sends AS (
    SELECT e.date,
           t.name                     AS territory,
           s.booker_segment,
           SUM(COALESCE(e.sends, 0))  AS sends,
           SUM(COALESCE(e.opens, 0))  AS opens,
           SUM(COALESCE(e.clicks, 0)) AS clicks

    FROM se.data.user_emails e
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u ON e.user_id = u.id
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a
                   ON u.original_affiliate_id = a.id
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
        INNER JOIN se.data.user_segmentation s ON e.user_id = s.shiro_user_id AND e.date = s.date
    WHERE e.date >= '2019-03-01'
    GROUP BY 1, 2, 3
)

SELECT date,
       SUM(sends)
FROM sends
GROUP BY 1;

SELECT date, COUNT(*)
FROM se.data.active_user_base
GROUP BY 1;


SELECT date, COUNT(*)
FROM se.data.user_emails
WHERE date >= '2020-01-01'
GROUP BY 1;

USE WAREHOUSE pipe_large;

SELECT date,

       SUM(active_1d)  AS active_1d,
       SUM(active_7d)  AS active_7d,
       SUM(active_14d) AS active_14d,
       SUM(active_30d) AS active_30d
FROM se.data.active_user_base
GROUP BY 1
ORDER BY 1;

GRANT USAGE ON SCHEMA collab.user_eng_segments TO ROLE personal_role__carmenmardiros;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.user_eng_segments TO ROLE personal_role__carmenmardiros;

SELECT *
FROM collab.information_schema.tables
WHERE table_schema = 'USER_ENG_SEGMENTS';

CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base CLONE se.data.active_user_base;

airflow backfill --start_date '2020-04-20 03:00:00' --end_date '2020-04-20 03:00:00' --task_regex '.*' dwh__user_segmentation__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
--include opt in status and 90d activity bucket
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.active_user_base
(

    -- (lineage) metadata for the current job
    schedule_tstamp TIMESTAMP,
    run_tstamp      TIMESTAMP,
    operation_id    VARCHAR,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,

    date            DATE,
    platform        VARCHAR,
    territory       VARCHAR,
    booker_segment  VARCHAR,
    opt_in_status   VARCHAR,
    active_1d       INT,
    active_7d       INT,
    active_14d      INT,
    active_30d      INT,
    active_90d      INT
)
    CLUSTER BY (date);

CREATE OR REPLACE PROCEDURE scratch.robinpatel.backfill_active_users_loop(p_first_run DOUBLE, p_max_runs DOUBLE
                                                                         )
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT
AS
$$
var i;
for (i = P_FIRST_RUN; i < P_MAX_RUNS; i++) {
    var sql_command = `SELECT '''' || TO_CHAR(DATEADD(DAY, -${i}, current_date)) || ''''`;
    var stmt = snowflake.createStatement( {sqlText: sql_command} );
    var res = stmt.execute();
    res.next()
    var date_var = res.getColumnValue(1);
    var sql_command =
        `INSERT INTO data_vault_mvp_dev_robin.dwh.active_user_base
        --run no: ${i}
        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            current_date                    AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'web_active' AS platform,
            t.name as territory,
            COALESCE(sg.booker_segment, 'Prospect') AS booker_segment,
            COALESCE(sg.opt_in_status, 'opted out') AS opt_in_status,
            COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 THEN user_id END) AS active_30d,
            COUNT(DISTINCT CASE WHEN web_sessions_90d > 0 THEN user_id END) AS active_90d
        FROM se.data.user_activity ua
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.user_id = su.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
            LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
        GROUP BY 1,2,3,4,5,6,7,8,9,10

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            current_date                    AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'app_active' AS platform,
            t.name as territory,
            COALESCE(sg.booker_segment, 'Prospect') AS booker_segment,
            COALESCE(sg.opt_in_status, 'opted out') AS opt_in_status,
            COUNT(DISTINCT CASE WHEN app_sessions_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN app_sessions_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN app_sessions_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN app_sessions_30d > 0 THEN user_id END) AS active_30d,
            COUNT(DISTINCT CASE WHEN app_sessions_90d > 0 THEN user_id END) AS active_90d
        FROM se.data.user_activity ua
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.user_id = su.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
            LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
        GROUP BY 1,2,3,4,5,6,7,8,9,10

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            current_date                    AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'email_active' AS platform,
            t.name as territory,
            COALESCE(sg.booker_segment, 'Prospect') AS booker_segment,
            COALESCE(sg.opt_in_status, 'opted out') AS opt_in_status,
            COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN user_id END) AS active_30d,
            COUNT(DISTINCT CASE WHEN emails_90d > 0 THEN user_id END) AS active_90d
        FROM se.data.user_activity ua
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.user_id = su.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
            LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
        GROUP BY 1,2,3,4,5,6,7,8,9,10

        UNION ALL

        SELECT
            '1970-01-01 00:00:00.000'       AS schedule_tstamp,
            current_date                    AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ua.date AS date,
            'user_active' AS platform,
            t.name as territory,
            COALESCE(sg.booker_segment, 'Prospect') AS booker_segment,
            COALESCE(sg.opt_in_status, 'opted out') AS opt_in_status,
            COUNT(DISTINCT CASE WHEN web_sessions_1d > 0 OR app_sessions_1d > 0 OR emails_1d > 0 THEN user_id END) AS active_1d,
            COUNT(DISTINCT CASE WHEN web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0 THEN user_id END) AS active_7d,
            COUNT(DISTINCT CASE WHEN web_sessions_14d > 0 OR app_sessions_14d > 0 OR emails_14d > 0 THEN user_id END) AS active_14d,
            COUNT(DISTINCT CASE WHEN web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0 THEN user_id END) AS active_30d,
            COUNT(DISTINCT CASE WHEN web_sessions_90d > 0 OR app_sessions_90d > 0 OR emails_90d > 0 THEN user_id END) AS active_90d
        FROM se.data.user_activity ua
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.user_id = su.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
            INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
            LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
        GROUP BY 1,2,3,4,5,6,7,8,9,10;`;

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
};
$$
;

USE WAREHOUSE pipe_2xlarge;
INSERT INTO data_vault_mvp_dev_robin.dwh.active_user_base
SELECT '1970-01-01 00:00:00.000'                                                   AS schedule_tstamp,
       CURRENT_DATE                                                                AS run_tstamp,
       'initial backfill'                                                          AS operation_id,
       CURRENT_DATE                                                                AS created_at,
       CURRENT_DATE                                                                AS updated_at,

       ua.date                                                                     AS date,
       'web_active'                                                                AS platform,
       su.original_affiliate_territory                                             AS territory,
       COALESCE(sg.booker_segment, 'Prospect')                                     AS booker_segment,
       COALESCE(sg.opt_in_status, 'opted out')                                     AS opt_in_status,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_1d > 0 THEN su.shiro_user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_7d > 0 THEN su.shiro_user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_14d > 0 THEN su.shiro_user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_30d > 0 THEN su.shiro_user_id END) AS active_30d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_90d > 0 THEN su.shiro_user_id END) AS active_90d
FROM se.data.user_activity ua
    INNER JOIN data_vault_mvp.dwh.user_attributes su ON ua.shiro_user_id = su.shiro_user_id
    LEFT JOIN  se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION ALL

SELECT '1970-01-01 00:00:00.000'                                                   AS schedule_tstamp,
       CURRENT_DATE                                                                AS run_tstamp,
       'initial backfill'                                                          AS operation_id,
       CURRENT_DATE                                                                AS created_at,
       CURRENT_DATE                                                                AS updated_at,

       ua.date                                                                     AS date,
       'app_active'                                                                AS platform,
       su.original_affiliate_territory                                             AS territory,
       COALESCE(sg.booker_segment, 'Prospect')                                     AS booker_segment,
       COALESCE(sg.opt_in_status, 'opted out')                                     AS opt_in_status,
       COUNT(DISTINCT CASE WHEN ua.app_sessions_1d > 0 THEN su.shiro_user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN ua.app_sessions_7d > 0 THEN su.shiro_user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN ua.app_sessions_14d > 0 THEN su.shiro_user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN ua.app_sessions_30d > 0 THEN su.shiro_user_id END) AS active_30d,
       COUNT(DISTINCT CASE WHEN ua.app_sessions_90d > 0 THEN su.shiro_user_id END) AS active_90d
FROM se.data.user_activity ua
    INNER JOIN data_vault_mvp.dwh.user_attributes su ON ua.shiro_user_id = su.shiro_user_id
    LEFT JOIN  se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION ALL

SELECT '1970-01-01 00:00:00.000'                                          AS schedule_tstamp,
       CURRENT_DATE                                                       AS run_tstamp,
       'initial backfill'                                                 AS operation_id,
       CURRENT_DATE                                                       AS created_at,
       CURRENT_DATE                                                       AS updated_at,

       ua.date                                                            AS date,
       'email_active'                                                     AS platform,
       su.original_affiliate_territory                                    AS territory,
       COALESCE(sg.booker_segment, 'Prospect')                            AS booker_segment,
       COALESCE(sg.opt_in_status, 'opted out')                            AS opt_in_status,
       COUNT(DISTINCT CASE WHEN emails_1d > 0 THEN su.shiro_user_id END)  AS active_1d,
       COUNT(DISTINCT CASE WHEN emails_7d > 0 THEN su.shiro_user_id END)  AS active_7d,
       COUNT(DISTINCT CASE WHEN emails_14d > 0 THEN su.shiro_user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN emails_30d > 0 THEN su.shiro_user_id END) AS active_30d,
       COUNT(DISTINCT CASE WHEN emails_90d > 0 THEN su.shiro_user_id END) AS active_90d
FROM se.data.user_activity ua
    INNER JOIN data_vault_mvp.dwh.user_attributes su ON ua.shiro_user_id = su.shiro_user_id
    LEFT JOIN  se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION ALL

SELECT '1970-01-01 00:00:00.000'                                                                                                   AS schedule_tstamp,
       CURRENT_DATE                                                                                                                AS run_tstamp,
       'initial backfill'                                                                                                          AS operation_id,
       CURRENT_DATE                                                                                                                AS created_at,
       CURRENT_DATE                                                                                                                AS updated_at,

       ua.date                                                                                                                     AS date,
       'user_active'                                                                                                               AS platform,
       su.original_affiliate_territory                                                                                             AS territory,
       COALESCE(sg.booker_segment, 'Prospect')                                                                                     AS booker_segment,
       COALESCE(sg.opt_in_status, 'opted out')                                                                                     AS opt_in_status,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_1d > 0 OR ua.app_sessions_1d > 0 OR ua.emails_1d > 0 THEN su.shiro_user_id END)    AS active_1d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_7d > 0 OR ua.app_sessions_7d > 0 OR ua.emails_7d > 0 THEN su.shiro_user_id END)    AS active_7d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_14d > 0 OR ua.app_sessions_14d > 0 OR ua.emails_14d > 0 THEN su.shiro_user_id END) AS active_14d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_30d > 0 OR ua.app_sessions_30d > 0 OR ua.emails_30d > 0 THEN su.shiro_user_id END) AS active_30d,
       COUNT(DISTINCT CASE WHEN ua.web_sessions_90d > 0 OR ua.app_sessions_90d > 0 OR ua.emails_90d > 0 THEN su.shiro_user_id END) AS active_90d
FROM se.data.user_activity ua
    INNER JOIN data_vault_mvp.dwh.user_attributes su ON ua.shiro_user_id = su.shiro_user_id
    LEFT JOIN  se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;



SELECT updated_at, COUNT(*)
FROM se_dev_robin.data.user_activity
GROUP BY 1;

SELECT au.date,
       au.platform,
       au.territory,
       au.booker_segment,
       au.opt_in_status,
       au.active_1d,
       au.active_7d,
       au.active_14d,
       au.active_30d,
       au.active_90d
FROM data_vault_mvp_dev_robin.dwh.active_user_base au
    EXCEPT
SELECT aub.date,
       aub.platform,
       aub.territory,
       aub.booker_segment,
       aub.opt_in_status,
       aub.active_1d,
       aub.active_7d,
       aub.active_14d,
       aub.active_30d,
       aub.active_90d
FROM data_vault_mvp.dwh.active_user_base aub;

SELECT aub.date,
       aub.platform,
       aub.territory,
       aub.booker_segment,
       aub.opt_in_status,
       aub.active_1d,
       aub.active_7d,
       aub.active_14d,
       aub.active_30d,
       aub.active_90d
FROM data_vault_mvp.dwh.active_user_base aub
WHERE date = CURRENT_DATE - 4;
SELECT aub.date,
       aub.platform,
       aub.territory,
       aub.booker_segment,
       aub.opt_in_status,
       aub.active_1d,
       aub.active_7d,
       aub.active_14d,
       aub.active_30d,
       aub.active_90d
FROM data_vault_mvp_dev_robin.dwh.active_user_base aub
WHERE date = CURRENT_DATE - 4;

SELECT aub.date,
       aub.platform,
       aub.territory,
       aub.booker_segment,
       aub.opt_in_status,
       COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.active_user_base aub
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) > 1;



self_describing_task
--include 'se/data/se_active_user_base'  --method 'run' --start '2020-04-27 00:00:00' --end '2020-04-27 00:00:00'


--check outputs match original
SELECT date,
       platform,
       SUM(active_1d)  AS active_1d,
       SUM(active_7d)  AS active_7d,
       SUM(active_14d) AS active_14d,
       SUM(active_30d) AS active_30d
FROM se_dev_robin.data.active_user_base
WHERE date IN ('2020-04-25', '2020-04-24')
GROUP BY 1, 2;

SELECT date,
       platform,
       SUM(active_1d)  AS active_1d,
       SUM(active_7d)  AS active_7d,
       SUM(active_14d) AS active_14d,
       SUM(active_30d) AS active_30d
FROM se.data.active_user_base
WHERE date IN ('2020-04-25', '2020-04-24')
GROUP BY 1, 2;

SELECT date,
       COUNT(*)
FROM se_dev_robin.data.active_user_base
GROUP BY 1;

SELECT MAX(date)
FROM se_dev_robin.data.active_user_base;

CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base_bkup CLONE se.data.active_user_base;

CREATE OR REPLACE TABLE se.data.active_user_base CLONE se_dev_robin.data.active_user_base;

airflow backfill --start_date '2020-04-27 03:00:00' --end_date '2020-04-27 03:00:00' --task_regex '.*' active_user_base__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
--adjust tableau report

WITH email_sends AS (
    SELECT e.date,
           t.name                    AS territory,
           s.booker_segment,
           s.opt_in_status,
           SUM(COALESCE(e.sends, 0)) AS sends

    FROM se.data.user_emails e
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u ON e.user_id = u.id
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
        INNER JOIN se.data.user_segmentation s ON e.user_id = s.shiro_user_id AND e.date = s.date
    WHERE e.date >= '2019-01-01'
    GROUP BY 1, 2, 3, 4
)

SELECT a.date,
       a.platform,
       a.territory,
       a.booker_segment,
       a.opt_in_status,
       a.active_1d,
       a.active_7d,
       a.active_14d,
       a.active_30d,
       a.active_90d,
       MAX(e.sends) AS sends
FROM se.data.active_user_base a
    LEFT JOIN email_sends e
              ON a.date = e.date
                  AND a.territory = e.territory
                  AND a.booker_segment = e.booker_segment
                  AND a.opt_in_status = e.opt_in_status
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;


SELECT date,
       updated_at::DATE,
       COUNT(*)
FROM se.data.active_user_base
GROUP BY 1, 2;

SELECT date, COUNT(*)
FROM data_vault_mvp.dwh.user_activity ua
GROUP BY 1;


SELECT *
FROM se.data.se_booking sb