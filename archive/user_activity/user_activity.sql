USE WAREHOUSE pipe_xlarge;
SET date_var = dateadd(DAY, -1, current_date);
WITH web_session_activity AS (
    SELECT attributed_user_id,
           sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS sessions_1d,
           sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS sessions_7d,
           sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS sessions_14d,
           sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -30, $date_var) THEN 1 ELSE 0 END) AS sessions_30d,
           count(*)                                                                            AS sessions_90d
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
    WHERE touch_start_tstamp >= dateadd(DAY, -90, $date_var)
      AND touch_start_tstamp <= $date_var
      AND stitched_identity_type = 'se_user_id'
      AND touch_experience != 'native app'
    GROUP BY 1
),
     app_session_activity AS (
         SELECT attributed_user_id,
                sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS sessions_1d,
                sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS sessions_7d,
                sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS sessions_14d,
                sum(CASE WHEN touch_start_tstamp >= dateadd(DAY, -30, $date_var) THEN 1 ELSE 0 END) AS sessions_30d,
                count(*)                                                                            AS sessions_90d
         FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
         WHERE touch_start_tstamp >= dateadd(DAY, -90, $date_var)
           AND touch_start_tstamp <= $date_var
           AND stitched_identity_type = 'se_user_id'
           AND touch_experience = 'native app'
         GROUP BY 1
     ),
     email_activity AS (
         SELECT user_id,
                sum(CASE WHEN date >= dateadd(DAY, -1, $date_var) THEN 1 ELSE 0 END)  AS emails_1d,
                sum(CASE WHEN date >= dateadd(DAY, -7, $date_var) THEN 1 ELSE 0 END)  AS emails_7d,
                sum(CASE WHEN date >= dateadd(DAY, -14, $date_var) THEN 1 ELSE 0 END) AS emails_14d,
                sum(CASE WHEN date >= dateadd(DAY, -30, $date_var) THEN 1 ELSE 0 END) AS emails_30d,
                count(*)                                                              AS emails_90d
         FROM se.data.user_emails
         WHERE date >= dateadd(DAY, -90, $date_var)
           AND date <= $date_var
           AND opens > 0 --any user with an open
         GROUP BY 1
     )
SELECT $date_var                                                          AS date,
       coalesce(ws.attributed_user_id, aps.attributed_user_id, e.user_id) AS user_id,

       coalesce(ws.sessions_1d, 0)                                        AS web_sessions_1d,
       coalesce(ws.sessions_7d, 0)                                        AS web_sessions_7d,
       coalesce(ws.sessions_14d, 0)                                       AS web_sessions_14d,
       coalesce(ws.sessions_30d, 0)                                       AS web_sessions_30d,
       coalesce(ws.sessions_90d, 0)                                       AS web_sessions_90d,

       coalesce(aps.sessions_1d, 0)                                       AS app_sessions_1d,
       coalesce(aps.sessions_7d, 0)                                       AS app_sessions_7d,
       coalesce(aps.sessions_14d, 0)                                      AS app_sessions_14d,
       coalesce(aps.sessions_30d, 0)                                      AS app_sessions_30d,
       coalesce(aps.sessions_90d, 0)                                      AS app_sessions_90d,

       coalesce(e.emails_1d, 0)                                           AS emails_1d,
       coalesce(e.emails_7d, 0)                                           AS emails_7d,
       coalesce(e.emails_14d, 0)                                          AS emails_14d,
       coalesce(e.emails_30d, 0)                                          AS emails_30d,
       coalesce(e.emails_90d, 0)                                          AS emails_90d

FROM web_session_activity ws
         FULL JOIN app_session_activity aps ON ws.attributed_user_id = aps.attributed_user_id
         FULL JOIN email_activity e ON coalesce(ws.attributed_user_id, aps.attributed_user_id) = e.user_id;


CREATE OR REPLACE TABLE se_dev_robin.data.user_activity
(
    date             DATE,
    shiro_user_id    INT,
    web_sessions_1d  INT,
    web_sessions_7d  INT,
    web_sessions_14d INT,
    web_sessions_30d INT,
    web_sessions_90d INT,
    app_sessions_1d  INT,
    app_sessions_7d  INT,
    app_sessions_14d INT,
    app_sessions_30d INT,
    app_sessions_90d INT,
    emails_1d        INT,
    emails_7d        INT,
    emails_14d       INT,
    emails_30d       INT,
    emails_90d       INT
);


------------------------------------------------------------------------------------------------------------------------
--stored procedure

CREATE OR REPLACE PROCEDURE scratch.robinpatel.backfill_user_activity_loop(p_first_run DOUBLE, p_max_runs DOUBLE
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
        `
        INSERT INTO data_vault_mvp_dev_robin.dwh.user_activity
        --run no: ${i}
        WITH web_session_activity AS (
            SELECT attributed_user_id,
                   SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, ${date_var}) THEN 1 ELSE 0 END)  AS sessions_1d,
                   SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, ${date_var}) THEN 1 ELSE 0 END)  AS sessions_7d,
                   SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, ${date_var}) THEN 1 ELSE 0 END) AS sessions_14d,
                   SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -30, ${date_var}) THEN 1 ELSE 0 END) AS sessions_30d,
                   count(*)                                                                            AS sessions_90d
            FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
            WHERE touch_start_tstamp >= DATEADD(DAY, -90, ${date_var})
              AND touch_start_tstamp <= ${date_var}
              AND stitched_identity_type = 'se_user_id'
              AND LOWER(se.data.platform_from_touch_experience(touch_experience)) IS DISTINCT FROM 'native app'
            GROUP BY 1
        ),
             app_session_activity AS (
                 SELECT attributed_user_id,
                        SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -1, ${date_var}) THEN 1 ELSE 0 END)  AS sessions_1d,
                        SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -7, ${date_var}) THEN 1 ELSE 0 END)  AS sessions_7d,
                        SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -14, ${date_var}) THEN 1 ELSE 0 END) AS sessions_14d,
                        SUM(CASE WHEN touch_start_tstamp >= DATEADD(DAY, -30, ${date_var}) THEN 1 ELSE 0 END) AS sessions_30d,
                        count(*)                                                                            AS sessions_90d
                 FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
                 WHERE touch_start_tstamp >= DATEADD(DAY, -90, ${date_var})
                   AND touch_start_tstamp <= ${date_var}
                   AND stitched_identity_type = 'se_user_id'
                   AND LOWER(se.data.platform_from_touch_experience(touch_experience)) = 'native app'
                 GROUP BY 1
             ),
             email_activity AS (
                 SELECT user_id,
                        SUM(CASE WHEN date >= DATEADD(DAY, -1, ${date_var}) THEN 1 ELSE 0 END)  AS emails_1d,
                        SUM(CASE WHEN date >= DATEADD(DAY, -7, ${date_var}) THEN 1 ELSE 0 END)  AS emails_7d,
                        SUM(CASE WHEN date >= DATEADD(DAY, -14, ${date_var}) THEN 1 ELSE 0 END) AS emails_14d,
                        SUM(CASE WHEN date >= DATEADD(DAY, -30, ${date_var}) THEN 1 ELSE 0 END) AS emails_30d,
                        count(*)                                                              AS emails_90d
                 FROM se.data.user_emails
                 WHERE date >= DATEADD(DAY, -90, ${date_var})
                   AND date <= ${date_var}
                   AND opens > 0 --any user with an open
                 GROUP BY 1
             )
        SELECT
            ${date_var}::DATE               AS schedule_tstamp,
            '2020-04-27 00:00:00.000'       AS run_tstamp,
            'initial backfill'              AS operation_id,
            current_date                    AS created_at,
            current_date                    AS updated_at,

            ${date_var}::DATE                       AS date,
            COALESCE(ws.attributed_user_id, aps.attributed_user_id, e.user_id) AS shiro_user_id,

            COALESCE(ws.sessions_1d, 0)             AS web_sessions_1d,
            COALESCE(ws.sessions_7d, 0)             AS web_sessions_7d,
            COALESCE(ws.sessions_14d, 0)            AS web_sessions_14d,
            COALESCE(ws.sessions_30d, 0)            AS web_sessions_30d,
            COALESCE(ws.sessions_90d, 0)            AS web_sessions_90d,

            COALESCE(aps.sessions_1d, 0)            AS app_sessions_1d,
            COALESCE(aps.sessions_7d, 0)            AS app_sessions_7d,
            COALESCE(aps.sessions_14d, 0)           AS app_sessions_14d,
            COALESCE(aps.sessions_30d, 0)           AS app_sessions_30d,
            COALESCE(aps.sessions_90d, 0)           AS app_sessions_90d,

            COALESCE(e.emails_1d, 0)                AS emails_1d,
            COALESCE(e.emails_7d, 0)                AS emails_7d,
            COALESCE(e.emails_14d, 0)               AS emails_14d,
            COALESCE(e.emails_30d, 0)               AS emails_30d,
            COALESCE(e.emails_90d, 0)               AS emails_90d

        FROM web_session_activity ws
             FULL JOIN app_session_activity aps ON ws.attributed_user_id = aps.attributed_user_id
             FULL JOIN email_activity e ON COALESCE(ws.attributed_user_id, aps.attributed_user_id) = e.user_id;
        `;

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
};
$$;

USE WAREHOUSE pipe_xlarge;
CALL scratch.robinpatel.backfill_user_activity_loop(1, 30);

SELECT *
FROM se_dev_robin.data.user_activity;

GRANT SELECT ON TABLE se.data.user_activity TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE se.data.user_activity TO ROLE personal_role__carmenmardiros;
GRANT SELECT ON TABLE se.data.user_activity TO ROLE personal_role__alexscottsimons;
GRANT SELECT ON TABLE se.data.user_activity TO ROLE personal_role__richardkunert;
GRANT SELECT ON TABLE se.data.user_activity TO ROLE personal_role__cianweeresinghe;

GRANT USAGE ON SCHEMA se_dev_robin.data TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA se_dev_robin.data TO ROLE personal_role__carmenmardiros;
GRANT USAGE ON SCHEMA se_dev_robin.data TO ROLE personal_role__alexscottsimons;
GRANT USAGE ON SCHEMA se_dev_robin.data TO ROLE personal_role__richardkunert;
GRANT USAGE ON SCHEMA se_dev_robin.data TO ROLE personal_role__cianweeresinghe;

SELECT date, count(*)
FROM se_dev_robin.data.user_activity
GROUP BY 1;

USE WAREHOUSE pipe_medium;

------------------------------------------------------------------------------------------------------------------------

SELECT date,

       count(DISTINCT CASE WHEN web_sessions_1d > 0 THEN shiro_user_id END)  AS web_active_1d,
       count(DISTINCT CASE WHEN web_sessions_7d > 0 THEN shiro_user_id END)  AS web_active_7d,
       count(DISTINCT CASE WHEN web_sessions_14d > 0 THEN shiro_user_id END) AS web_active_14d,
       count(DISTINCT CASE WHEN web_sessions_30d > 0 THEN shiro_user_id END) AS web_active_30d,

       count(DISTINCT CASE WHEN app_sessions_1d > 0 THEN shiro_user_id END)  AS app_active_1d,
       count(DISTINCT CASE WHEN app_sessions_7d > 0 THEN shiro_user_id END)  AS app_active_7d,
       count(DISTINCT CASE WHEN app_sessions_14d > 0 THEN shiro_user_id END) AS app_active_14d,
       count(DISTINCT CASE WHEN app_sessions_30d > 0 THEN shiro_user_id END) AS app_active_30d,

       count(DISTINCT CASE WHEN emails_1d > 0 THEN shiro_user_id END)        AS email_active_1d,
       count(DISTINCT CASE WHEN emails_7d > 0 THEN shiro_user_id END)        AS email_active_7d,
       count(DISTINCT CASE WHEN emails_14d > 0 THEN shiro_user_id END)       AS email_active_14d,
       count(DISTINCT CASE WHEN emails_30d > 0 THEN shiro_user_id END)       AS email_active_30d

FROM se_dev_robin.data.user_activity
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--populate the active user base
SELECT ua.date,
       'web_active'                                                                AS platform,
       t.name                                                                      AS territory,
       coalesce(sg.booker_segment, 'Prospect')                                     AS booker_segment,
       coalesce(sg.opt_in_status, 'opted out')                                     AS opt_in_status,

       count(DISTINCT CASE WHEN ua.web_sessions_1d > 0 THEN ua.shiro_user_id END)  AS active_1d,
       count(DISTINCT CASE WHEN ua.web_sessions_7d > 0 THEN ua.shiro_user_id END)  AS active_7d,
       count(DISTINCT CASE WHEN ua.web_sessions_14d > 0 THEN ua.shiro_user_id END) AS active_14d,
       count(DISTINCT CASE WHEN ua.web_sessions_30d > 0 THEN ua.shiro_user_id END) AS active_30d,
       count(DISTINCT CASE WHEN ua.web_sessions_90d > 0 THEN ua.shiro_user_id END) AS active_90d

FROM se_dev_robin.data.user_activity ua
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.shiro_user_id = su.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
         LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT ua.date,
       'app_active'                                                                AS platform,
       t.name                                                                      AS territory,
       coalesce(sg.booker_segment, 'Prospect')                                     AS booker_segment,
       coalesce(sg.opt_in_status, 'opted out')                                     AS opt_in_status,

       count(DISTINCT CASE WHEN ua.app_sessions_1d > 0 THEN ua.shiro_user_id END)  AS active_1d,
       count(DISTINCT CASE WHEN ua.app_sessions_7d > 0 THEN ua.shiro_user_id END)  AS active_7d,
       count(DISTINCT CASE WHEN ua.app_sessions_14d > 0 THEN ua.shiro_user_id END) AS active_14d,
       count(DISTINCT CASE WHEN ua.app_sessions_30d > 0 THEN ua.shiro_user_id END) AS active_30d,
       count(DISTINCT CASE WHEN ua.app_sessions_90d > 0 THEN ua.shiro_user_id END) AS active_90d

FROM se_dev_robin.data.user_activity ua
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.shiro_user_id = su.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
         LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT ua.date,
       'email_active'                                                        AS platform,
       t.name                                                                AS territory,
       coalesce(sg.booker_segment, 'Prospect')                               AS booker_segment,
       coalesce(sg.opt_in_status, 'opted out')                               AS opt_in_status,

       count(DISTINCT CASE WHEN ua.emails_1d > 0 THEN ua.shiro_user_id END)  AS active_1d,
       count(DISTINCT CASE WHEN ua.emails_7d > 0 THEN ua.shiro_user_id END)  AS active_7d,
       count(DISTINCT CASE WHEN ua.emails_14d > 0 THEN ua.shiro_user_id END) AS active_14d,
       count(DISTINCT CASE WHEN ua.emails_30d > 0 THEN ua.shiro_user_id END) AS active_30d,
       count(DISTINCT CASE WHEN ua.emails_90d > 0 THEN ua.shiro_user_id END) AS active_90d

FROM se_dev_robin.data.user_activity ua
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.shiro_user_id = su.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
         LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT ua.date,
       'user_active'                                     AS platform,
       t.name                                            AS territory,
       coalesce(sg.booker_segment, 'Prospect')           AS booker_segment,
       coalesce(sg.opt_in_status, 'opted out')           AS opt_in_status,

       count(DISTINCT CASE
                          WHEN web_sessions_1d > 0 OR app_sessions_1d > 0 OR emails_1d > 0
                              THEN ua.shiro_user_id END) AS active_1d,
       count(DISTINCT CASE
                          WHEN web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0
                              THEN ua.shiro_user_id END) AS active_7d,
       count(DISTINCT CASE
                          WHEN web_sessions_14d > 0 OR app_sessions_14d > 0 OR emails_14d > 0
                              THEN ua.shiro_user_id END) AS active_14d,
       count(DISTINCT CASE
                          WHEN web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0
                              THEN ua.shiro_user_id END) AS active_30d,
       count(DISTINCT CASE
                          WHEN web_sessions_90d > 0 OR app_sessions_90d > 0 OR emails_90d > 0
                              THEN ua.shiro_user_id END) AS active_90d

FROM se_dev_robin.data.user_activity ua
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON ua.shiro_user_id = su.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON su.original_affiliate_id = a.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
         LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND sg.date = ua.date
GROUP BY 1, 2, 3, 4, 5
;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM se_dev_robin.data.user_activity
WHERE shiro_user_id IN ('24127330',
                        '47369491',
                        '47288137',
                        '73608275',
                        '46701904',
                        '23284296',
                        '32321007',
                        '35636616',
                        '32130775'
    )

SELECT min(date)
FROM se_dev_robin.data.user_activity;
--2020-03-25

------------------------------------------------------------------------------------------------------------------------
ALTER TABLE se_dev_robin.data.user_activity
    RENAME TO user_activity_bkup;
CREATE OR REPLACE TABLE se_dev_robin.data.user_emails CLONE se.data.user_emails;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

self_describing_task --include 'se/data/se_user_activity'  --method 'run' --start '2020-04-26 03:00:00' --end '2020-04-26 03:00:00'


SELECT date,
       count(*)
FROM se_dev_robin.data.user_activity
GROUP BY 1;

USE WAREHOUSE pipe_xlarge;
CALL scratch.robinpatel.backfill_user_activity_loop(125, 483);

CREATE OR REPLACE TABLE se.data.user_activity CLONE se_dev_robin.data.user_activity;
------------------------------------------------------------------------------------------------------------------------
--testing
SELECT *
FROM se.data.user_activity; --selecting user 34464504 because has high 90d active sessions


SELECT touch_start_tstamp::DATE, count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
WHERE attributed_user_id = '34464504'
  AND touch_start_tstamp::DATE >= dateadd(DAY, -90, '2020-04-25')
GROUP BY 1; --output matches

SELECT max(date)
FROM se.data.user_activity;

airflow backfill --start_date '2020-04-27 03:00:00' --end_date '2020-04-27 03:00:00' --task_regex '.*' dwh__user_activity__daily_at_03h00

SELECT date,
       updated_at::DATE,
       count(*)
FROM se.data.user_activity
GROUP BY 1, 2;

SELECT updated_at, count(*)
FROM se.data.user_activity
GROUP BY 1;
------------------------------------------------------------------------------------------------------------------------
-- to recreate the table if we want to adjust activity buckets.
USE WAREHOUSE pipe_large;
CREATE OR REPLACE TABLE se_dev_robin.data.user_activity AS (
    SELECT schedule_tstamp,
           run_tstamp,
           operation_id,
           created_at,
           updated_at,
           date,
           shiro_user_id,
           web_sessions_1d,
           web_sessions_7d - web_sessions_1d   AS web_sessions_7d,
           web_sessions_14d - web_sessions_7d  AS web_sessions_14d,
           web_sessions_30d - web_sessions_14d AS web_sessions_30d,
           web_sessions_90d - web_sessions_30d AS web_sessions_90d,
           app_sessions_1d,
           app_sessions_7d - app_sessions_1d   AS app_sessions_7d,
           app_sessions_14d - app_sessions_7d  AS app_sessions_14d,
           app_sessions_30d - app_sessions_14d AS app_sessions_30d,
           app_sessions_90d - app_sessions_30d AS app_sessions_90d,
           emails_1d,
           emails_7d - emails_1d               AS emails_7d,
           emails_14d - emails_7d              AS emails_14d,
           emails_30d - emails_14d             AS emails_30d,
           emails_90d - emails_30d             AS emails_90d
    FROM se.data.user_activity
);


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
CALL scratch.robinpatel.backfill_user_activity_loop(714, 1079);

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity;
SELECT date, count(*)
FROM data_vault_mvp_dev_robin.dwh.user_activity
GROUP BY 1
ORDER BY 1;