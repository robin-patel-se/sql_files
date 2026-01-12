CREATE OR REPLACE PROCEDURE scratch.localuser.backfill_user_activity_loop(p_first_run DOUBLE, p_max_runs DOUBLE
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
        INSERT INTO data_vault_mvp_dev_localuser.dwh.user_activity
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
            '2021-01-24 00:00:00.000'       AS run_tstamp,
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


CALL scratch.localuser.backfill_user_activity_loop(1, 50);