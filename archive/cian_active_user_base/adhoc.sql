WITH active_base AS
         (SELECT date,
                 shiro_user_id,
                 CASE WHEN web_sessions_1d > 0 THEN 1 ELSE 0 END                                              AS web_active1d,
                 CASE WHEN app_sessions_1d > 0 THEN 1 ELSE 0 END                                              AS app_active1d,
                 CASE WHEN emails_1d > 0 THEN 1 ELSE 0 END                                                    AS email_active1d,
                 CASE
                     WHEN web_sessions_1d > 0 OR app_sessions_1d > 0 OR emails_1d > 0 THEN 1
                     ELSE 0 END                                                                               AS user_active1d,
                 CASE WHEN web_sessions_7d > 0 THEN 1 ELSE 0 END                                              AS web_active7d,
                 CASE WHEN app_sessions_7d > 0 THEN 1 ELSE 0 END                                              AS app_active7d,
                 CASE WHEN emails_7d > 0 THEN 1 ELSE 0 END                                                    AS email_active7d,
                 CASE
                     WHEN web_sessions_7d > 0 OR app_sessions_7d > 0 OR emails_7d > 0 THEN 1
                     ELSE 0 END                                                                               AS user_active7d,
                 CASE WHEN web_sessions_14d > 0 THEN 1 ELSE 0 END                                             AS web_active14d,
                 CASE WHEN app_sessions_14d > 0 THEN 1 ELSE 0 END                                             AS app_active14d,
                 CASE WHEN emails_14d > 0 THEN 1 ELSE 0 END                                                   AS email_active14d,
                 CASE
                     WHEN web_sessions_14d > 0 OR app_sessions_14d > 0 OR emails_14d > 0 THEN 1
                     ELSE 0 END                                                                               AS user_active14d,
                 CASE WHEN web_sessions_30d > 0 THEN 1 ELSE 0 END                                             AS web_active30d,
                 CASE WHEN app_sessions_30d > 0 THEN 1 ELSE 0 END                                             AS app_active30d,
                 CASE WHEN emails_30d > 0 THEN 1 ELSE 0 END                                                   AS email_active30d,
                 CASE
                     WHEN web_sessions_30d > 0 OR app_sessions_30d > 0 OR emails_30d > 0 THEN 1
                     ELSE 0 END                                                                               AS user_active30d,
                 CASE WHEN web_sessions_90d > 0 THEN 1 ELSE 0 END                                             AS web_active90d,
                 CASE WHEN app_sessions_90d > 0 THEN 1 ELSE 0 END                                             AS app_active90d,
                 CASE WHEN emails_90d > 0 THEN 1 ELSE 0 END                                                   AS email_active90d,
                 CASE
                     WHEN web_sessions_90d > 0 OR app_sessions_90d > 0 OR emails_90d > 0 THEN 1
                     ELSE 0 END                                                                               AS user_active90d,
                 CASE
                     WHEN web_sessions_90d = 0 AND app_sessions_90d = 0 AND emails_90d = 0 THEN 1
                     ELSE 0 END                                                                               AS inactive
          FROM se.data.user_activity)

SELECT b.date,
       territory,
       booker_segment,
       opt_in_status,
       sum(web_active1d),
       sum(app_active1d),
       sum(email_active1d),
       sum(user_active1d),
       sum(web_active7d),
       sum(app_active7d),
       sum(email_active7d),
       sum(user_active7d),
       sum(web_active14d),
       sum(app_active14d),
       sum(email_active14d),
       sum(user_active14d),
       sum(web_active30d),
       sum(app_active30d),
       sum(email_active30d),
       sum(user_active30d),
       sum(web_active90d),
       sum(app_active90d),
       sum(email_active90d),
       sum(user_active90d),
       sum(inactive)
FROM active_base b
         INNER JOIN se.data.user_segmentation c
                    ON b.shiro_user_id = c.shiro_user_id
                        AND b.date = c.date
         INNER JOIN
     (SELECT s.id   AS shiro_user_id,
             t.name AS territory
      FROM se.data.se_shiro_user s
               INNER JOIN se.data.se_affiliate a
                          ON s.original_affiliate_id = a.id
               INNER JOIN se.data.se_territory t ON a.territory_id = t.id) t
     ON b.shiro_user_id = t.shiro_user_id
GROUP BY 1, 2, 3, 4



SELECT ua.date AS date,
       'user_active' AS platform,
       t.name AS territory,
       COALESCE(sg.booker_segment, 'Prospect') AS booker_segment,
       COALESCE(sg.opt_in_status, 'opted out') AS opt_in_status,
       COUNT(CASE WHEN ua.web_sessions_1d > 0 OR ua.app_sessions_1d > 0 OR ua.emails_1d > 0 THEN 1 END) AS active_1d,
       COUNT(CASE WHEN ua.web_sessions_7d > 0 OR ua.app_sessions_7d > 0 OR ua.emails_7d > 0 THEN 1 END) AS active_7d,
       COUNT(CASE WHEN ua.web_sessions_14d > 0 OR ua.app_sessions_14d > 0 OR ua.emails_14d > 0 THEN 1 END) AS active_14d,
       COUNT(CASE WHEN ua.web_sessions_30d > 0 OR ua.app_sessions_30d > 0 OR ua.emails_30d > 0 THEN 1 END) AS active_30d,
       COUNT(CASE WHEN ua.web_sessions_90d > 0 OR ua.app_sessions_90d > 0 OR ua.emails_90d > 0 THEN 1 END) AS active_90d

FROM se.data.user_activity ua
    INNER JOIN se.data.se_shiro_user su ON ua.shiro_user_id = su.id
    INNER JOIN se.data.se_affiliate a ON su.original_affiliate_id = a.id
    INNER JOIN se.data.se_territory t ON a.territory_id = t.id
    LEFT JOIN se.data.user_segmentation sg ON ua.shiro_user_id = sg.shiro_user_id AND ua.date = sg.date
GROUP BY 1, 2, 3, 4, 5;