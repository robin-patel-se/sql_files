USE WAREHOUSE pipe_xlarge;

--found instances where email addresses live in the subscriber key, so separating out and joining back to shiro user to get user id


--sends
CREATE OR REPLACE TRANSIENT TABLE collab.muse_data_modelling.email_sends AS (
    SELECT s.event_date::DATE                                      AS event_date_dt,
           COALESCE(u.id::VARCHAR, s.subscriber_key::VARCHAR)::INT AS user_id,
           COUNT(DISTINCT send_id)                                 AS nb_unique_sends,
           COUNT(1)                                                AS nb_sends
    FROM raw_vault_mvp.sfmc.events_sends s
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                       ON s.subscriber_key::VARCHAR = u.username
    WHERE event_date >= DATEADD(WEEK, -4, current_date) --TODO repalce with loaded_at >= TIMESTAMPADD('{datetime_part}', -{periods}, '{schedule_tstamp}'::TIMESTAMP)
      AND (
            TRY_TO_NUMBER(s.subscriber_key) IS NOT NULL --sub key is a user id
            OR
            ( -- sub key is an email address and it matches a username in shiro user
                    TRY_TO_NUMBER(s.subscriber_key) IS NULL
                    AND
                    u.id IS NOT NULL
                )
        )
    GROUP BY 1, 2
)
;

SELECT *
FROM collab.muse_data_modelling.email_sends;


--opens
CREATE OR REPLACE TRANSIENT TABLE collab.muse_data_modelling.email_opens AS (
    SELECT o.event_date::DATE                                      AS event_date_dt,
           COALESCE(u.id::VARCHAR, o.subscriber_key::VARCHAR)::INT AS user_id,
           COUNT(DISTINCT send_id)                                 AS nb_unique_opens,
           COUNT(1)                                                AS nb_opens
    FROM raw_vault_mvp.sfmc.events_opens_plus_inferred o
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                       ON o.subscriber_key::VARCHAR = u.username
    WHERE event_date >= DATEADD(WEEK, -4, current_date) --TODO repalce with loaded_at >= TIMESTAMPADD('{datetime_part}', -{periods}, '{schedule_tstamp}'::TIMESTAMP)
      AND (
            TRY_TO_NUMBER(o.subscriber_key) IS NOT NULL --sub key is a user id
            OR
            ( -- sub key is an email address and it matches a username in shiro user
                    TRY_TO_NUMBER(o.subscriber_key) IS NULL
                    AND
                    u.id IS NOT NULL
                )
        )
    GROUP BY 1, 2
)
;

--clicks
CREATE OR REPLACE TRANSIENT TABLE collab.muse_data_modelling.email_clicks AS (
    SELECT c.event_date::DATE                                      AS event_date_dt,
           COALESCE(u.id::VARCHAR, c.subscriber_key::VARCHAR)::INT AS user_id,
           COUNT(DISTINCT send_id)                                 AS nb_unique_clicks,
           COUNT(1)                                                AS nb_clicks
    FROM raw_vault_mvp.sfmc.events_clicks c
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u ON c.subscriber_key = u.username
    WHERE event_date >= DATEADD(WEEK, -4, current_date) --TODO repalce with loaded_at >= TIMESTAMPADD('{datetime_part}', -{periods}, '{schedule_tstamp}'::TIMESTAMP)
      AND (
            TRY_TO_NUMBER(c.subscriber_key) IS NOT NULL --sub key is a user id
            OR
            ( -- sub key is an email address and it matches a username in shiro user
                    TRY_TO_NUMBER(c.subscriber_key) IS NULL
                    AND
                    u.id IS NOT NULL
                )
        )
    GROUP BY 1, 2
);


CREATE OR REPLACE TRANSIENT TABLE collab.muse_data_modelling.aggregate_user_emails AS (
    WITH grain AS (
        --found some instances were opens exist without clicks so creating
        --artificial grain so we can infer opens when clicks have occurred
        SELECT DISTINCT event_date_dt,
                        user_id
        FROM collab.muse_data_modelling.email_sends

        UNION

        SELECT DISTINCT event_date_dt,
                        user_id
        FROM collab.muse_data_modelling.email_opens

        UNION

        SELECT DISTINCT event_date_dt,
                        user_id
        FROM collab.muse_data_modelling.email_clicks
    )

    SELECT g.user_id,
           g.event_date_dt    AS date,
           s.nb_unique_sends  AS unique_sends,
           s.nb_sends         AS sends,
           o.nb_unique_opens  AS unique_opens,
           o.nb_opens         AS opens,
           c.nb_unique_clicks AS unique_clicks,
           c.nb_clicks        AS clicks

    FROM grain g
             LEFT JOIN collab.muse_data_modelling.email_sends s
                       ON g.event_date_dt = s.event_date_dt AND g.user_id = s.user_id
             LEFT JOIN collab.muse_data_modelling.email_opens o
                       ON g.event_date_dt = o.event_date_dt AND g.user_id = o.user_id
             LEFT JOIN collab.muse_data_modelling.email_clicks c
                       ON g.event_date_dt = c.event_date_dt AND g.user_id = c.user_id
);
--grain
WITH grain AS (
    --found some instances were opens exist without clicks so creating
    --artificial grain so we can infer opens when clicks have occurred
    SELECT DISTINCT event_date_dt,
                    user_id
    FROM collab.muse_data_modelling.email_sends

    UNION

    SELECT DISTINCT event_date_dt,
                    user_id
    FROM collab.muse_data_modelling.email_opens

    UNION

    SELECT DISTINCT event_date_dt,
                    user_id
    FROM collab.muse_data_modelling.email_clicks
)

SELECT g.user_id,
       g.event_date_dt    AS date,
       s.nb_unique_sends  AS unique_sends,
       s.nb_sends         AS sends,
       o.nb_unique_opens  AS unique_opens,
       o.nb_opens         AS opens,
       c.nb_unique_clicks AS unique_clicks,
       c.nb_clicks        AS clicks

FROM grain g
         LEFT JOIN collab.muse_data_modelling.email_sends s
                   ON g.event_date_dt = s.event_date_dt AND g.user_id = s.user_id
         LEFT JOIN collab.muse_data_modelling.email_opens o
                   ON g.event_date_dt = o.event_date_dt AND g.user_id = o.user_id
         LEFT JOIN collab.muse_data_modelling.email_clicks c
                   ON g.event_date_dt = c.event_date_dt AND g.user_id = c.user_id;

--full outer join
SELECT COALESCE(s.user_id, o.user_id, c.user_id)                   AS se_user_id,
       COALESCE(s.event_date_dt, o.event_date_dt, c.event_date_dt) AS date,
       s.nb_unique_sends                                           AS unique_sends,
       s.nb_sends                                                  AS sends,
       o.nb_unique_opens                                           AS unique_opens,
       o.nb_opens                                                  AS opens,
       c.nb_unique_clicks                                          AS unique_clicks,
       c.nb_clicks                                                 AS clicks

FROM collab.muse_data_modelling.email_sends s
         FULL JOIN collab.muse_data_modelling.email_opens o
                   ON s.event_date_dt = o.event_date_dt AND s.user_id = o.user_id
         FULL JOIN collab.muse_data_modelling.email_clicks c
                   ON COALESCE(s.event_date_dt, o.event_date_dt) = c.event_date_dt AND
                      COALESCE(s.user_id, o.user_id) = c.user_id; --execution: 2 s 182 ms, fetching: 97 ms


SELECT *
FROM collab.muse_data_modelling.aggregate_user_emails;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_sends CLONE raw_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_opens CLONE raw_vault_mvp.sfmc.events_opens;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_clicks CLONE raw_vault_mvp.sfmc.events_clicks;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;


self_describing_task --include 'dv/dwh_rec/email/sfmc_user_emails'  --method 'run' --start '2020-03-26 00:00:00' --end '2020-03-26 00:00:00'



------------------------------------------------------------------------------------------------------------------------
--for lina/cian
SELECT e.date,
       t.name                     AS territory,
       sum(coalesce(e.sends, 0))  AS sends,
       sum(coalesce(e.opens, 0))  AS opens,
       sum(coalesce(e.clicks, 0)) AS clicks

FROM se.data.user_emails e
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u ON e.user_id = u.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
WHERE e.date >= DATEADD(WEEK, -4, current_date)
GROUP BY 1, 2
ORDER BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------
DROP TABLE se_dev_robin.data.user_emails;

--find min loaded at
SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_sends; -- 2020-03-24 18:35:23.282174000
SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_opens; -- 2019-12-16 14:57:55.169085000
SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_opens_plus_inferred; -- 2020-03-24 18:35:15.424981000
SELECT MIN(loaded_at)
FROM raw_vault_mvp.sfmc.events_clicks;
--2019-12-16 14:44:06.836756000

DROP TABLE se_dev_robin.data.user_emails;

--to backfill run these first to ensure dev data is up to date.
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_sends CLONE raw_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_opens CLONE raw_vault_mvp.sfmc.events_opens;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE raw_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_clicks CLONE raw_vault_mvp.sfmc.events_clicks;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;

--then run locally _without_ dag dependencies to backfill all history
self_describing_task --include 'dv/dwh_rec/email/sfmc_user_emails'  --method 'run' --start '2019-12-15 00:00:00' --end '2019-12-15 00:00:00'

--on prod run this to copy local copy over to production db:
CREATE OR REPLACE TABLE se.data.user_emails CLONE se_dev_robin.data.user_emails;


USE WAREHOUSE pipe_large;

SELECT COUNT(*)
FROM se_dev_robin.data.user_emails;
--inferred_opens
SELECT event_date::DATE,
       COUNT(*)
FROM raw_vault_mvp.sfmc.events_opens_plus_inferred
WHERE event_date >= '2020-02-20'
GROUP BY 1
ORDER BY 1;
--opens
SELECT event_date::DATE,
       COUNT(*)
FROM raw_vault_mvp.sfmc.events_opens
WHERE event_date >= '2020-02-20'
GROUP BY 1
ORDER BY 1;


SELECT e.date,
       sum(coalesce(e.sends, 0))  AS sends,
       sum(coalesce(e.opens, 0))  AS opens,
       sum(coalesce(e.clicks, 0)) AS clicks

FROM se.data.user_emails e
WHERE e.date >= '2020-04-20'
GROUP BY 1
ORDER BY 1;

airflow backfill --start_date '2020-03-31 00:00:00' --end_date '2020-03-31 00:00:00' --task_regex '.*' dwh__user_emails__daily
airflow backfill --start_date '2020-04-01 00:00:00' --end_date '2020-04-01 00:00:00' --task_regex '.*' dwh__user_emails__daily


SELECT * FROM se.data.user_emails;


SELECT * FROM raw_vault_mvp.sfmc.jobs_list;
SELECT * FROM raw_vault_mvp.sfmc.se