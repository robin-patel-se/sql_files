USE WAREHOUSE pipe_xlarge;
--create fake full calandar table
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg__20200323t000000__every7days.static_member_calendar AS (
    WITH input_members AS (
        SELECT u.id                 AS user_id,
               u.date_created::DATE AS member_created
        FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                 INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
                 INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
        WHERE t.name IN ('UK', 'DE')
    ),
         calendar AS (
             SELECT tstamp::DATE AS date
             FROM raw_vault_mvp.calendar.hours
             GROUP BY 1
         )
    SELECT c.date AS calendar_date,
           m.user_id
    FROM calendar c
             LEFT JOIN input_members m
                       ON m.member_created <= c.date
    WHERE c.date <= CURRENT_DATE
);

--create fake daily calandar table
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.customer_model_last7days_uk_de_stg__20200321t000000__daily.static_member_calendar AS (
    WITH input_members AS (
        SELECT u.id                 AS user_id,
               u.date_created::DATE AS member_created
        FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                 INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
                 INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
        WHERE t.name IN ('UK', 'DE')
    ),
         calendar AS (
             SELECT tstamp::DATE AS date
             FROM raw_vault_mvp.calendar.hours
             GROUP BY 1
         )
    SELECT c.date AS calendar_date,
           m.user_id
    FROM calendar c
             LEFT JOIN input_members m
                       ON m.member_created <= c.date
    WHERE c.date >= DATEADD(DAY, -7, CURRENT_DATE())
      AND m.member_created >= DATEADD(DAY, -7, CURRENT_DATE())
);


SELECT *
FROM collab.muse_data_modelling.static_member_calendar
WHERE user_id = 19510035
ORDER BY date;


SELECT *
FROM se_dev_robin.data.user_emails
WHERE opens IS NOT NULL;


SELECT c.user_id,
       c.calendar_date,
       e.opens         AS email_opens_count,
       e.unique_opens  AS email_unique_opens_count,
       e.clicks        AS email_clicks_count,
       e.unique_clicks AS email_unique_clicks_count
FROM collab.muse_data_modelling.static_member_calendar c
         LEFT JOIN se_dev_robin.data.user_emails e ON c.date = e.date AND c.user_id = e.user_id
WHERE c.user_id = 64683013;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg__20200323t000000__every7days.static_member_calendar CLONE collab.muse_data_modelling.static_member_calendar;

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.customer_model_full_uk_de_stg__20200323t000000__every7days;

self_describing_task --include 'dv/customer_model_full_uk_de/040_stream_email'  --method 'run' --start '2020-03-26 00:00:00' --end '2020-03-26 00:00:00'

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM raw_vault_mvp.sfmc.events_opens;

DROP TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_email;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar CLONE data_vault_mvp.customer_model_full_uk_de_stg.static_member_calendar;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_last7days_uk_de_stg.static_member_calendar CLONE data_vault_mvp.customer_model_last7days_uk_de_stg.static_member_calendar;
CREATE OR REPLACE TABLE se_dev_robin.data.user_emails CLONE se.data.user_emails;

self_describing_task --include 'dv/customer_model_full_uk_de/040_stream_email'  --method 'run' --start '2020-03-26 00:00:00' --end '2020-03-26 00:00:00'

--after email backfill is complete run stream email with dependencies commented out.

self_describing_task --include 'dv/customer_model_full_uk_de/040_stream_email'  --method 'run' --start '2010-01-01 00:00:00' --end '2010-01-01 00:00:00'
self_describing_task --include 'dv/customer_model_last7days_uk_de/040_stream_email'  --method 'run' --start '2020-04-01 00:00:00' --end '2020-04-01 00:00:00'

--run on prod
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_full_uk_de_stg.stream_email CLONE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_email;

self_describing_task --include 'dv/customer_model_last7days_uk_de/040_stream_email'  --method 'run' --start '2020-03-21 00:00:00' --end '2020-03-21 00:00:00'
--run on prod
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_last7days_uk_de_stg.stream_email CLONE data_vault_mvp_dev_robin.customer_model_last7days_uk_de_stg.stream_email;

SELECT * FROM data_vault_mvp_dev_robin.customer_model_last7days_uk_de_stg.stream_email;
