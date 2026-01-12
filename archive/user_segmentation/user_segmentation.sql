USE WAREHOUSE pipe_xlarge;

SET date_var = DATEADD(DAY, -1, current_date);

SELECT $date_var                            AS date,
       u.id                                 AS shiro_user_id,
       count(DISTINCT
             CASE
                 WHEN b.booking_completed_date >= DATEADD(MONTH, -13, $date_var)
                     THEN b.booking_id END) AS bookings_less_13m,
       count(DISTINCT
             CASE
                 WHEN b.booking_completed_date < DATEADD(MONTH, -13, $date_var)
                     THEN b.booking_id END) AS bookings_more_13m,
       CASE
           WHEN bookings_less_13m = 1 THEN 'Single'
           WHEN bookings_less_13m > 1 THEN 'Repeat'
           WHEN bookings_more_13m = 1 THEN 'Lapsed Single'
           WHEN bookings_more_13m > 1 THEN 'Lapsed Repeat'
           ELSE 'Prospect'
           END                              AS booker_segment
FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
         LEFT JOIN data_vault_mvp.dwh.se_booking b ON u.id = b.shiro_user_id
    AND b.booking_status = 'COMPLETE'
    AND b.booking_completed_date <= $date_var
WHERE u.date_created <= $date_var
GROUP BY 1, 2;

CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation
(
    schedule_tstamp   TIMESTAMP,
    run_tstamp        TIMESTAMP,
    operation_id      VARCHAR,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,

    date              DATE,
    shiro_user_id     INT,
    bookings_less_13m INT,
    bookings_more_13m INT,
    booker_segment    VARCHAR
)
    CLUSTER BY (date);

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE scratch.robinpatel.backfill_user_segmentation_loop(p_first_run DOUBLE, p_max_runs DOUBLE)
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
        `INSERT INTO se_dev_robin.data.user_segmentation
            SELECT

                    '1970-01-01 00:00:00.000'       AS schedule_tstamp,
                    '2020-04-08 00:00:00.000'       AS run_tstamp,
                    'initial backfill'              AS operation_id,
                    current_date                    AS created_at,
                    current_date                    AS updated_at,

                    ${date_var} AS date,
                    u.id AS shiro_user_id,
                    count(DISTINCT
                         CASE
                             WHEN b.booking_completed_date >= DATEADD(MONTH, -13, ${date_var})
                                 THEN b.booking_id END) AS bookings_less_13m,
                    count(DISTINCT
                         CASE
                             WHEN b.booking_completed_date < DATEADD(MONTH, -13, ${date_var})
                                 THEN b.booking_id END) AS bookings_more_13m,
                    CASE
                       WHEN bookings_less_13m = 1 THEN 'Single'
                       WHEN bookings_less_13m > 1 THEN 'Repeat'
                       WHEN bookings_more_13m = 1 THEN 'Lapsed Single'
                       WHEN bookings_more_13m > 1 THEN 'Lapsed Repeat'
                       ELSE 'Prospect'
                       END                              AS booker_segment
            FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                     LEFT JOIN data_vault_mvp.dwh_rec.se_booking b ON u.id = b.shiro_user_id
                     AND b.booking_status = 'COMPLETE'
                     AND b.booking_completed_date <= ${date_var}
            WHERE u.date_created <= ${date_var}
            GROUP BY 1, 2, 3, 4, 5, 6, 7;
        `;

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
};
$$;

TRUNCATE se_dev_robin.data.user_segmentation;

CALL scratch.robinpatel.backfill_user_segmentation_loop(98, 464);

SELECT *
FROM se_dev_robin.data.user_segmentation
WHERE date >= '2020-04-05';

SELECT date,
       booker_segment,
       count(*) AS users
FROM se_dev_robin.data.user_segmentation
WHERE date = '2020-04-06'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date,
       booker_segment,
       count(*) AS users
FROM se.data.user_segmentation
WHERE date = '2020-04-06'
GROUP BY 1, 2
ORDER BY 1, 2;


UPDATE se_dev_robin.data.user_segmentation target
SET target.booker_segment = 'Lapsed Single'
WHERE target.booker_segment = 'Lapsed';

UPDATE se_dev_robin.data.user_segmentation target
SET target.booker_segment = 'Lapsed Repeat'
WHERE target.booker_segment = 'Lapsed High Value';



SELECT date,
       SUM(CASE WHEN booker_segment = 'Single' THEN 1 END)        AS single_users,
       SUM(CASE WHEN booker_segment = 'Repeat' THEN 1 END)        AS repeat_users,
       SUM(CASE WHEN booker_segment = 'Lapsed Single' THEN 1 END) AS lapsed_single_users,
       SUM(CASE WHEN booker_segment = 'Lapsed Repeat' THEN 1 END) AS lapsed_repeat_users,
       SUM(CASE WHEN booker_segment = 'Prospect' THEN 1 END)      AS prospect_users
FROM se_dev_robin.data.user_segmentation
GROUP BY 1
ORDER BY 1;


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

self_describing_task --include 'dv/user_segmentation/user_segmentation'  --method 'run' --start '2020-04-06 00:00:00' --end '2020-04-06 00:00:00'


SELECT user_id,
       calendar_date,
       subscription_type,
       CASE WHEN subscription_type = 0 THEN 'opted out' ELSE 'opted in' END AS opt_in_status
FROM se.data.user_subscription
WHERE calendar_date >= '2019-01-01'
;

CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation CLONE se.data.user_segmentation;
CREATE OR REPLACE TABLE se_dev_robin.data.user_subscription CLONE se.data.user_subscription;

ALTER TABLE se_dev_robin.data.user_segmentation
    ADD COLUMN subscription_type INT;
ALTER TABLE se_dev_robin.data.user_segmentation
    ADD COLUMN opt_in_status VARCHAR;

------------------------------------------------------------------------------------------------------------------------
--to backfill the user segmentation table with subscription status

UPDATE se_dev_robin.data.user_segmentation target
SET target.subscription_type = batch.subscription_type,
    target.opt_in_status     = batch.opt_in_status
FROM (
         SELECT user_id,
                calendar_date AS date,
                subscription_type,
                CASE
                    WHEN subscription_type = 0 THEN 'opted out'
                    WHEN subscription_type IN (1, 2) THEN 'opted in'
                    END       AS opt_in_status
         FROM se.data.user_subscription
         WHERE calendar_date >= '2019-01-01'
     ) AS batch
WHERE target.date = batch.date
  AND target.shiro_user_id = batch.user_id;

SELECT *
FROM se_dev_robin.data.user_segmentation
WHERE date >= '2020-04-13';

self_describing_task --include 'se/data/se_user_segmentation'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation CLONE se.data.user_segmentation;
CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation_bkup CLONE se.data.user_segmentation;
USE WAREHOUSE pipe_xlarge;

--run on prod
CREATE OR REPLACE TABLE se.data.user_segmentation CLONE se_dev_robin.data.user_segmentation;

SELECT *
FROM se.data.user_segmentation
WHERE date = '2020-03-01';

airflow backfill --start_date '2020-04-20 03:00:00' --end_date '2020-04-20 03:00:00' --task_regex '.*' dwh__user_segmentation__daily_at_03h00
airflow backfill --start_date '2020-04-20 03:00:00' --end_date '2020-04-20 03:00:00' --task_regex '.*' active_user_base__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------
--new columns

CREATE OR REPLACE PROCEDURE scratch.robinpatel.backfill_user_segmentation_loop(p_first_run DOUBLE, p_max_runs DOUBLE)
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
        `INSERT INTO se_dev_robin.data.user_segmentation
            SELECT

                    '1970-01-01 00:00:00.000'       AS schedule_tstamp,
                    '2020-04-08 00:00:00.000'       AS run_tstamp,
                    'initial backfill'              AS operation_id,
                    current_date                    AS created_at,
                    current_date                    AS updated_at,

                    ${date_var} AS date,
                    u.id AS shiro_user_id,
                    count(DISTINCT
                         CASE
                             WHEN b.booking_completed_date >= DATEADD(MONTH, -13, ${date_var})
                                 THEN b.booking_id END) AS bookings_less_13m,
                    count(DISTINCT
                         CASE
                             WHEN b.booking_completed_date < DATEADD(MONTH, -13, ${date_var})
                                 THEN b.booking_id END) AS bookings_more_13m,
                    CASE
                       WHEN bookings_less_13m = 1 THEN 'Single'
                       WHEN bookings_less_13m > 1 THEN 'Repeat'
                       WHEN bookings_more_13m = 1 THEN 'Lapsed Single'
                       WHEN bookings_more_13m > 1 THEN 'Lapsed Repeat'
                       ELSE 'Prospect'
                       END                              AS booker_segment
            FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
                     LEFT JOIN data_vault_mvp.dwh_rec.se_booking b ON u.id = b.shiro_user_id
                     AND b.booking_status = 'COMPLETE'
                     AND b.booking_completed_date <= ${date_var}
            WHERE u.date_created <= ${date_var}
            GROUP BY 1, 2, 3, 4, 5, 6, 7;
        `;

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
};
$$;

SET date_var = current_date;

SELECT '1970-01-01 00:00:00.000'            AS schedule_tstamp,
       '2020-04-08 00:00:00.000'            AS run_tstamp,
       'initial backfill'                   AS operation_id,
       current_date                         AS created_at,
       current_date                         AS updated_at,

       $date_var                            AS date,
       u.id                                 AS shiro_user_id,
       count(DISTINCT
             CASE
                 WHEN b.booking_completed_date >= DATEADD(MONTH, -13, $date_var)
                     THEN b.booking_id END) AS bookings_less_13m,
       count(DISTINCT
             CASE
                 WHEN b.booking_completed_date < DATEADD(MONTH, -13, $date_var)
                     THEN b.booking_id END) AS bookings_more_13m,
       CASE
           WHEN bookings_less_13m = 1 THEN 'Single'
           WHEN bookings_less_13m > 1 THEN 'Repeat'
           WHEN bookings_more_13m = 1 THEN 'Lapsed Single'
           WHEN bookings_more_13m > 1 THEN 'Lapsed Repeat'
           ELSE 'Prospect'
           END                              AS booker_segment
FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
         LEFT JOIN se.data.fact_complete_booking fcb b
                   ON u.id = b.shiro_user_id
                       AND b.booking_completed_date <= $date_var
WHERE u.date_created <= ${date_var}
GROUP BY 1, 2, 3, 4, 5, 6, 7;

USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------
--query to populate history
USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation AS (
    WITH dates AS (
        SELECT sc.date_value AS date
        FROM se.data.se_calendar sc
        WHERE sc.date_value >= '2019-01-01'
          AND sc.date_value < CURRENT_DATE
    )
       , grain AS (
        SELECT sus.id AS shiro_user_id,
               d.date
        FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot sus
                 LEFT JOIN dates d ON sus.date_created <= d.date
    )
    SELECT current_time                                                       AS schedule_tstamp,
           current_time                                                       AS run_tstamp,
           'initial backfill'                                                 AS operation_id,
           '1970-01-01 00:00:00'                                              AS created_at,
           current_time                                                       AS updated_at,

           g.date,
           g.shiro_user_id,

           COUNT(CASE
                     WHEN fcb.booking_completed_date >= DATEADD(MONTH, -13, g.date)
                         THEN fcb.booking_id
               END)                                                           AS bookings_less_13m,
           COUNT(CASE
                     WHEN fcb.booking_completed_date < DATEADD(MONTH, -13, g.date)
                         THEN fcb.booking_id
               END)                                                           AS bookings_more_13m,
           CASE
               WHEN bookings_less_13m = 1 THEN 'Single'
               WHEN bookings_less_13m > 1 THEN 'Repeat'
               WHEN bookings_more_13m = 1 THEN 'Lapsed Single'
               WHEN bookings_more_13m > 1 THEN 'Lapsed Repeat'
               ELSE 'Prospect'
               END                                                            AS booker_segment,

           SUM(fcb.gross_booking_value_gbp)                                   AS total_booking_value,
           MAX(fcb.gross_booking_value_gbp)                                   AS max_booking_value,
           AVG(fcb.gross_booking_value_gbp)                                   AS avg_booking_value,

           COUNT(fcb.booking_id)                                              AS total_bookings,
           SUM(CASE WHEN fcb.infant_guests + fcb.child_guests > 1 THEN 1 END) AS total_family_bookings,

           SUM(fcb.margin_gross_of_toms_gbp)                                  AS total_margin,
           AVG(fcb.no_nights)                                                 AS avg_no_nights,

           MAX(fcb.adult_guests + fcb.child_guests + fcb.infant_guests)       AS max_travellers,
           AVG(fcb.adult_guests + fcb.child_guests + fcb.infant_guests)       AS avg_travellers,

           MAX(fcb.price_per_night)                                           AS max_price_per_night,
           AVG(fcb.price_per_night)                                           AS avg_price_per_night,
           MAX(fcb.price_per_person_per_night)                                AS max_price_per_person_per_night,
           AVG(fcb.price_per_person_per_night)                                AS avg_price_per_person_per_night,

           us.subscription_type,
           CASE
               WHEN us.subscription_type = 0 THEN 'opted out'
               WHEN us.subscription_type IN (1, 2) THEN 'opted in'
               END                                                            AS opt_in_status
    FROM grain g
             LEFT JOIN se_dev_robin.data.fact_complete_booking fcb
                       ON g.shiro_user_id = fcb.shiro_user_id AND g.date >= fcb.booking_completed_date
             LEFT JOIN se.data.user_subscription us ON g.shiro_user_id = us.user_id AND g.date = us.calendar_date
--     WHERE fcb.shiro_user_id = 62972247
    GROUP BY g.date, g.shiro_user_id, us.subscription_type, opt_in_status
);

SELECT
       us.date,
       us.shiro_user_id,
       us.bookings_less_13m,
       us.bookings_more_13m,
       us.booker_segment,
       us.total_booking_value,
       us.max_booking_value,
       us.avg_booking_value,
       us.total_bookings,
       us.total_family_bookings,
       us.total_margin,
       us.avg_no_nights,
       us.max_travellers,
       us.avg_travellers,
       us.max_price_per_night,
       us.avg_price_per_night,
       us.max_price_per_person_per_night,
       us.avg_price_per_person_per_night,
       us.subscription_type,
       us.opt_in_status
FROM se.data.user_segmentation us
WHERE us.shiro_user_id = 62972247
ORDER BY date;

DROP TABLE data_vault_mvp_dev_robin.travelbird_cms.user_segmentation_v1;

SELECT * FROM data_vault_mvp.dwh.user_attributes ua WHERE ua.shiro_user_id = 62972247;


INSERT INTO se_dev_robin.data.user_segmentation
SELECT '2020-06-03 00:00:00',
       '2020-06-03 00:00:00',
       operation_id,
       '1970-01-01 00:00:00',
       '2020-06-03 00:00:00',
       date,
       shiro_user_id,
       bookings_less_13m,
       bookings_more_13m,
       booker_segment,
       total_booking_value,
       max_booking_value,
       avg_booking_value,
       total_bookings,
       total_family_bookings,
       total_margin,
       avg_no_nights,
       max_travellers,
       avg_travellers,
       max_price_per_night,
       avg_price_per_night,
       max_price_per_person_per_night,
       avg_price_per_person_per_night,
       subscription_type,
       opt_in_status
FROM se_dev_robin.data.user_segmentation_v1


CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation_bkup CLONE se.data.user_segmentation;
CREATE OR REPLACE TABLE se.data.user_segmentation COPY GRANTS CLONE se_dev_robin.data.user_segmentation;

SELECT us.date,
       us.shiro_user_id,
       us.bookings_less_13m,
       us.bookings_more_13m,
       us.booker_segment,
       us.total_booking_value,
       us.max_booking_value,
       us.avg_booking_value,
       us.total_bookings,
       us.total_family_bookings,
       us.total_margin,
       us.avg_no_nights,
       us.max_travellers,
       us.avg_travellers,
       us.max_price_per_night,
       us.avg_price_per_night,
       us.max_price_per_person_per_night,
       us.avg_price_per_person_per_night,
       us.subscription_type,
       us.opt_in_status
FROM se.data.user_segmentation us
WHERE us.date = (
    SELECT MAX(date)
    FROM se.data.user_segmentation u
)

SELECT MIN(date) FROM data_vault_mvp.dwh.user_activity ua