-- https://docs.google.com/spreadsheets/d/18Wmo15Zin35pYZ-kcr9BqZT0Ezkvj0psKLnkmqFPV3o/edit#gid=873261199

SELECT us.schedule_tstamp,
       us.run_tstamp,
       us.operation_id,
       us.created_at,
       us.updated_at,
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
WHERE us.shiro_user_id = 62972247;


SELECT *
FROM se.data.user_subscription us;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE se_dev_robin.data.fact_complete_booking CLONE se.data.fact_complete_booking;
CREATE OR REPLACE TABLE se_dev_robin.data.user_subscription CLONE se.data.user_subscription;


SELECT GET_DDL('table', 'se_dev_robin.data.fact_complete_booking');


CREATE OR REPLACE VIEW fact_complete_booking
    COPY GRANTS
AS
(
SELECT fb.booking_id,
       fb.booking_status,
       fb.se_sale_id,
       fb.se_sale_id AS sale_id, --dupe column, to ensure existing uses of sale_id don't fail
       fb.shiro_user_id,

       fb.check_in_date,
       fb.check_out_date,
       fb.booking_lead_time_days,
       fb.booking_created_date,
       fb.booking_completed_date,
       fb.booking_transaction_completed_date,

       fb.customer_total_price_gbp,
       fb.customer_total_price_gbp_constant_currency,
       fb.gross_booking_value_gbp,
       fb.commission_ex_vat_gbp,
       fb.booking_fee_net_rate_gbp,
       fb.payment_surcharge_net_rate_gbp,
       fb.insurance_commission_gbp,

       fb.margin_gross_of_toms_gbp,
       fb.margin_gross_of_toms_gbp_constant_currency,
       fb.margin_gross_of_toms_eur_constant_currency,
       fb.no_nights,
       fb.adult_guests,
       fb.child_guests,
       fb.infant_guests,
       fb.price_per_night,
       fb.price_per_person_per_night,
       fb.rooms,
       fb.tech_platform

FROM se.data.fact_booking fb
WHERE fb.booking_status_type = 'live' -- only bring in bookings that are complete
    );


self_describing_task --include 'dv/dwh/user_attributes/user_segmentation.py'  --method 'run' --start '2020-11-29 00:00:00' --end '2020-11-29 00:00:00'

SELECT TO_DATE('2020-11-28 03:00:00')                                         AS date,
       u.id                                                                   AS shiro_user_id,
       COUNT(CASE
                 WHEN b.booking_completed_date >= DATEADD(MONTH, -13, TO_DATE('2020-11-28 03:00:00'))
                     THEN b.booking_id
           END)                                                               AS bookings_less_13m,
       COUNT(CASE
                 WHEN b.booking_completed_date < DATEADD(MONTH, -13, TO_DATE('2020-11-28 03:00:00'))
                     THEN b.booking_id
           END)                                                               AS bookings_more_13m,
       CASE
           WHEN bookings_less_13m = 1 THEN 'Single'
           WHEN bookings_less_13m > 1 THEN 'Repeat'
           WHEN bookings_more_13m = 1 THEN 'Lapsed Single'
           WHEN bookings_more_13m > 1 THEN 'Lapsed Repeat'
           ELSE 'Prospect'
           END                                                                AS booker_segment,

       SUM(b.gross_booking_value_gbp)                                         AS total_booking_value,
       MAX(b.gross_booking_value_gbp)                                         AS max_booking_value,
       AVG(b.gross_booking_value_gbp)                                         AS avg_booking_value,

       COUNT(b.booking_id)                                                    AS total_bookings,
       SUM(CASE WHEN b.infant_guests + b.child_guests > 1 THEN 1 END)         AS total_family_bookings,

       SUM(b.margin_gross_of_toms_gbp)                                        AS total_margin,
       AVG(b.no_nights)                                                       AS avg_no_nights,

       MAX(b.adult_guests + b.child_guests + b.infant_guests)                 AS max_travellers,
       AVG(b.adult_guests + b.child_guests + b.infant_guests)                 AS avg_travellers,

       MAX(b.price_per_night)                                                 AS max_price_per_night,
       AVG(b.price_per_night)                                                 AS avg_price_per_night,

       MAX(b.price_per_person_per_night)                                      AS max_price_per_person_per_night,
       AVG(b.price_per_person_per_night)                                      AS avg_price_per_person_per_night,

       s.subscription_type,
       CASE
           WHEN subscription_type = 0 THEN 'opted out'
           WHEN subscription_type IN (1, 2) THEN 'opted in'
           END                                                                AS opt_in_status,

       CASE
           WHEN total_bookings > 5 THEN '5+'
           WHEN total_bookings > 2 THEN '3-4'
           WHEN total_bookings = 2 THEN '2'
           WHEN total_bookings = 1 THEN '1'
           ELSE '0'
           END                                                                AS total_bookings_bucket,
       CASE
           WHEN total_margin > 1000 THEN '£1000+'
           WHEN total_margin > 500 THEN '£501-£1000'
           WHEN total_margin > 100 THEN '£101-£500'
           ELSE '<£100'
           END                                                                AS total_margin_bucket,
       SUM(IFF(ds.travel_type = 'domestic', 1, 0))                            AS domestic_bookings,
       SUM(IFF(ds.travel_type = 'international', 1, 0))                       AS international_bookings,
       CASE
           WHEN domestic_bookings / total_bookings = 1 THEN '100% Domestic'
           WHEN domestic_bookings / total_bookings >= 0.6 THEN '>=60% Domestic'
           WHEN international_bookings / total_bookings = 1 THEN '100% International'
           WHEN international_bookings / total_bookings >= 0.6 THEN '>=60% International'
           ELSE 'Mixed'
           END                                                                AS travel_type_segment,
       se.data.member_recency_status(u.date_created, CURRENT_DATE::TIMESTAMP) AS member_recency_status


FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot u
    LEFT JOIN se_dev_robin.data.fact_complete_booking b
              ON u.id = b.shiro_user_id
                  AND b.booking_completed_date <= TO_DATE('2020-11-28 03:00:00')
    LEFT JOIN se.data.dim_sale ds ON b.se_sale_id = ds.se_sale_id
    LEFT JOIN se_dev_robin.data.user_subscription s
              ON u.id = s.user_id
                  AND s.calendar_date = TO_DATE('2020-11-28 03:00:00')
WHERE u.date_created <= TO_DATE('2020-11-28 03:00:00')
GROUP BY TO_DATE('2020-11-28 03:00:00'),
         u.id,
         s.subscription_type,
         member_recency_status

DROP TABLE data_vault_mvp_dev_robin.dwh.user_segmentation;

------------------------------------------------------------------------------------------------------------------------
--update historical

DROP TABLE data_vault_mvp_dev_robin.dwh.user_segmentation;

CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.dwh.user_segmentation
(

    -- (lineage) metadata for the current job
    schedule_tstamp                TIMESTAMP,
    run_tstamp                     TIMESTAMP,
    operation_id                   VARCHAR,
    created_at                     TIMESTAMP,
    updated_at                     TIMESTAMP,

    date                           DATE,
    shiro_user_id                  INT,
    bookings_less_13m              INT,
    bookings_more_13m              INT,
    booker_segment                 VARCHAR,

    total_booking_value            FLOAT,
    max_booking_value              FLOAT,
    avg_booking_value              FLOAT,

    total_bookings                 INT,
    total_family_bookings          INT,

    total_margin                   FLOAT,

    avg_no_nights                  FLOAT,

    max_travellers                 INT,
    avg_travellers                 INT,

    max_price_per_night            FLOAT,
    avg_price_per_night            FLOAT,
    max_price_per_person_per_night FLOAT,
    avg_price_per_person_per_night FLOAT,

    subscription_type              INT,
    opt_in_status                  VARCHAR,

    total_bookings_bucket          VARCHAR,
    total_margin_bucket            VARCHAR,

    domestic_bookings              INT,
    international_bookings         INT,
    travel_type_segment            VARCHAR,
    member_recency_status          VARCHAR
)
    CLUSTER BY (date);

CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation_example AS
SELECT *
FROM se.data.user_segmentation us
WHERE us.shiro_user_id IN (62972247, 9523266, 18171132);


------------------------------------------------------------------------------------------------------------------------

--model bookings at daily grain
WITH daily_grain AS (
    SELECT fb.booking_completed_date                                                      AS booking_date,
           fb.cancellation_date                                                           AS canx_date,
           fb.shiro_user_id,
           COUNT(*)                                                                       AS total_gross_bookings,
           SUM(IFF(fb.booking_status_type = 'cancelled', 1, 0))                           AS total_cancelled_bookings,
           SUM(IFF(fb.infant_guests + fb.child_guests > 1, 1, 0))                         AS total_gross_family_bookings,
           SUM(
                   IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic',
                       1, 0))                                                             AS total_gross_domestic_bookings,
           SUM(
                   IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International',
                       1, 0))                                                             AS total_gross_international_bookings,

           SUM(fb.customer_total_price_gbp)                                               AS gross_booking_total_price,
           SUM(IFF(fb.booking_status_type = 'cancelled', fb.customer_total_price_gbp, 0)) AS cancelled_booking_total_price,
           MAX(fb.customer_total_price_gbp)                                               AS max_gross_booking_total_price,
           AVG(fb.customer_total_price_gbp)                                               AS avg_gross_booking_total_price,

           SUM(fb.margin_gross_of_toms_gbp)                                               AS gross_margin,
           SUM(IFF(fb.booking_status_type = 'cancelled', fb.margin_gross_of_toms_gbp, 0)) AS canx_margin,

           AVG(fb.no_nights)                                                              AS avg_gross_no_nights,
           MAX(fb.price_per_night)                                                        AS max_gross_price_per_night,
           AVG(fb.price_per_night)                                                        AS avg_gross_price_per_night,
           MAX(fb.price_per_person_per_night)                                             AS max_gross_price_per_person_per_night,
           AVG(fb.price_per_person_per_night)                                             AS avg_gross_price_per_person_per_night


    FROM se.data.fact_booking fb
        LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
    WHERE fb.booking_status_type IN ('live', 'cancelled')
    GROUP BY 1, 2, 3
);

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.user_segmentation AS (
    WITH gross AS (
        SELECT fb.shiro_user_id,
               COUNT(*)                                               AS total_gross_bookings,
               SUM(IFF(fb.infant_guests + fb.child_guests > 1, 1, 0)) AS total_gross_family_bookings,
               SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic',
                       1, 0))                                         AS total_gross_domestic_bookings,
               SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International',
                       1, 0))                                         AS total_gross_international_bookings,
               SUM(IFF(fb.booking_completed_date >= DATEADD(MONTH, -13, CURRENT_DATE),
                       1, 0))                                         AS total_gross_bookings_less_13m,-- replace with TO_DATE('{schedule.tstamp}'
               SUM(IFF(fb.booking_completed_date < DATEADD(MONTH, -13, CURRENT_DATE),
                       1,
                       0))                                            AS total_gross_bookings_more_13m, -- replace with TO_DATE('{schedule.tstamp}'

               SUM(fb.gross_revenue_gbp)                              AS gross_revenue, --needs to be updated to gross revenue
               MAX(fb.gross_revenue_gbp)                              AS max_gross_revenue, --needs to be updated to gross revenue
               AVG(fb.gross_revenue_gbp)                              AS avg_gross_revenue, --needs to be updated to gross revenue

               SUM(fb.margin_gross_of_toms_gbp)                       AS gross_margin,
               AVG(fb.no_nights)                                      AS avg_gross_no_nights,
               MAX(fb.price_per_night)                                AS max_gross_price_per_night,
               AVG(fb.price_per_night)                                AS avg_gross_price_per_night,
               MAX(fb.price_per_person_per_night)                     AS max_gross_price_per_person_per_night,
               AVG(fb.price_per_person_per_night)                     AS avg_gross_price_per_person_per_night
        FROM se.data.fact_booking fb
            LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
        WHERE fb.booking_status_type IN ('live', 'cancelled')
          AND fb.booking_completed_date <= CURRENT_DATE --change to TO_DATE('{schedule.tstamp}')
        GROUP BY 1
    ),
         cancelled AS (
             SELECT fb.shiro_user_id,
                    COUNT(*)                                                                      AS total_cancelled_bookings,
                    SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic',
                            1,
                            0))                                                                   AS total_cancelled_domestic_bookings,
                    SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International',
                            1,
                            0))                                                                   AS total_cancelled_international_bookings,
                    SUM(
                            IFF(fb.cancellation_date >= DATEADD(MONTH, -13, CURRENT_DATE), 1, 0)) AS total_cancelled_bookings_less_13m, -- replace with TO_DATE('{schedule.tstamp}'
                    SUM(
                            IFF(fb.cancellation_date < DATEADD(MONTH, -13, CURRENT_DATE), 1, 0))  AS total_cancelled_bookings_more_13m, -- replace with TO_DATE('{schedule.tstamp}'
                    SUM(fb.customer_total_price_gbp)                                              AS cancelled_booking_total_price,     --needs to be updated to gross revenue
                    SUM(fb.margin_gross_of_toms_gbp)                                              AS canx_margin

             FROM se.data.fact_booking fb
                 LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
             WHERE fb.booking_status_type = 'cancelled'
               AND fb.cancellation_date <= CURRENT_DATE --change to TO_DATE('{schedule.tstamp}')
             GROUP BY 1
         ),
         combine_users_bookings AS (
             SELECT CURRENT_DATE                                                       AS date,
                    ua.shiro_user_id,
                    ua.signup_tstamp::DATE                                             AS signup_date,
                    se.data.member_recency_status(ua.signup_tstamp, CURRENT_TIMESTAMP) AS member_recency_status,
                    COALESCE(g.total_gross_bookings, 0)                                AS total_gross_bookings,
                    COALESCE(c.total_cancelled_bookings, 0)                            AS total_cancelled_bookings,
                    COALESCE(g.total_gross_family_bookings, 0)                         AS total_gross_family_bookings,
                    COALESCE(g.total_gross_domestic_bookings, 0)                       AS total_gross_domestic_bookings,
                    COALESCE(g.total_gross_international_bookings, 0)                  AS total_gross_international_bookings,
                    COALESCE(c.total_cancelled_domestic_bookings, 0)                   AS total_cancelled_domestic_bookings,
                    COALESCE(c.total_cancelled_international_bookings, 0)              AS total_cancelled_international_bookings,

                    COALESCE(g.total_gross_bookings_less_13m, 0)                       AS total_gross_bookings_less_13m,
                    COALESCE(c.total_cancelled_bookings_less_13m, 0)                   AS total_cancelled_bookings_less_13m,
                    COALESCE(g.total_gross_bookings_more_13m, 0)                       AS total_gross_bookings_more_13m,
                    COALESCE(c.total_cancelled_bookings_more_13m, 0)                   AS total_cancelled_bookings_more_13m,

                    COALESCE(g.gross_booking_total_price, 0)                           AS gross_booking_total_price,
                    COALESCE(c.cancelled_booking_total_price, 0)                       AS cancelled_booking_total_price,
                    COALESCE(g.max_gross_booking_total_price, 0)                       AS max_gross_booking_total_price,
                    COALESCE(g.avg_gross_booking_total_price, 0)                       AS avg_gross_booking_total_price,
                    COALESCE(g.gross_margin, 0)                                        AS gross_margin,
                    COALESCE(c.canx_margin, 0)                                         AS canx_margin,
                    COALESCE(g.avg_gross_no_nights, 0)                                 AS avg_gross_no_nights,
                    COALESCE(g.max_gross_price_per_night, 0)                           AS max_gross_price_per_night,
                    COALESCE(g.avg_gross_price_per_night, 0)                           AS avg_gross_price_per_night,
                    COALESCE(g.max_gross_price_per_person_per_night, 0)                AS max_gross_price_per_person_per_night,
                    COALESCE(g.avg_gross_price_per_person_per_night, 0)                AS avg_gross_price_per_person_per_night

             FROM data_vault_mvp.dwh.user_attributes ua
                 LEFT JOIN gross g ON ua.shiro_user_id = g.shiro_user_id
                 LEFT JOIN cancelled c ON ua.shiro_user_id = c.shiro_user_id
             WHERE ua.signup_tstamp::DATE <= CURRENT_DATE --change to TO_DATE('{schedule.tstamp}')
         )
    SELECT cub.date,
           cub.shiro_user_id,
           cub.signup_date,
           cub.member_recency_status,
           cub.total_gross_bookings,
           cub.total_cancelled_bookings,
           total_gross_bookings - cub.total_cancelled_bookings                                 AS total_net_bookings,
           cub.total_gross_family_bookings,
           cub.total_gross_domestic_bookings,
           cub.total_cancelled_domestic_bookings,
           cub.total_gross_domestic_bookings - cub.total_cancelled_domestic_bookings           AS net_domestic_bookings,

           cub.total_gross_international_bookings,
           cub.total_cancelled_international_bookings,
           cub.total_gross_international_bookings - cub.total_cancelled_international_bookings AS net_international_bookings,

           cub.total_gross_bookings_less_13m,
           cub.total_cancelled_bookings_less_13m,
           cub.total_gross_bookings_less_13m - cub.total_cancelled_bookings_less_13m           AS net_bookings_less_13m,
           cub.total_gross_bookings_more_13m,
           cub.total_cancelled_bookings_more_13m,
           cub.total_gross_bookings_more_13m - cub.total_cancelled_bookings_more_13m           AS net_bookings_more_13m,
           CASE
               WHEN total_net_bookings = 1 AND net_bookings_less_13m = 1 THEN 'Single'
               WHEN total_net_bookings > 1 AND net_bookings_less_13m > 1 THEN 'Repeat'
               WHEN total_net_bookings = 1 AND net_bookings_more_13m = 1 THEN 'Lapsed Single'
               WHEN total_net_bookings > 1 AND net_bookings_more_13m > 1 THEN 'Lapsed Repeat'
               ELSE 'Prospect'
               END                                                                             AS booker_segment,

           cub.gross_booking_total_price,
           cub.cancelled_booking_total_price,
           cub.gross_booking_total_price - cub.cancelled_booking_total_price                   AS net_booking_total_price,
           cub.max_gross_booking_total_price,
           cub.avg_gross_booking_total_price,
           cub.gross_margin,
           cub.canx_margin,
           cub.gross_margin - cub.canx_margin                                                  AS net_margin,
           cub.avg_gross_no_nights,
           cub.max_gross_price_per_night,
           cub.avg_gross_price_per_night,
           cub.max_gross_price_per_person_per_night,
           cub.avg_gross_price_per_person_per_night,

           CASE
               WHEN total_net_bookings > 5 THEN '5+'
               WHEN total_net_bookings > 2 THEN '3-4'
               WHEN total_net_bookings = 2 THEN '2'
               WHEN total_net_bookings = 1 THEN '1'
               ELSE '0'
               END                                                                             AS total_net_bookings_bucket,
           CASE
               WHEN net_margin > 1000 THEN '£1000+'
               WHEN net_margin > 500 THEN '£500-£1000'
               WHEN net_margin > 100 THEN '£100-£500'
               ELSE '<£100'
               END                                                                             AS total_net_margin_bucket,
           CASE
               WHEN total_net_bookings = 0 THEN 'Non Booker'
               WHEN net_domestic_bookings / total_net_bookings = 1 THEN '100% Domestic'
               WHEN net_domestic_bookings / total_net_bookings >= 0.6 THEN '>=60% Domestic'
               WHEN net_international_bookings / total_net_bookings = 1 THEN '100% International'
               WHEN net_international_bookings / total_net_bookings >= 0.6 THEN '>=60% International'
               ELSE 'Mixed'
               END                                                                             AS travel_type_segment
    FROM combine_users_bookings cub
);


SELECT GET_DDL('table', 'se.data.fact_booking');



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE se_dev_robin.data.user_subscription CLONE se.data.user_subscription;


self_describing_task --include '/dv/dwh/user_attributes/user_segmentation.py'  --method 'run' --start '2020-12-08 00:00:00' --end '2020-12-08 00:00:00'

SELECT us.user_id,
       us.subscription_type,
       CASE
           WHEN us.subscription_type = 0 THEN 'opted out'
           WHEN us.subscription_type IN (1, 2) THEN 'opted in'
           END AS opt_in_status
FROM se.data.user_subscription us
WHERE us.calendar_date = CURRENT_DATE --scheduled tstamp
;

SELECT ua.shiro_user_id,
       ua.web_sessions_1d + ua.app_sessions_1d + ua.emails_1d    AS last_active_1d,
       ua.web_sessions_7d + ua.app_sessions_7d + ua.emails_7d    AS last_active_7d,
       ua.web_sessions_14d + ua.app_sessions_14d + ua.emails_14d AS last_active_14d,
       ua.web_sessions_30d + ua.app_sessions_30d + ua.emails_30d AS last_active_30d,
       ua.web_sessions_90d + ua.app_sessions_90d + ua.emails_90d AS last_active_90d
FROM data_vault_mvp_dev_robin.dwh.user_activity ua
WHERE ua.date = CURRENT_DATE - 1 --scheduled tstamp


SELECT fb.shiro_user_id,
       COUNT(*)                                                                                AS total_gross_bookings,
       SUM(IFF(fb.infant_guests + fb.child_guests > 1, 1, 0))                                  AS total_gross_family_bookings,
       SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic', 1, 0)) AS total_gross_domestic_bookings,
       SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International', 1,
               0))                                                                             AS total_gross_international_bookings,
       SUM(IFF(fb.booking_completed_date >= DATEADD(MONTH, -13, TO_DATE('2020-12-07 03:00:00')), 1,
               0))                                                                             AS total_gross_bookings_less_13m,
       SUM(IFF(fb.booking_completed_date < DATEADD(MONTH, -13, TO_DATE('2020-12-07 03:00:00')), 1,
               0))                                                                             AS total_gross_bookings_more_13m,

       SUM(fb.gross_revenue_gbp)                                                               AS total_gross_revenue,
       MAX(fb.gross_revenue_gbp)                                                               AS max_gross_revenue,
       AVG(fb.gross_revenue_gbp)                                                               AS avg_gross_revenue,

       SUM(fb.margin_gross_of_toms_gbp)                                                        AS total_gross_margin,
       AVG(fb.no_nights)                                                                       AS avg_gross_no_nights,
       MAX(fb.price_per_night)                                                                 AS max_gross_price_per_night,
       AVG(fb.price_per_night)                                                                 AS avg_gross_price_per_night,
       MAX(fb.price_per_person_per_night)                                                      AS max_gross_price_per_person_per_night,
       AVG(fb.price_per_person_per_night)                                                      AS avg_gross_price_per_person_per_night
FROM se_dev_robin.data.fact_booking fb
    LEFT JOIN se_dev_robin.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
WHERE fb.booking_status_type IN ('live', 'cancelled')
  AND fb.booking_completed_date <= TO_DATE('2020-12-07 03:00:00')
GROUP BY 1

SELECT sua.shiro_user_id,
       sua.original_affiliate_id,
       sua.original_affiliate_name,
       sua.original_affiliate_territory_id,
       sua.original_affiliate_territory,
       sua.member_original_affiliate_classification,
       sua.current_affiliate_id,
       sua.current_affiliate_name,
       sua.current_affiliate_territory_id,
       sua.current_affiliate_territory,
       sua.cohort_id,
       sua.cohort_year_month,
       sua.signup_tstamp,
       sua.acquisition_platform,
       sua.email_opt_in,
       sua.push_opt_in,
       sua.app_cohort_id,
       sua.app_cohort_year_month,
       sua.first_app_activity_tstamp,
       sua.last_email_open_tstamp,
       sua.last_email_click_tstamp,
       sua.last_pageview_tstamp,
       sua.last_sale_pageview_tstamp,
       sua.last_abandoned_booking_tstamp,
       sua.last_complete_booking_tstamp
FROM se.data.se_user_attributes sua

DROP TABLE data_vault_mvp_dev_robin.dwh.user_segmentation;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_segmentation us;

------------------------------------------------------------------------------------------------------------------------
--populate history
--create a loop so we don't have to aggregate to day and create proxy avg figures etc

SELECT *
FROM se.data.user_segmentation us
WHERE us.shiro_user_id IN (52788434, 46906266, 35771075);


SET date_var = CURRENT_DATE - 10;

WITH create_grain AS (
    SELECT $date_var                                                             AS date,
           ua.shiro_user_id,
           ua.signup_tstamp::DATE                                                AS signup_date,
           se.data.member_recency_status(ua.signup_tstamp, $date_var::TIMESTAMP) AS member_recency_status
    FROM data_vault_mvp.dwh.user_attributes ua
    WHERE ua.signup_tstamp::DATE <= $date_var
      AND ua.shiro_user_id IN (52788434, 46906266, 35771075)
),
     model_gross AS (
         SELECT fb.shiro_user_id,
                COUNT(*)                                                                    AS total_gross_bookings,
                SUM(IFF(fb.infant_guests + fb.child_guests > 1, 1, 0))                      AS total_gross_family_bookings,
                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic', 1,
                        0))                                                                 AS total_gross_domestic_bookings,
                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International', 1,
                        0))                                                                 AS total_gross_international_bookings,
                SUM(IFF(fb.booking_completed_date >= DATEADD(MONTH, -13, $date_var), 1, 0)) AS total_gross_bookings_less_13m,
                SUM(IFF(fb.booking_completed_date < DATEADD(MONTH, -13, $date_var), 1, 0))  AS total_gross_bookings_more_13m,

                SUM(fb.gross_revenue_gbp)                                                   AS total_gross_revenue,
                MAX(fb.gross_revenue_gbp)                                                   AS max_gross_revenue,
                AVG(fb.gross_revenue_gbp)                                                   AS avg_gross_revenue,

                SUM(fb.margin_gross_of_toms_gbp)                                            AS total_gross_margin,
                AVG(fb.no_nights)                                                           AS avg_gross_no_nights,
                MAX(fb.price_per_night)                                                     AS max_gross_price_per_night,
                AVG(fb.price_per_night)                                                     AS avg_gross_price_per_night,
                MAX(fb.price_per_person_per_night)                                          AS max_gross_price_per_person_per_night,
                AVG(fb.price_per_person_per_night)                                          AS avg_gross_price_per_person_per_night
         FROM se.data.fact_booking fb
             LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
         WHERE fb.booking_status_type IN ('live', 'cancelled')
           AND fb.booking_completed_date <= $date_var
         GROUP BY 1
     ),
     model_canx AS (
         SELECT fb.shiro_user_id,
                COUNT(*)                                                               AS total_cancelled_bookings,
                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic', 1,
                        0))                                                            AS total_cancelled_domestic_bookings,
                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International', 1,
                        0))                                                            AS total_cancelled_international_bookings,
                SUM(IFF(fb.cancellation_date >= DATEADD(MONTH, -13, $date_var), 1, 0)) AS total_cancelled_bookings_less_13m,
                SUM(IFF(fb.cancellation_date < DATEADD(MONTH, -13, $date_var), 1, 0))  AS total_cancelled_bookings_more_13m,
                SUM(fb.gross_revenue_gbp)                                              AS total_cancelled_revenue,
                SUM(fb.margin_gross_of_toms_gbp)                                       AS total_cancelled_margin
         FROM se.data.fact_booking fb
             LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
         WHERE fb.booking_status_type = 'cancelled'
           AND fb.cancellation_date <= $date_var
         GROUP BY 1
     ),
     model_subscription AS (
         SELECT us.user_id,
                us.subscription_type,
                CASE
                    WHEN us.subscription_type = 0 THEN 'opted out'
                    WHEN us.subscription_type IN (1, 2) THEN 'opted in'
                    END AS opt_in_status
         FROM se.data.user_subscription us
         WHERE us.calendar_date = $date_var
     ),
     model_user_activity AS (
         SELECT ua.shiro_user_id,
                ua.web_sessions_1d + ua.app_sessions_1d + ua.emails_1d    AS activity_1d,
                ua.web_sessions_7d + ua.app_sessions_7d + ua.emails_7d    AS activity_7d,
                ua.web_sessions_14d + ua.app_sessions_14d + ua.emails_14d AS activity_14d,
                ua.web_sessions_30d + ua.app_sessions_30d + ua.emails_30d AS activity_30d,
                ua.web_sessions_90d + ua.app_sessions_90d + ua.emails_90d AS activity_90d
         FROM data_vault_mvp.dwh.user_activity ua
         WHERE ua.date = $date_var
     )
SELECT cg.date,
       cg.shiro_user_id,
       cg.signup_date,
       cg.member_recency_status,
       COALESCE(g.total_gross_bookings, 0)                             AS gross_bookings,
       COALESCE(c.total_cancelled_bookings, 0)                         AS cancelled_bookings,
       gross_bookings - cancelled_bookings                             AS net_bookings,

       COALESCE(g.total_gross_family_bookings, 0)                      AS gross_family_bookings,

       COALESCE(g.total_gross_domestic_bookings, 0)                    AS gross_domestic_bookings,
       COALESCE(c.total_cancelled_domestic_bookings, 0)                AS cancelled_domestic_bookings,
       gross_domestic_bookings - cancelled_domestic_bookings           AS net_domestic_bookings,

       COALESCE(g.total_gross_international_bookings, 0)               AS gross_international_bookings,
       COALESCE(c.total_cancelled_international_bookings, 0)           AS cancelled_international_bookings,
       gross_international_bookings - cancelled_international_bookings AS net_international_bookings,

       COALESCE(g.total_gross_bookings_less_13m, 0)                    AS gross_bookings_less_13m,
       COALESCE(c.total_cancelled_bookings_less_13m, 0)                AS cancelled_bookings_less_13m,
       gross_bookings_less_13m - cancelled_bookings_less_13m           AS net_bookings_less_13m,

       COALESCE(g.total_gross_bookings_more_13m, 0)                    AS gross_bookings_more_13m,
       COALESCE(c.total_cancelled_bookings_more_13m, 0)                AS cancelled_bookings_more_13m,
       gross_bookings_more_13m - cancelled_bookings_more_13m           AS net_bookings_more_13m,

       CASE
           WHEN net_bookings = 1 AND net_bookings_less_13m = 1 THEN 'Single'
           WHEN net_bookings > 1 AND net_bookings_less_13m > 1 THEN 'Repeat'
           WHEN net_bookings = 1 AND net_bookings_more_13m = 1 THEN 'Lapsed Single'
           WHEN net_bookings > 1 AND net_bookings_more_13m > 1 THEN 'Lapsed Repeat'
           ELSE 'Prospect'
           END                                                         AS booker_segment,

       COALESCE(g.total_gross_revenue, 0)                              AS gross_revenue,
       COALESCE(c.total_cancelled_revenue, 0)                          AS cancelled_revenue,
       gross_revenue - cancelled_revenue                               AS net_revenue,

       COALESCE(g.max_gross_revenue, 0)                                AS max_gross_revenue,
       COALESCE(g.avg_gross_revenue, 0)                                AS avg_gross_revenue,

       COALESCE(g.total_gross_margin, 0)                               AS gross_margin,
       COALESCE(c.total_cancelled_margin, 0)                           AS cancelled_margin,
       gross_margin - cancelled_margin                                 AS net_margin,

       COALESCE(g.avg_gross_no_nights, 0)                              AS avg_gross_no_nights,
       COALESCE(g.max_gross_price_per_night, 0)                        AS max_gross_price_per_night,
       COALESCE(g.avg_gross_price_per_night, 0)                        AS avg_gross_price_per_night,
       COALESCE(g.max_gross_price_per_person_per_night, 0)             AS max_gross_price_per_person_per_night,
       COALESCE(g.avg_gross_price_per_person_per_night, 0)             AS avg_gross_price_per_person_per_night,

       CASE
           WHEN net_bookings >= 5 THEN '5+'
           WHEN net_bookings > 2 THEN '3-4'
           WHEN net_bookings = 2 THEN '2'
           WHEN net_bookings = 1 THEN '1'
           ELSE '0'
           END                                                         AS net_bookings_bucket,
       CASE
           WHEN net_margin > 1000 THEN '£1000+'
           WHEN net_margin > 500 THEN '£500-£1000'
           WHEN net_margin > 100 THEN '£100-£500'
           ELSE '<£100'
           END                                                         AS net_margin_bucket,
       CASE
           WHEN net_bookings = 0 THEN 'Non Booker'
           WHEN net_domestic_bookings / net_bookings = 1 THEN '100% Domestic'
           WHEN net_domestic_bookings / net_bookings >= 0.6 THEN '>=60% Domestic'
           WHEN net_international_bookings / net_bookings = 1 THEN '100% International'
           WHEN net_international_bookings / net_bookings >= 0.6 THEN '>=60% International'
           ELSE 'Mixed'
           END                                                         AS travel_type_segment,

       s.subscription_type,
       s.opt_in_status,

       ua.activity_1d,
       ua.activity_7d,
       ua.activity_14d,
       ua.activity_30d,
       ua.activity_90d,

       CASE
           WHEN ua.activity_1d > 0 THEN 'last_active_1d'
           WHEN ua.activity_7d > 0 THEN 'last_active_7d'
           WHEN ua.activity_14d > 0 THEN 'last_active_14d'
           WHEN ua.activity_30d > 0 THEN 'last_active_30d'
           WHEN ua.activity_90d > 0 THEN 'last_active_90d'
           ELSE 'last_active_90d+'
           END                                                         AS engagement_segment

FROM create_grain cg
    LEFT JOIN model_gross g ON cg.shiro_user_id = g.shiro_user_id
    LEFT JOIN model_canx c ON cg.shiro_user_id = c.shiro_user_id
    LEFT JOIN model_subscription s ON cg.shiro_user_id = s.user_id
    LEFT JOIN model_user_activity ua ON cg.shiro_user_id = ua.shiro_user_id;


CREATE OR REPLACE PROCEDURE scratch.robinpatel.backfill_user_segmentation_loop(p_first_run DOUBLE, p_max_runs DOUBLE
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
        `INSERT INTO data_vault_mvp_dev_robin.dwh.user_segmentation
          WITH create_grain AS (
            SELECT ${date_var}                                                             AS date,
                   ua.shiro_user_id,
                   ua.signup_tstamp::DATE                                                AS signup_date,
                   se.data.member_recency_status(ua.signup_tstamp, ${date_var}::TIMESTAMP) AS member_recency_status
            FROM data_vault_mvp.dwh.user_attributes ua
            WHERE ua.signup_tstamp::DATE <= ${date_var}
        ), model_gross AS (
                SELECT
                fb.shiro_user_id,
                COUNT(*) AS total_gross_bookings,
                SUM(IFF(fb.infant_guests + fb.child_guests > 1, 1, 0)) AS total_gross_family_bookings,
                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic', 1, 0)) AS total_gross_domestic_bookings,
                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International', 1, 0)) AS total_gross_international_bookings,
                SUM(IFF(fb.booking_completed_date >= DATEADD(MONTH, -13, ${date_var}), 1, 0)) AS total_gross_bookings_less_13m,
                SUM(IFF(fb.booking_completed_date < DATEADD(MONTH, -13, ${date_var}), 1, 0)) AS total_gross_bookings_more_13m,

                SUM(fb.gross_revenue_gbp) AS total_gross_revenue,
                MAX(fb.gross_revenue_gbp) AS max_gross_revenue,
                AVG(fb.gross_revenue_gbp) AS avg_gross_revenue,

                SUM(fb.margin_gross_of_toms_gbp) AS total_gross_margin,
                AVG(fb.no_nights) AS avg_gross_no_nights,
                MAX(fb.price_per_night) AS max_gross_price_per_night,
                AVG(fb.price_per_night) AS avg_gross_price_per_night,
                MAX(fb.price_per_person_per_night) AS max_gross_price_per_person_per_night,
                AVG(fb.price_per_person_per_night) AS avg_gross_price_per_person_per_night
            FROM se.data.fact_booking fb
                     LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
            WHERE fb.booking_status_type IN ('live', 'cancelled')
              AND fb.booking_completed_date <= ${date_var}
            GROUP BY 1
            ), model_canx AS (
                         SELECT fb.shiro_user_id,
                                COUNT(*) AS total_cancelled_bookings,
                                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'Domestic', 1, 0)) AS total_cancelled_domestic_bookings,
                                SUM(IFF(se.data.se_sale_travel_type(fb.territory, ds.posu_country) = 'International', 1, 0)) AS total_cancelled_international_bookings,
                                SUM(IFF(fb.cancellation_date >= DATEADD(MONTH, -13, ${date_var}), 1, 0)) AS total_cancelled_bookings_less_13m,
                                SUM(IFF(fb.cancellation_date < DATEADD(MONTH, -13, ${date_var}), 1, 0)) AS total_cancelled_bookings_more_13m,
                                SUM(fb.gross_revenue_gbp) AS total_cancelled_revenue,
                                SUM(fb.margin_gross_of_toms_gbp) AS total_cancelled_margin
                         FROM se.data.fact_booking fb
                                  LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
                         WHERE fb.booking_status_type = 'cancelled'
                           AND fb.cancellation_date <= ${date_var}
                         GROUP BY 1
            ), model_subscription AS (
                            SELECT us.user_id,
                               us.subscription_type,
                               CASE
                                   WHEN us.subscription_type = 0 THEN 'opted out'
                                   WHEN us.subscription_type IN (1, 2) THEN 'opted in'
                               END AS opt_in_status
                        FROM se.data.user_subscription us
                        WHERE us.calendar_date = ${date_var}
            ), model_user_activity AS (
                            SELECT ua.shiro_user_id,
                               ua.web_sessions_1d + ua.app_sessions_1d + ua.emails_1d AS activity_1d,
                               ua.web_sessions_7d + ua.app_sessions_7d + ua.emails_7d AS activity_7d,
                               ua.web_sessions_14d + ua.app_sessions_14d + ua.emails_14d AS activity_14d,
                               ua.web_sessions_30d + ua.app_sessions_30d + ua.emails_30d AS activity_30d,
                               ua.web_sessions_90d + ua.app_sessions_90d + ua.emails_90d AS activity_90d
                        FROM data_vault_mvp.dwh.user_activity ua
                        WHERE ua.date = ${date_var}
            )
             SELECT
                    '1970-01-01 00:00:00.000'       AS schedule_tstamp,
                    '2020-12-10 00:00:00.000'       AS run_tstamp,
                    'initial backfill'              AS operation_id,
                    current_date                    AS created_at,
                    current_date                    AS updated_at,

                    cg.date,
                    cg.shiro_user_id,
                    cg.signup_date,
                    cg.member_recency_status,
                    COALESCE(g.total_gross_bookings, 0) AS gross_bookings,
                    COALESCE(c.total_cancelled_bookings, 0) AS cancelled_bookings,
                    gross_bookings - cancelled_bookings AS net_bookings,

                    COALESCE(g.total_gross_family_bookings, 0) AS gross_family_bookings,

                    COALESCE(g.total_gross_domestic_bookings, 0) AS gross_domestic_bookings,
                    COALESCE(c.total_cancelled_domestic_bookings, 0) AS cancelled_domestic_bookings,
                    gross_domestic_bookings - cancelled_domestic_bookings AS net_domestic_bookings,

                    COALESCE(g.total_gross_international_bookings, 0) AS gross_international_bookings,
                    COALESCE(c.total_cancelled_international_bookings, 0) AS cancelled_international_bookings,
                    gross_international_bookings - cancelled_international_bookings AS net_international_bookings,

                    COALESCE(g.total_gross_bookings_less_13m, 0) AS gross_bookings_less_13m,
                    COALESCE(c.total_cancelled_bookings_less_13m, 0) AS cancelled_bookings_less_13m,
                    gross_bookings_less_13m - cancelled_bookings_less_13m AS net_bookings_less_13m,

                    COALESCE(g.total_gross_bookings_more_13m, 0) AS gross_bookings_more_13m,
                    COALESCE(c.total_cancelled_bookings_more_13m, 0) AS cancelled_bookings_more_13m,
                    gross_bookings_more_13m - cancelled_bookings_more_13m AS net_bookings_more_13m,

                    CASE
                        WHEN net_bookings = 1 AND net_bookings_less_13m = 1 THEN 'Single'
                        WHEN net_bookings > 1 AND net_bookings_less_13m >= 1 THEN 'Repeat'
                        WHEN net_bookings = 1 AND net_bookings_more_13m = 1 THEN 'Lapsed Single'
                        WHEN net_bookings > 1 AND net_bookings_more_13m >= 1 THEN 'Lapsed Repeat'
                        ELSE 'Prospect'
                    END AS booker_segment,

                    COALESCE(g.total_gross_revenue, 0) AS gross_revenue,
                    COALESCE(c.total_cancelled_revenue, 0) AS cancelled_revenue,
                    gross_revenue - cancelled_revenue AS net_revenue,

                    COALESCE(g.max_gross_revenue, 0) AS max_gross_revenue,
                    COALESCE(g.avg_gross_revenue, 0) AS avg_gross_revenue,

                    COALESCE(g.total_gross_margin, 0) AS gross_margin,
                    COALESCE(c.total_cancelled_margin, 0) AS cancelled_margin,
                    gross_margin - cancelled_margin AS net_margin,

                    COALESCE(g.avg_gross_no_nights, 0) AS avg_gross_no_nights,
                    COALESCE(g.max_gross_price_per_night, 0) AS max_gross_price_per_night,
                    COALESCE(g.avg_gross_price_per_night, 0) AS avg_gross_price_per_night,
                    COALESCE(g.max_gross_price_per_person_per_night, 0) AS max_gross_price_per_person_per_night,
                    COALESCE(g.avg_gross_price_per_person_per_night, 0) AS avg_gross_price_per_person_per_night,

                    CASE
                        WHEN net_bookings >= 5 THEN '5+'
                        WHEN net_bookings > 2 THEN '3-4'
                        WHEN net_bookings = 2 THEN '2'
                        WHEN net_bookings = 1 THEN '1'
                        ELSE '0'
                    END AS net_bookings_bucket,
                    CASE
                        WHEN net_margin > 1000 THEN '£1000+'
                        WHEN net_margin > 500 THEN '£500-£1000'
                        WHEN net_margin > 100 THEN '£100-£500'
                        ELSE '<£100'
                    END AS net_margin_bucket,
                    CASE
                        WHEN net_bookings = 0 THEN 'Non Booker'
                        WHEN net_domestic_bookings / net_bookings = 1 THEN '100% Domestic'
                        WHEN net_domestic_bookings / net_bookings >= 0.6 THEN '>=60% Domestic'
                        WHEN net_international_bookings / net_bookings = 1 THEN '100% International'
                        WHEN net_international_bookings / net_bookings >= 0.6 THEN '>=60% International'
                        ELSE 'Mixed'
                    END AS travel_type_segment,

                    s.subscription_type,
                    s.opt_in_status,

                    CASE
                        WHEN ua.activity_1d > 0 THEN 'last_active_1d'
                        WHEN ua.activity_7d > 0 THEN 'last_active_7d'
                        WHEN ua.activity_14d > 0 THEN 'last_active_14d'
                        WHEN ua.activity_30d > 0 THEN 'last_active_30d'
                        WHEN ua.activity_90d > 0 THEN 'last_active_90d'
                    ELSE 'last_active_90d+'
                    END AS engagement_segment

             FROM create_grain cg
                      LEFT JOIN model_gross g ON cg.shiro_user_id = g.shiro_user_id
                      LEFT JOIN model_canx c ON cg.shiro_user_id = c.shiro_user_id
                      LEFT JOIN model_subscription s ON cg.shiro_user_id = s.user_id
                      LEFT JOIN model_user_activity ua ON cg.shiro_user_id = ua.shiro_user_id;
        `;

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
};
$$;

TRUNCATE data_vault_mvp_dev_robin.dwh.user_segmentation;

--started at 5:01PM
CALL scratch.robinpatel.backfill_user_segmentation_loop(1, 3);
CALL scratch.robinpatel.backfill_user_segmentation_loop(10, 11);
CALL scratch.robinpatel.backfill_user_segmentation_loop(100, 500);
CALL scratch.robinpatel.backfill_user_segmentation_loop(503, 1000);
CALL scratch.robinpatel.backfill_user_segmentation_loop(1076, 1077);

--approx 4 hours

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_segmentation us;

--check which dates are populated
SELECT date, COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.user_segmentation us
GROUP BY 1
ORDER BY 1;

USE WAREHOUSE pipe_xlarge;


--DELETE ACTIVITY PAST 2019

USE WAREHOUSE pipe_xlarge;

DELETE
FROM data_vault_mvp_dev_robin.dwh.user_segmentation us
WHERE us.date < '2019-01-01';


UPDATE data_vault_mvp_dev_robin.dwh.user_segmentation target
SET target.booker_segment      =
        CASE
            WHEN net_bookings = 1 AND net_bookings_less_13m = 1 THEN 'Single'
            WHEN net_bookings > 1 AND net_bookings_less_13m >= 1 THEN 'Repeat'
            WHEN net_bookings = 1 AND net_bookings_more_13m = 1 THEN 'Lapsed Single'
            WHEN net_bookings > 1 AND net_bookings_more_13m >= 1 THEN 'Lapsed Repeat'
            ELSE 'Prospect'
            END,
    target.net_bookings_bucket =
        CASE
            WHEN net_bookings >= 5 THEN '5+'
            WHEN net_bookings > 2 THEN '3-4'
            WHEN net_bookings = 2 THEN '2'
            WHEN net_bookings = 1 THEN '1'
            ELSE '0'
            END

CALL scratch.robinpatel.backfill_user_segmentation_loop(714, 1079);

