USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE sale_active = 1;

CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation COPY GRANTS CLONE se.data.user_segmentation;
CREATE OR REPLACE TABLE se_dev_robin.data.user_subscription COPY GRANTS CLONE se.data.user_subscription;

SELECT *
FROM se_dev_robin.data.user_segmentation us;



CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation
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
    opt_in_status                  VARCHAR
)
    CLUSTER BY (date);

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

       SUM(fcb.gross_booking_value_gbp)                                   AS total_booking_value,
       MAX(fcb.gross_booking_value_gbp)                                   AS max_booking_value,
       AVG(fcb.gross_booking_value_gbp)                                   AS avg_booking_value,

       COUNT(fcb.booking_id)                                              AS total_bookings,
       SUM(CASE WHEN fcb.infant_guests + fcb.child_guests > 1 THEN 1 END) AS total_family_bookings,

       SUM(fcb.margin_gross_of_toms_gbp)                                  AS total_margin,
       AVG(fcb.no_nights)                                                 AS avg_no_nights,

       MAX(fcb.adult_guests + fcb.child_guests + fcb.infant_guests)       AS max_travellers,
       AVG(fcb.adult_guests + fcb.child_guests + fcb.infant_guests)       AS avg_travellers,

       AVG(fcb.price_per_night)                                           AS avg_price_per_night,

       us.subscription_type,
       us.opt_in_status
FROM se.data.user_segmentation us
         LEFT JOIN se_dev_robin.data.fact_complete_booking fcb ON us.shiro_user_id = fcb.shiro_user_id;

DROP TABLE se_dev_robin.data.user_segmentation;

self_describing_task --include 'se/data/se_user_segmentation'  --method 'run' --start '2020-06-01 00:00:00' --end '2020-06-01 00:00:00'

SELECT *
FROM se_dev_robin.data.user_segmentation us
WHERE us.shiro_user_id = 62972247;

SELECT *
FROM se.data.user_subscription us
WHERE us.user_id = 62972247;
SELECT MAX(us.calendar_date)
FROM se.data.user_subscription us;

WITH user_segmentation AS (
    --get recent segmentation data from user_segmentation table
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

    FROM se_dev_robin.data.user_segmentation us
    WHERE us.date = (
        SELECT MAX(date)
        FROM se_dev_robin.data.user_segmentation
    )
)
SELECT ua.shiro_user_id                 AS user_id,
       ua.current_affiliate_id,
       ua.current_affiliate_name,
       ua.current_affiliate_territory_id,
       ua.current_affiliate_territory   AS current_affiliate_territory_name,
       ua.cohort_id,
       ua.signup_tstamp,
       ua.email_opt_in,
       ua.push_opt_in,
       uf.first_app_activity_tstamp,
       ur.last_email_open_tstamp,
       ur.last_email_click_tstamp,
       ur.last_pageview_tstamp,
       ur.last_sale_pageview_tstamp,
       ur.last_abandoned_booking_tstamp AS last_booking_abandon_tstamp,
       ur.last_complete_booking_tstamp  AS last_booking_complete_tstamp,
       us.max_price_per_night,
       us.avg_price_per_night,
       us.max_price_per_person_per_night,
       us.avg_price_per_person_per_night
FROM {user_attributes_table_ref} ua
         LEFT JOIN {first_activity_table_ref} uf
ON ua.shiro_user_id = uf.shiro_user_id
    LEFT JOIN {recent_activity_table_ref} ur ON ua.shiro_user_id = ur.shiro_user_id
    LEFT JOIN user_segmentation US ON ua.shiro_user_id = US.shiro_user_id
    self_describing_task --include 'ds/user_engagement_snapshot/user_snapshot'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT fcb.booking_id,
       fcb.booking_status,
       fcb.sale_id,
       fcb.shiro_user_id,
       fcb.check_in_date,
       fcb.check_out_date,
       fcb.booking_lead_time_days,
       fcb.booking_created_date,
       fcb.booking_completed_date,
       fcb.gross_booking_value_gbp,
       fcb.commission_ex_vat_gbp,
       fcb.booking_fee_net_rate_gbp,
       fcb.payment_surcharge_net_rate_gbp,
       fcb.insurance_commission_gbp,
       fcb.margin_gross_of_toms_gbp,
       fcb.no_nights,
       fcb.adult_guests,
       fcb.child_guests,
       fcb.infant_guests,
       fcb.price_per_night,
       fcb.price_per_person_per_night,
       fcb.tech_platform
FROM se.data.fact_complete_booking fcb