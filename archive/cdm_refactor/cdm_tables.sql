USE WAREHOUSE pipe_xlarge;

SELECT calendar_date,
       user_id,
       booking_date,
       check_in_date,
       check_out_date,
       no_nights,
       commission_ex_vat,
       margin_gross_of_toms,
       first_booking_date,
       last_booking_date,
       customer_age,
       member_days_since_prev_unique_booking,
       member_cumulative_gone_on_holidays_cnt,
       member_days_since_previous_checkout,
       member_days_since_first_checkout,
       member_completed_bookings_cnt,
       member_unique_completed_hotel_bookings_cnt,
       member_unique_completed_package_bookings_cnt,
       member_cumulative_bookings_cnt,
       member_cumulative_margin_gbp
FROM data_vault_mvp.customer_model_last7days_uk_de_stg.stream_booking;


CREATE TABLE scratch.robinpatel.stream_booking
(
    schedule_tstamp                              TIMESTAMP,
    run_tstamp                                   TIMESTAMP,
    operation_id                                 VARCHAR,
    created_at                                   TIMESTAMP,
    updated_at                                   TIMESTAMP,

    calendar_date                                DATE,
    user_id                                      INT,
    booking_date                                 DATE,
    check_in_date                                DATE,
    check_out_date                               DATE,
    no_nights                                    INT,
    commission_ex_vat                            DOUBLE,
    margin_gross_of_toms                         DOUBLE,
    first_booking_date                           DATE,
    last_booking_date                            DATE,
    customer_age                                 INT,
    member_cumulative_gone_on_holidays_cnt       INT,
    member_days_since_first_checkout             INT,
    member_days_since_previous_checkout          INT,
    member_days_since_prev_unique_booking        INT,
    member_completed_bookings_cnt                INT,
    member_unique_completed_hotel_bookings_cnt   INT,
    member_unique_completed_package_bookings_cnt INT,
    member_cumulative_bookings_cnt               INT,
    member_cumulative_margin_gbp                 FLOAT
);

INSERT INTO scratch.robinpatel.stream_booking
SELECT '2020-03-24 00:00:00',
       '2020-03-25 00:00:00',
       'initial_backfill',
       '2020-03-25 00:00:00',
       '2020-03-25 00:00:00',
       calendar_date,
       user_id,
       booking_date,
       check_in_date,
       check_out_date,
       no_nights,
       commission_ex_vat,
       margin_gross_of_toms,
       first_booking_date,
       last_booking_date,
       customer_age,
       member_cumulative_gone_on_holidays_cnt,
       member_days_since_first_checkout,
       member_days_since_previous_checkout,
       member_days_since_prev_unique_booking,
       member_completed_bookings_cnt,
       member_unique_completed_hotel_bookings_cnt,
       member_unique_completed_package_bookings_cnt,
       member_cumulative_bookings_cnt,
       member_cumulative_margin_gbp
FROM data_vault_mvp.customer_model_last7days_uk_de_stg.stream_booking;


SELECT * FROM scratch.robinpatel.stream_booking;

SELECT COUNT(*) FROM scratch.robinpatel.stream_booking;
SELECT COUNT(*) FROM data_vault_mvp.customer_model_last7days_uk_de_stg.stream_booking;


SELECT * FROM scratch.robinpatel.stream_booking_update;


CREATE TABLE scratch.robinpatel.stream_booking_full
(
    schedule_tstamp                              TIMESTAMP,
    run_tstamp                                   TIMESTAMP,
    operation_id                                 VARCHAR,
    created_at                                   TIMESTAMP,
    updated_at                                   TIMESTAMP,

    calendar_date                                DATE,
    user_id                                      INT,
    booking_date                                 DATE,
    check_in_date                                DATE,
    check_out_date                               DATE,
    no_nights                                    INT,
    commission_ex_vat                            DOUBLE,
    margin_gross_of_toms                         DOUBLE,
    first_booking_date                           DATE,
    last_booking_date                            DATE,
    customer_age                                 INT,
    member_cumulative_gone_on_holidays_cnt       INT,
    member_days_since_first_checkout             INT,
    member_days_since_previous_checkout          INT,
    member_days_since_prev_unique_booking        INT,
    member_completed_bookings_cnt                INT,
    member_unique_completed_hotel_bookings_cnt   INT,
    member_unique_completed_package_bookings_cnt INT,
    member_cumulative_bookings_cnt               INT,
    member_cumulative_margin_gbp                 FLOAT
);

INSERT INTO scratch.robinpatel.stream_booking_full
SELECT '2020-03-24 00:00:00',
       '2020-03-25 00:00:00',
       'initial_backfill',
       '2020-03-25 00:00:00',
       '2020-03-25 00:00:00',
       calendar_date,
       user_id,
       booking_date,
       check_in_date,
       check_out_date,
       no_nights,
       commission_ex_vat,
       margin_gross_of_toms,
       first_booking_date,
       last_booking_date,
       customer_age,
       member_cumulative_gone_on_holidays_cnt,
       member_days_since_first_checkout,
       member_days_since_previous_checkout,
       member_days_since_prev_unique_booking,
       member_completed_bookings_cnt,
       member_unique_completed_hotel_bookings_cnt,
       member_unique_completed_package_bookings_cnt,
       member_cumulative_bookings_cnt,
       member_cumulative_margin_gbp
FROM scratch.robinpatel.stream_booking_update;

SELECT * FROM scratch.robinpatel.stream_booking_full WHERE user_id = '62972247';