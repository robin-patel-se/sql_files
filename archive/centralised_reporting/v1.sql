USE WAREHOUSE pipe_xlarge;

SET var_date = current_date - 7;
WITH spvs AS (
--spvs
    SELECT sts.se_sale_id,
           sts.event_tstamp::DATE         AS date,
           CASE
               WHEN stmc.touch_mkt_channel IN ('Other', 'Partner', 'Email - Other') THEN 'Other'
               WHEN stmc.touch_mkt_channel IN ('Blog', 'Direct', 'Organic Search', 'Organic Social') THEN 'Free'
               WHEN stmc.touch_mkt_channel IN ('Email - Other', 'Media', 'Other', 'Partner', 'YouTube') THEN 'Other'
               WHEN stmc.touch_mkt_channel IN
                    ('Affiliate Program', 'Display', 'Paid Social', 'PPC - Brand', 'PPC - Non Brand CPA', 'PPC - Non Brand CPL',
                     'PPC - Undefined') THEN 'Paid'
               ELSE stmc.touch_mkt_channel
               END                        AS channel, --last click channel
           CASE
               WHEN stba.touch_experience IN ('mobile wrap android', 'mobile wrap ios') THEN 'Wrap App'
               ELSE INITCAP(stba.touch_experience)
               END                        AS platform,
           COUNT(DISTINCT sts.event_hash) AS spvs,
           COUNT(DISTINCT sts.touch_id)   AS sessions
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    WHERE sts.event_tstamp >= $var_date
    GROUP BY 1, 2, 3, 4
),
     bookings AS (
         --bookings
         SELECT fcb.sale_id                         AS se_sale_id,
                fcb.booking_completed_date          AS date,
                CASE
                    WHEN stmc.touch_mkt_channel IN ('Other', 'Partner', 'Email - Other') THEN 'Other'
                    WHEN stmc.touch_mkt_channel IN ('Blog', 'Direct', 'Organic Search', 'Organic Social') THEN 'Free'
                    WHEN stmc.touch_mkt_channel IN ('Email - Other', 'Media', 'Other', 'Partner', 'YouTube') THEN 'Other'
                    WHEN stmc.touch_mkt_channel IN
                         ('Affiliate Program', 'Display', 'Paid Social', 'PPC - Brand', 'PPC - Non Brand CPA',
                          'PPC - Non Brand CPL',
                          'PPC - Undefined') THEN 'Paid'
                    ELSE stmc.touch_mkt_channel
                    END                             AS channel, --last click channel
                CASE
                    WHEN stba.touch_experience IN ('mobile wrap android', 'mobile wrap ios') THEN 'Wrap App'
                    ELSE INITCAP(stba.touch_experience)
                    END                             AS platform,
                COUNT(1)                            AS trx,
                SUM(fcb.margin_gross_of_toms_gbp)   AS margin,
                SUM(fcb.gross_booking_value_gbp)    AS gross_revenue,
                AVG(fcb.price_per_night)            AS appn,
                AVG(fcb.price_per_person_per_night) AS appppn,
                SUM(fcb.no_nights)                  AS nights,
                AVG(fcb.adult_guests
                    + fcb.child_guests
                    + fcb.infant_guests)            AS avg_guests
         FROM se.data.fact_complete_booking fcb
                  INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
                  INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
         WHERE fcb.booking_completed_date >= $var_date
         GROUP BY 1, 2, 3, 4
     )

SELECT s.se_sale_id,
       CASE
           WHEN ssa.posa_territory = 'UK' THEN 'UK'
           WHEN ssa.posa_territory IN ('DE', 'CH') THEN 'DACH'
           WHEN ssa.posa_territory IN ('SE', 'DK', 'NO') THEN 'Scandi'
           WHEN ssa.posa_territory = 'BE' THEN 'Belgium'
           WHEN ssa.posa_territory = 'NL' THEN 'Netherlands'
           WHEN ssa.posa_territory = 'FR' THEN 'France'
           WHEN ssa.posa_territory = 'IT' THEN 'Italy'
           WHEN ssa.posa_territory = 'SE' THEN 'Spain'
           WHEN ssa.posa_territory IN ('SG', 'HK', 'MY', 'ID') THEN 'Asia'
           END
                        AS posa_category,
       ssa.posa_territory,
       ssa.posu_division,
       ssa.posu_country,
       ssa.posu_city,
       ssa.sale_active,
       ssa.start_date,
       ssa.sale_name,
       ssa.salesforce_opportunity_id,
       cs.name          AS company_name,
       s.date,
       sc.date_value,
       sc.day_name,
       sc.year,
       sc.se_year,
       sc.se_week,
       sc.month,
       sc.month_name,
       sc.day_of_month,
       sc.day_of_week,
       sc.week_start,
       sc.yesterday,
       sc.yesterday_last_week,
       sc.this_week_wtd AS this_week,
       sc.last_week_wtd AS wtd_last_week,
       sc.last_week     AS last_week,
       s.channel,
       s.platform,
       s.spvs,
       s.sessions,
       b.trx,
       b.margin,
       b.gross_revenue,
       b.appn,
       b.appppn,
       b.nights,
       b.avg_guests

FROM spvs s
         LEFT JOIN bookings b ON
        s.se_sale_id = b.se_sale_id
        AND s.date = b.date
        AND s.channel = b.channel
        AND s.platform = b.platform
         INNER JOIN se.data.se_sale_attributes ssa ON s.se_sale_id = ssa.se_sale_id
         LEFT JOIN se.data.se_calendar sc ON s.date = sc.date_value
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot cs ON ssa.company_id = cs.id
WHERE ssa.product_configuration = 'Hotel'
  AND ssa.data_model = 'New Data Model';

SET date = '2020-05-25'::DATE;

SELECT *
FROM se.data.se_room_rates srr;


------------------------------------------------------------------------------------------------------------------------
--se_calendar
SELECT *
FROM (
         SELECT sc.date_value,
                sc.day_name,
                sc.year,
                sc.se_year,
                sc.se_week,
                sc.month,
                sc.month_name,
                sc.day_of_month,
                sc.day_of_week,
                DAYOFWEEKISO(sc.date_value)                                                            AS day_of_week_iso,
                sc.week_start,
                IFF(sc.date_value = $date - 1, TRUE, FALSE)                                            AS yesterday,
                IFF(sc.date_value = $date - 8, TRUE, FALSE)                                            AS yesterday_last_week,
                IFF(sc.week_start = DATE_TRUNC(WEEK, $date)
                        AND sc.date_value < $date, TRUE, FALSE)                                        AS this_week,
                IFF(sc.week_start = DATE_TRUNC(WEEK, $date - 8), TRUE, FALSE)                          AS last_week,
                IFF(last_week AND DAYOFWEEKISO(sc.date_value) <= DAYOFWEEKISO($date - 1), TRUE, FALSE) AS wtd_last_week,

                IFF(TO_CHAR(sc.date_value, 'YYYY-MM') = TO_CHAR($date - 1, 'YYYY-MM'), TRUE, FALSE)    AS this_month,
                IFF(TO_CHAR(sc.date_value, 'YYYY-MM') = TO_CHAR(DATEADD(MONTH, -1, ($date - 1)), 'YYYY-MM'), TRUE,
                    FALSE)                                                                             AS last_month,
                IFF(last_month AND sc.day_of_month <= DAYOFMONTH($date - 1), TRUE, FALSE)              AS mtd_last_month
         FROM se.data.se_calendar sc
     )
WHERE wtd_last_week;

self_describing_task --include 'dv/dwh/ad_hoc/calendar'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_calendar
WHERE date_value >= '2020-05-01';
self_describing_task --include 'se/data/se_calendar'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM se_dev_robin.data.se_calendar sc
WHERE sc.this_week;

SELECT *
FROM se.data.se_calendar sc;


--deal count
--Y lw
--LW WOW remove (metric then variance)

SELECT *
FROM se.data.se_calendar sc
WHERE last_week;

------------------------------------------------------------------------------------------------------------------------
--top up
--stop sale??

SELECT DISTINCT touch_mkt_channel
FROM se.data.scv_touch_marketing_channel stmc;


SELECT sts.se_sale_id,
       sts.event_tstamp::DATE                                               AS date,
       se.data.channel_category(stmc.touch_mkt_channel)                     AS channel, -- last click channel
       se.data.platform_from_touch_experience(stba.touch_experience)        AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory) AS posa_category,
       COUNT(DISTINCT sts.event_hash)                                       AS spvs,
       COUNT(DISTINCT sts.touch_id)                                         AS sessions
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
         INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= $var_date
GROUP BY 1, 2, 3, 4, 5;


SELECT fcb.sale_id                                                          AS se_sale_id,
       fcb.booking_completed_date                                           AS date,
       se.data.channel_category(stmc.touch_mkt_channel)                     AS channel, -- last click channel
       se.data.platform_from_touch_experience(stba.touch_experience)        AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory) AS posa_category,
       COUNT(DISTINCT fcb.booking_id)                                       AS trx,
       SUM(fcb.margin_gross_of_toms_gbp)                                    AS margin,
       SUM(fcb.gross_booking_value_gbp)                                     AS gross_revenue,
       AVG(fcb.price_per_night)                                             AS appn,
       AVG(fcb.price_per_person_per_night)                                  AS appppn,
       SUM(fcb.no_nights)                                                   AS nights,
       AVG(fcb.adult_guests
           + fcb.child_guests
           + fcb.infant_guests)                                             AS avg_guests
FROM se.data.fact_complete_booking fcb
         INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE fcb.booking_completed_date ::DATE = current_date - 1
  AND fcb.sale_id = 'A10864'
GROUP BY 1, 2, 3, 4, 5;

SELECT *
FROM collab.dach.cms_customer_value_jh;

CREATE TABLE dwh.master_se_booking_list
(
    schedule_tstamp                                TIMESTAMPNTZ,
    run_tstamp                                     TIMESTAMPNTZ,
    operation_id                                   VARCHAR,
    created_at                                     TIMESTAMPNTZ,
    updated_at                                     TIMESTAMPNTZ,
    transaction_id                                 VARCHAR,
    booking_id                                     VARCHAR,
    margin_gross_of_toms_gbp                       DOUBLE,
    gross_booking_value_gbp                        DOUBLE,
    shiro_user_id                                  NUMBER,
    sale_name                                      VARCHAR,
    type                                           VARCHAR,
    company                                        VARCHAR,
    supplier                                       VARCHAR,
    country                                        VARCHAR,
    division                                       VARCHAR,
    city                                           VARCHAR,
    provider_name                                  VARCHAR,
    customer_email                                 VARCHAR,
    contractor                                     VARCHAR,
    saleid                                         VARCHAR,
    offername                                      VARCHAR,
    departure_airport_code                         VARCHAR,
    departure_airport_name                         VARCHAR,
    adults                                         NUMBER,
    children                                       NUMBER,
    infants                                        NUMBER,
    county                                         VARCHAR,
    customer_name                                  VARCHAR,
    affiliate                                      VARCHAR,
    original_acquiring_affiliate                   VARCHAR,
    date_booked                                    DATE,
    date_time_booked                               TIMESTAMPNTZ,
    time_booked                                    VARCHAR,
    check_in_date                                  DATE,
    check_out_date                                 DATE,
    no_nights                                      NUMBER,
    rooms                                          NUMBER,
    currency                                       VARCHAR,
    territory                                      VARCHAR,
    total_sell_rate_in_currency                    DOUBLE,
    rate_to_gbp                                    DOUBLE,
    total_sell_rate                                DOUBLE,
    commission_ex_vat                              DOUBLE,
    vat_on_commission                              DOUBLE,
    gross_commission                               DOUBLE,
    total_net_rate                                 DOUBLE,
    customer_total_price                           DOUBLE,
    customer_payment                               DOUBLE,
    credits_used                                   DOUBLE,
    credit_amount_deductible_from_commission       DOUBLE,
    booking_fee_net_rate                           DOUBLE,
    vat_on_booking_fee                             DOUBLE,
    booking_fee                                    DOUBLE,
    payment_type                                   VARCHAR,
    payment_surcharge_net_rate                     DOUBLE,
    vat_on_payment_surcharge                       DOUBLE,
    payment_surcharge                              DOUBLE,
    top_discount                                   VARCHAR,
    total_room_nights                              NUMBER,
    impulse                                        VARCHAR,
    notes                                          VARCHAR,
    user_join_date                                 TIMESTAMPNTZ,
    gross_profit                                   DOUBLE,
    sale_start_date                                DATE,
    sale_end_date                                  DATE,
    destination_name                               VARCHAR,
    destination_type                               VARCHAR,
    week                                           NUMBER,
    month                                          NUMBER,
    postcode                                       VARCHAR,
    city_district                                  VARCHAR,
    total_custom_tax                               VARCHAR,
    platform_name                                  VARCHAR,
    app_download_date                              VARCHAR,
    adx_network                                    VARCHAR,
    adx_creative                                   VARCHAR,
    user_acquisition_platform                      VARCHAR,
    gross_booking_value_in_currency                DOUBLE,
    gross_booking_value                            DOUBLE,
    customer_id                                    VARCHAR,
    number_of_flash_nights                         NUMBER,
    number_of_backfilled_nights                    NUMBER,
    flash_gross_commission_in_supplier_currency    DOUBLE,
    backfill_gross_commission_in_supplier_currency DOUBLE,
    user_country                                   VARCHAR,
    user_state                                     VARCHAR,
    bundle_id                                      VARCHAR,
    sale_dimension                                 VARCHAR,
    dynamic_flight_booked                          VARCHAR,
    arrival_airport                                VARCHAR,
    flight_buy_rate                                DOUBLE,
    flight_sell_rate                               DOUBLE,
    carrier                                        VARCHAR,
    flight_commission                              DOUBLE,
    number_of_bags                                 NUMBER,
    baggage_sell_rate                              DOUBLE,
    atol_fee                                       DOUBLE,
    unique_transaction_reference                   VARCHAR,
    insurance_name                                 VARCHAR,
    insurance_type                                 VARCHAR,
    insurance_policy                               VARCHAR,
    insurance_in_supplier_currency                 DOUBLE,
    insurance_in_customer_currency                 DOUBLE,
    net_insurance_commission_in_customer_currency  DOUBLE,
    agent_id                                       VARCHAR,
    lifetime_bookings                              NUMBER,
    lifetime_margin_gbp                            DOUBLE,
    bookings_less_13m                              NUMBER,
    bookings_more_13m                              NUMBER,
    booker_segment                                 VARCHAR,
    cancelled                                      BOOLEAN,
    refunded                                       BOOLEAN,
    rebooked                                       BOOLEAN,
    sf_case_number                                 NUMBER,
    sf_case_owner_full_name                        VARCHAR,
    sf_transaction_id                              VARCHAR,
    sf_subject                                     VARCHAR,
    sf_opportunity_sale_id                         VARCHAR,
    sf_status                                      VARCHAR,
    sf_case_origin                                 VARCHAR,
    sf_view                                        VARCHAR,
    sf_booking_lookup_check_in_date                TIMESTAMPNTZ,
    sf_booking_lookup_check_out_date               TIMESTAMPNTZ,
    sf_requested_rebooking_date                    DATE,
    sf_postponed_booking_request                   BOOLEAN,
    sf_booking_lookup_store_id                     VARCHAR,
    sf_booking_lookup_supplier_territory           VARCHAR,
    sf_contact_reason                              VARCHAR,
    sf_last_modified_by_full_name                  VARCHAR,
    sf_overbooking_rebooking_stage                 VARCHAR,
    sf_reason                                      VARCHAR,
    sf_case_id                                     VARCHAR,
    sf_date_time_opened                            TIMESTAMPNTZ,
    sf_case_name                                   NUMBER,
    sf_last_modified_date                          DATE,
    sf_last_modified_by_case_overview              VARCHAR,
    sf_priority_type                               VARCHAR,
    sf_covid19_member_resolution_cs                VARCHAR,
    sf_case_overview_id                            VARCHAR,
    sf_case_thread_id                              VARCHAR,
    sf_priority                                    VARCHAR,
    sf_number_dup_cases_solved                     NUMBER,
    sf_status_ho                                   VARCHAR,
    sf_status_pkg                                  VARCHAR,
    adjusted_check_in_date                         DATE,
    adjusted_check_out_date                        DATE,
    voucher_stay_by_date                           TIMESTAMPNTZ,
    bk_cnx_date                                    TIMESTAMPNTZ,
    bk_cnx_last_updated                            TIMESTAMPNTZ,
    bk_cnx_fault                                   VARCHAR,
    bk_cnx_reason                                  VARCHAR,
    bk_cnx_refund_channel                          VARCHAR,
    bk_cnx_refund_type                             VARCHAR,
    bk_cnx_who_pays                                VARCHAR,
    bk_cnx_cancel_with_provider                    BOOLEAN,
    cr_credit_active                               DOUBLE,
    cr_credit_deleted                              DOUBLE,
    cr_credit_used                                 DOUBLE,
    cr_credit_used_tb                              DOUBLE,
    cr_credit_refunded_cash                        DOUBLE,
    m_bacs_refund_timestamp                        VARCHAR,
    m_bacs_payment_status                          VARCHAR,
    m_bacs_customer_currency                       VARCHAR,
    m_bacs_amount_in_customer_currency             VARCHAR,
    m_bacs_bank_details_type                       VARCHAR,
    m_bacs_product_type                            VARCHAR,
    m_bacs_type_of_refund                          VARCHAR,
    m_bacs_reference_transaction_id                VARCHAR,
    m_bacs_refund_speed                            VARCHAR,
    m_bacs_duplicate                               VARCHAR,
    m_bacs_cb_raised                               VARCHAR,
    m_bacs_fraud_team_comment                      VARCHAR,
    cb_se_date                                     DATE,
    cb_se_order_code                               VARCHAR,
    cb_se_payment_method                           VARCHAR,
    cb_se_currency                                 VARCHAR,
    cb_se_payment_amount                           VARCHAR,
    cb_se_status                                   VARCHAR,
    finance_include_flight                         VARCHAR,
    finance_net_amount_paid_fx                     VARCHAR,
    finance_net_amount_paid_gbp                    VARCHAR,
    finance_non_flight_spls_cash_held              VARCHAR,
    finance_non_flight_vcc_held                    VARCHAR,
    finance_flight_refunds_received_gbp            VARCHAR,
    finance_total_held_gbp                         VARCHAR,
    finance_perc_held                              VARCHAR,
    finance_flight_and_non_flight_components_held  VARCHAR,
    finance_refund_made                            VARCHAR,
    finance_refund_type                            VARCHAR,
    finance_amount                                 VARCHAR,
    finance_chargeback                             VARCHAR,
    finance_currency                               VARCHAR,
    finance_amount_inc_margin_adj                  VARCHAR,
    car_flight_pnr                                 VARCHAR,
    car_total_flights                              NUMBER,
    car1_airline_name                              VARCHAR,
    car1_supplier                                  VARCHAR,
    car1_overall_booking_status                    VARCHAR,
    car1_flight_booking_status                     VARCHAR,
    car1_cost_in_buying_currency                   DOUBLE,
    car1_cost_in_gbp                               DOUBLE,
    car1_member_refund_type                        VARCHAR,
    car1_booking_system                            VARCHAR,
    car1_mapping_updated                           DATE,
    car1_mapping_flight_carrier                    VARCHAR,
    car1_mapping_type                              VARCHAR,
    car1_mapping_refund_type                       VARCHAR,
    car1_mapping_reported_refund_type              VARCHAR,
    car2_airline_name                              VARCHAR,
    car2_supplier                                  VARCHAR,
    car2_overall_booking_status                    VARCHAR,
    car2_flight_booking_status                     VARCHAR,
    car2_cost_in_buying_currency                   DOUBLE,
    car2_cost_in_gbp                               DOUBLE,
    car2_member_refund_type                        VARCHAR,
    car2_booking_system                            VARCHAR,
    car2_mapping_updated                           DATE,
    car2_mapping_flight_carrier                    VARCHAR,
    car2_mapping_type                              VARCHAR,
    car2_mapping_refund_type                       VARCHAR,
    car2_mapping_reported_refund_type              VARCHAR,
    car3_airline_name                              VARCHAR,
    car3_supplier                                  VARCHAR,
    car3_overall_booking_status                    VARCHAR,
    car3_flight_booking_status                     VARCHAR,
    car3_cost_in_buying_currency                   DOUBLE,
    car3_cost_in_gbp                               DOUBLE,
    car3_member_refund_type                        VARCHAR,
    car3_booking_system                            VARCHAR,
    car3_mapping_updated                           DATE,
    car3_mapping_flight_carrier                    VARCHAR,
    car3_mapping_type                              VARCHAR,
    car3_mapping_refund_type                       VARCHAR,
    car3_mapping_reported_refund_type              VARCHAR,
    car4_airline_name                              VARCHAR,
    car4_supplier                                  VARCHAR,
    car4_overall_booking_status                    VARCHAR,
    car4_flight_booking_status                     VARCHAR,
    car4_cost_in_buying_currency                   DOUBLE,
    car4_cost_in_gbp                               DOUBLE,
    car4_member_refund_type                        VARCHAR,
    car4_booking_system                            VARCHAR,
    car4_mapping_updated                           DATE,
    car4_mapping_flight_carrier                    VARCHAR,
    car4_mapping_type                              VARCHAR,
    car4_mapping_refund_type                       VARCHAR,
    car4_mapping_reported_refund_type              VARCHAR,
    worldpay_min_event_date                        DATE,
    worldpay_max_event_date                        DATE,
    worldpay_number_events                         NUMBER,
    worldpay_currency                              VARCHAR,
    worldpay_amount                                DOUBLE,
    ratepay_currency                               VARCHAR,
    ratepay_amount                                 DOUBLE,
    ratepay_disagio                                DOUBLE,
    ratepay_transaction_fee                        DOUBLE,
    ratepay_payment_change_fee                     DOUBLE
);

SELECT * FROM se.data.master_se_booking_list msbl;