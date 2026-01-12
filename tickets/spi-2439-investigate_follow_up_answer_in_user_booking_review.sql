SELECT *
FROM se.data.user_booking_review ubr
WHERE ubr.survey_source = 'survey_sparrow'
  AND ubr.review_date = '2022-04-30';

SELECT *
FROM latest_vault.survey_sparrow.nps_responses nr
WHERE nr.response_created_at::DATE = '2022-04-30'
;

SELECT
    PARSE_JSON(nr.record):answers[1]:answer_txt::VARCHAR,
    PARSE_JSON(nr.record):id::VARCHAR,
    PARSE_JSON(nr.record),
    *
FROM raw_vault.survey_sparrow.nps_responses nr
WHERE PARSE_JSON(nr.record):answers[1]:answer_txt::VARCHAR LIKE '%Dilly Hotel%';

SELECT *
FROM latest_vault.survey_sparrow.nps_responses nr
WHERE nr.id = '123509_2721191';

SELECT *
FROM se.data.user_booking_review ubr
WHERE ubr.booking_id = 'A8683502'



SELECT *
FROM latest_vault.survey_sparrow.nps_responses nr
WHERE nr.booking_id = 'A8683502';



WITH flatten_questions AS (
    SELECT
        nr.record,
        nr.booking_id,
        elements.key                           AS follow_up_question_id,
        elements.value                         AS follow_up_question_context,
        TRIM(elements.value:question::VARCHAR) AS follow_up_question,
        elements.value:answer::VARCHAR         AS follow_up_answer
    FROM latest_vault.survey_sparrow.nps_responses nr,
         LATERAL FLATTEN(INPUT => nr.record, OUTER => TRUE) elements
    WHERE elements.key LIKE 'question_%' -- filter flatten based on question keys
      AND TRY_TO_NUMBER(elements.value:answer::VARCHAR) IS NULL --filter out questions that result in the customer score
)
SELECT
    fq.record,
    fq.booking_id,
    fq.follow_up_question_id,
    fq.follow_up_question_context,
    fq.follow_up_question,
    fq.follow_up_answer
FROM flatten_questions fq
WHERE fq.booking_id = 'A8683502'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fq.booking_id ORDER BY fq.follow_up_question_id) = 1
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/reviews/user_booking_review.py' --create-source-objects --method 'run' --start '2022-06-27 00:00:00' --end '2022-06-27 00:00:00'


SELECT
    COUNT(*),
    SUM(IFF(ubr.follow_up_answer IS NOT NULL, 1, 0))
FROM data_vault_mvp.dwh.user_booking_review ubr
WHERE ubr.survey_source = 'survey_sparrow';
SELECT
    COUNT(*),
    SUM(IFF(ubr.follow_up_answer IS NOT NULL, 1, 0))
FROM data_vault_mvp_dev_robin.dwh.user_booking_review ubr
WHERE ubr.survey_source = 'survey_sparrow';


SELECT *
FROM data_vault_mvp.dwh.user_booking_review ubr
WHERE ubr.booking_id = 'A8683502';
SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_booking_review ubr
WHERE ubr.booking_id = 'A8683502';


SELECT
    COUNT(*),
    SUM(IFF(ubr.follow_up_answer IS NOT NULL, 1, 0))
FROM data_vault_mvp.dwh.user_booking_review ubr
WHERE ubr.survey_source = 'survey_sparrow';

-- before 11827
-- after 16624


SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.product_configuration = 'WRD - direct'
  AND ssa.sale_active


SELECT *
FROM latest_vault.perfectstay.wrd_booking;
SELECT *
FROM latest_vault.jetline_travel.wrd_booking;


SELECT *
FROM data_vault_mvp.dwh.wrd_booking wb;

SELECT *
FROM se.data.fact_booking fb
WHERE fb.tech_platform LIKE 'WRD%'

SELECT *
FROM data_vault_mvp.dwh.inactive_users iu;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile AS
SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile iup;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.iterable__user_profile');



CREATE OR REPLACE TRANSIENT TABLE iterable__user_profile
(
    shiro_user_id NUMBER(38, 0)
        email_address VARCHAR (16777216)
        membership_account_status VARCHAR (16777216)
        reference VARCHAR (128)
        locale VARCHAR (16777216)
        territory VARCHAR (16777216)
        territory_region VARCHAR (16777216)
        affiliate_id NUMBER (38, 0)
        main_affiliate_id NUMBER (38, 0)
        affiliate_brand VARCHAR (16777216)
        affiliate_domain VARCHAR (16777216)
        signup_tstamp TIMESTAMP_NTZ (9)
        weekly_opt_in BOOLEAN
        daily_opt_in BOOLEAN
        third_party_optin BOOLEAN
        pause_subscription_end_tstamp TIMESTAMP_NTZ (9)
        title VARCHAR (16777216)
        FIRST_NAME VARCHAR (16777216)
        surname VARCHAR (16777216)
        region VARCHAR (16777216)
        country VARCHAR (16777216)
        referrer_id NUMBER (38, 0)
        acquisition_platform VARCHAR (16777216)
        is_email_address_duplicate BOOLEAN
);



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__catalogue_product AS
SELECT *
FROM data_vault_mvp.dwh.iterable__catalogue_product;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.iterable__catalogue_product');


CREATE OR REPLACE TRANSIENT TABLE iterable__catalogue_product
(
    schedule_tstamp                          TIMESTAMP_NTZ(9),
    run_tstamp                               TIMESTAMP_NTZ(9),
    operation_id                             VARCHAR(16777216),
    created_at                               TIMESTAMP_NTZ(9),
    updated_at                               TIMESTAMP_NTZ(9),
    compound_id                              VARCHAR(16777216),
    se_sale_id                               VARCHAR(16777216),
    id                                       VARCHAR(16777216),
    affiliate_active                         BOOLEAN,
    affiliate_domain                         VARCHAR(16777216),
    affiliate_id                             NUMBER(38, 0),
    affiliate_is_default                     BOOLEAN,
    affiliate_is_main_for_domain             BOOLEAN,
    affiliate_name                           VARCHAR(16777216),
    affiliate_sale_url                       VARCHAR(16777216),
    affiliate_url_string                     VARCHAR(16777216),
    badges                                   ARRAY,
    cancellation_description                 VARCHAR(16777216),
    cancellation_description_warning         VARCHAR(16777216),
    cancellation_summary                     VARCHAR(16777216),
    city_district_id                         VARCHAR(16777216),
    city_district_name                       VARCHAR(16777216),
    city_id                                  NUMBER(38, 0),
    city_name                                VARCHAR(16777216),
    continent_id                             NUMBER(38, 0),
    continent_name                           VARCHAR(16777216),
    country_id                               NUMBER(38, 0),
    country_name                             VARCHAR(16777216),
    currency_code                            VARCHAR(16777216),
    current_sale                             BOOLEAN,
    current_sale_visitors                    NUMBER(38, 0),
    dates_end                                TIMESTAMP_NTZ(9),
    dates_start                              TIMESTAMP_NTZ(9),
    deal_includes                            VARCHAR(16777216),
    deposit_from_price_for_display           VARCHAR(16777216),
    deposit_from_price_unit                  NUMBER(13, 2),
    deposit_from_price_unit_per_person       NUMBER(13, 2),
    destination_name                         VARCHAR(16777216),
    discount                                 NUMBER(13, 2),
    discount_display                         VARCHAR(16777216),
    discount_tooltip                         VARCHAR(16777216),
    display_order                            NUMBER(38, 0),
    division_id                              NUMBER(38, 0),
    division_name                            VARCHAR(16777216),
    end_date_display                         VARCHAR(16777216),
    has_flights_available                    BOOLEAN,
    has_flights_included                     BOOLEAN,
    hash                                     VARCHAR(16777216),
    hotel_details                            VARCHAR(16777216),
    is_catalogue                             BOOLEAN,
    is_connected                             BOOLEAN,
    is_current                               BOOLEAN,
    is_deposit_sale                          BOOLEAN,
    is_dynamic_package                       BOOLEAN,
    is_editors_pick                          BOOLEAN,
    is_exclusive                             BOOLEAN,
    is_hidden_for_app                        BOOLEAN,
    is_hidden_for_whitelabels                BOOLEAN,
    is_hotel_chain                           BOOLEAN,
    is_mysterious                            BOOLEAN,
    is_package                               BOOLEAN,
    is_refundable                            BOOLEAN,
    is_smart_stay                            BOOLEAN,
    is_time_limited                          BOOLEAN,
    is_zero_deposit                          BOOLEAN,
    latitude                                 NUMBER(9, 6),
    lead_image_url_array                     ARRAY,
    lead_image_url_with_size_array           ARRAY,
    lead_rate_for_display                    VARCHAR(16777216),
    lead_rate_label                          VARCHAR(16777216),
    lead_rate_tooltip                        VARCHAR(16777216),
    lead_rate_unit                           NUMBER(13, 2),
    lead_rate_unit_label                     VARCHAR(16777216),
    lead_rate_unit_per_person                NUMBER(13, 2),
    links_price_comparison                   VARCHAR(16777216),
    links_sale                               VARCHAR(16777216),
    links_trip_advisor                       VARCHAR(16777216),
    longitude                                NUMBER(9, 6),
    main_paragraph                           VARCHAR(16777216),
    max_number_of_adults                     NUMBER(38, 0),
    month_availability                       ARRAY,
    number_of_hotel_nights                   NUMBER(38, 0),
    offer_ids                                ARRAY,
    photos                                   ARRAY,
    pricing_model_for_display                VARCHAR(16777216),
    promotion                                VARCHAR(16777216),
    rack_rate_for_display                    VARCHAR(16777216),
    rack_rate_unit                           NUMBER(13, 2),
    rack_rate_unit_per_person                NUMBER(13, 2),
    reason_to_love                           VARCHAR(16777216),
    record                                   VARIANT,
    room_description                         VARCHAR(16777216),
    row_hash                                 VARCHAR(16777216),
    sale_active                              BOOLEAN,
    sale_url                                 VARCHAR(16777216),
    second_opinion                           VARCHAR(16777216),
    show_discount                            BOOLEAN,
    show_prices                              BOOLEAN,
    show_rack_rate                           BOOLEAN,
    summary                                  VARCHAR(16777216),
    tags                                     ARRAY,
    territory                                VARCHAR(16777216),
    territory_id                             NUMBER(38, 0),
    territory_locale                         VARCHAR(16777216),
    times_booked                             NUMBER(38, 0),
    title                                    VARCHAR(16777216),
    travel_details                           VARCHAR(16777216),
    travel_type                              VARCHAR(16777216),
    type                                     VARCHAR(16777216),
    we_like                                  VARCHAR(16777216),
    no_of_reviews                            NUMBER(38, 0),
    average_review_score_zero_to_five        NUMBER(13, 2),
    average_review_score_zero_to_ten         NUMBER(13, 2),
    offer_inclusions_english_locale          ARRAY,
    offer_inclusions_territory_locale        ARRAY,
    common_offer_inclusions_english_locale   ARRAY,
    common_offer_inclusions_territory_locale ARRAY
);

