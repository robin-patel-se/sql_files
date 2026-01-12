SELECT MIN(loaded_at)
FROM raw_vault_mvp.ratepay.clearing c

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.ratepay.clearing CLONE raw_vault_mvp.ratepay.clearing;
self_describing_task --include 'staging/hygiene/ratepay/clearing.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/ratepay/clearing.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00';


CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.cash_refunds_stripe CLONE raw_vault_mvp.finance_gsheets.cash_refunds_stripe;

self_describing_task --include 'staging/hygiene/finance_gsheets/cash_refunds_stripe.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/cash_refunds_stripe.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se.data.se_hotel_rooms_and_rates shrar;
SELECT *
FROM se.data.se_room_type_rooms_and_rates srtrar;

SELECT *
FROM se.data.master_all_booking_list mabl
WHERE mabl.transaction_id = 'A2052-B-21872663';


SELECT row_number() OVER (ORDER BY ho_margin DESC )          AS rank,
       shiro_user_id,
       ho_bookings,
       ho_margin,
       SUM(ho_margin) OVER (ORDER BY ho_margin DESC)         AS margin_cumulative,
       margin_cumulative / (
           SELECT SUM(ho_margin)
           FROM collab.dach.cms_customer_value_jh
       )                                                     AS cumulative_percent,
       cumulative_percent * 100                              AS percent_value,
       CASE
           WHEN percent_value <= 5 THEN 'HO Ultra-High Value'
           WHEN percent_value <= 20 THEN 'HO High Value'
           WHEN percent_value <= 70 THEN 'HO Usual Value'
           WHEN percent_value <= 100 THEN 'HO Low Value' END AS ho_segment
FROM collab.dach.cms_customer_value_jh
WHERE ho_margin > 0
ORDER BY rank;


SELECT *
FROM se.data.se_room_rates srr;
SELECT *
FROM data_vault_mvp.mari_snapshots.rate_plan_snapshot rps;

SELECT s.map_location
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale s;

SELECT id,
       ss.sale_name,
       ss.posu_country,
       s.map_location,
       SPLIT(s.map_location, '&')[1]::VARCHAR                                       AS map_loc,
       REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, 'll=(.*),', 1, 1, 'e') AS longitude,
       REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, ',(.*)', 1, 1, 'e')    AS latitude
FROM raw_vault_mvp.cms_mysql.sale s
         LEFT JOIN data_vault_mvp.dwh.se_sale ss ON s.id = ss.sale_id;

SELECT *
FROM raw_vault_mvp.cms_mysql.base_sale bs;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.sale CLONE raw_vault_mvp.cms_mysql.sale;

self_describing_task --include 'staging/hygiene/cms_mysql/sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mysql.sale;

CREATE OR REPLACE TABLE hygiene_vault_mvp.cms_mysql.sale_bkup CLONE hygiene_vault_mvp.cms_mysql.sale;

CREATE OR REPLACE TABLE hygiene_vault_mvp.cms_mysql.sale_tmp
(
    -- (lineage) metadata for the current job
    schedule_tstamp                                      TIMESTAMP,
    run_tstamp                                           TIMESTAMP,
    operation_id                                         VARCHAR,
    created_at                                           TIMESTAMP,
    updated_at                                           TIMESTAMP,

    -- (lineage) original metadata columns from previous step
    row_dataset_name                                     VARCHAR,
    row_dataset_source                                   VARCHAR,
    row_loaded_at                                        TIMESTAMP,
    row_schedule_tstamp                                  TIMESTAMP,
    row_run_tstamp                                       TIMESTAMP,
    row_filename                                         VARCHAR,
    row_file_row_number                                  INT,

    -- hygiened columns
    sale_id                                              VARCHAR,
    latitude                                             DOUBLE,
    longitude                                            DOUBLE,

    -- original columns that don't require any hygiene
    id                                                   NUMBER,
    version                                              NUMBER,
    active                                               NUMBER,
    date_created                                         TIMESTAMP,
    destination_name                                     VARCHAR,
    end_date                                             TIMESTAMP,
    last_updated                                         TIMESTAMP,
    location                                             VARCHAR,
    slug                                                 VARCHAR,
    start_date                                           TIMESTAMP,
    title                                                VARCHAR,
    top_discount                                         NUMBER,
    type                                                 VARCHAR,
    promotion                                            VARCHAR,
    commission                                           DOUBLE,
    discount_note                                        VARCHAR,
    board_type                                           VARCHAR,
    destination_type                                     VARCHAR,
    halo                                                 NUMBER,
    promoted                                             NUMBER,
    travel_type                                          VARCHAR,
    contractor_id                                        NUMBER,
    room_description                                     VARCHAR,
    vat_exclusive                                        NUMBER,
    require_address                                      NUMBER,
    require_age                                          NUMBER,
    require_passport                                     NUMBER,
    require_title                                        NUMBER,
    custom_url_slug                                      VARCHAR,
    commission_type                                      VARCHAR,
    default_offer_id                                     NUMBER,
    show_discount_prefix                                 NUMBER,
    enable_hold                                          NUMBER,
    mysterious                                           NUMBER,
    mysterious_title                                     VARCHAR,
    premium                                              DOUBLE,
    instant                                              NUMBER,
    instant_destination                                  VARCHAR,
    instant_type                                         VARCHAR,
    base_currency                                        VARCHAR,
    deposit                                              NUMBER,
    closest_airport_code                                 VARCHAR,
    require_date_of_birth                                NUMBER,
    top_discount_eur                                     NUMBER,
    top_discount_gbp                                     NUMBER,
    top_discount_sek                                     NUMBER,
    main_photo_id                                        NUMBER,
    county                                               VARCHAR,
    destination_country                                  VARCHAR,
    show_price                                           NUMBER,
    repeated                                             NUMBER,
    location_info_id                                     NUMBER,
    city_district_id                                     NUMBER,
    jb_hotel_id                                          NUMBER,
    top_discount_usd                                     NUMBER,
    top_discount_dkk                                     NUMBER,
    send_summary                                         NUMBER,
    top_discount_nok                                     NUMBER,
    top_discount_chf                                     NUMBER,
    with_shared_allocations                              NUMBER,
    salesforce_opportunity_id                            VARCHAR,
    sale_ancillary_details_id                            NUMBER,
    supplier_id                                          NUMBER,
    smart_stay                                           NUMBER,
    hotel_chain_link                                     VARCHAR,
    excluded_from_api                                    NUMBER,
    top_discount_pln                                     NUMBER,
    top_discount_sgd                                     NUMBER,
    top_discount_php                                     NUMBER,
    top_discount_idr                                     NUMBER,
    top_discount_hkd                                     NUMBER,
    top_discount_myr                                     NUMBER,
    trip_advisor_ratings_image                           VARCHAR,
    top_discount_czk                                     NUMBER,
    top_discount_huf                                     NUMBER,
    is_ean_secret_price_sale                             NUMBER,
    is_cee_sale                                          NUMBER,
    is_able_to_sell_flights                              NUMBER,
    joint_contractor_id                                  NUMBER,
    is_team20package                                     NUMBER,
    is_overnight_flight                                  NUMBER,
    zero_deposit                                         NUMBER,
    ski_insurance                                        NUMBER,
    additional_text                                      VARCHAR,
    main_paragraph                                       VARCHAR,
    map_location                                         VARCHAR,
    need_to_know                                         VARCHAR,
    second_opinion                                       VARCHAR,
    we_like                                              VARCHAR,
    hotel_details                                        VARCHAR,
    travel_details                                       VARCHAR,
    reason_to_love                                       VARCHAR,
    expired_copy                                         VARCHAR,
    reviews                                              VARCHAR,
    deal_includes                                        VARCHAR,
    price_compare                                        VARCHAR,
    notes                                                VARCHAR,
    exclusive                                            BOOLEAN,

    -- hygiene flags (minimum as per current business requirements, can be left blank if none needed)
    fails_validation__id__expected_nonnull               INT,
    fails_validation__type__expected_nonnull             INT,
    fails_validation__is_team20package__expected_nonnull INT,
    failed_some_validation                               INT
);

INSERT INTO hygiene_vault_mvp.cms_mysql.sale_tmp
SELECT s.schedule_tstamp,
       s.run_tstamp,
       s.operation_id,
       s.created_at,
       s.updated_at,
       s.row_dataset_name,
       s.row_dataset_source,
       s.row_loaded_at,
       s.row_schedule_tstamp,
       s.row_run_tstamp,
       s.row_filename,
       s.row_file_row_number,
       s.sale_id,
       REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, ',(.*)', 1, 1, 'e')    AS latitude,
       REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, 'll=(.*),', 1, 1, 'e') AS longitude,
       s.id,
       s.version,
       s.active,
       s.date_created,
       s.destination_name,
       s.end_date,
       s.last_updated,
       s.location,
       s.slug,
       s.start_date,
       s.title,
       s.top_discount,
       s.type,
       s.promotion,
       s.commission,
       s.discount_note,
       s.board_type,
       s.destination_type,
       s.halo,
       s.promoted,
       s.travel_type,
       s.contractor_id,
       s.room_description,
       s.vat_exclusive,
       s.require_address,
       s.require_age,
       s.require_passport,
       s.require_title,
       s.custom_url_slug,
       s.commission_type,
       s.default_offer_id,
       s.show_discount_prefix,
       s.enable_hold,
       s.mysterious,
       s.mysterious_title,
       s.premium,
       s.instant,
       s.instant_destination,
       s.instant_type,
       s.base_currency,
       s.deposit,
       s.closest_airport_code,
       s.require_date_of_birth,
       s.top_discount_eur,
       s.top_discount_gbp,
       s.top_discount_sek,
       s.main_photo_id,
       s.county,
       s.destination_country,
       s.show_price,
       s.repeated,
       s.location_info_id,
       s.city_district_id,
       s.jb_hotel_id,
       s.top_discount_usd,
       s.top_discount_dkk,
       s.send_summary,
       s.top_discount_nok,
       s.top_discount_chf,
       s.with_shared_allocations,
       s.salesforce_opportunity_id,
       s.sale_ancillary_details_id,
       s.supplier_id,
       s.smart_stay,
       s.hotel_chain_link,
       s.excluded_from_api,
       s.top_discount_pln,
       s.top_discount_sgd,
       s.top_discount_php,
       s.top_discount_idr,
       s.top_discount_hkd,
       s.top_discount_myr,
       s.trip_advisor_ratings_image,
       s.top_discount_czk,
       s.top_discount_huf,
       s.is_ean_secret_price_sale,
       s.is_cee_sale,
       s.is_able_to_sell_flights,
       s.joint_contractor_id,
       s.is_team20package,
       s.is_overnight_flight,
       s.zero_deposit,
       s.ski_insurance,
       s.additional_text,
       s.main_paragraph,
       s.map_location,
       s.need_to_know,
       s.second_opinion,
       s.we_like,
       s.hotel_details,
       s.travel_details,
       s.reason_to_love,
       s.expired_copy,
       s.reviews,
       s.deal_includes,
       s.price_compare,
       s.notes,
       s."EXCLUSIVE",
       s.fails_validation__id__expected_nonnull,
       s.fails_validation__type__expected_nonnull,
       s.fails_validation__is_team20package__expected_nonnull,
       s.failed_some_validation
FROM hygiene_vault_mvp.cms_mysql.sale s;

CREATE OR REPLACE TABLE hygiene_vault_mvp.cms_mysql.sale CLONE hygiene_vault_mvp.cms_mysql.sale_tmp;
SELECT * FROM hygiene_vault_mvp.cms_mysql.sale;

DROP TABLE hygiene_vault_mvp.cms_mysql.sale_tmp;

DROP TABLE hygiene_vault_mvp_dev_robin.cms_mysql.sale;
------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT MIN(updated_at)
FROM hygiene_vault_mvp.cms_mysql.sale s; --2019-12-16 14:42:40.907454000

airflow backfill --start_date '2019-12-16 01:00:00' --end_date '2019-12-16 01:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__sale__daily_at_01h00
self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/sale.py'  --method 'run' --start '2019-12-16 01:00:00' --end '2019-12-16 01:00:00'

------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT * FROM data_vault_mvp_dev_robin.dwh.se_sale ss WHERE ss.data_model = 'Old Data Model'


SELECT REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, ',(.*)', 1, 1, 'e')    AS latitude,
       REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, 'll=(.*),', 1, 1, 'e') AS longitude,
       s.map_location
FROM hygiene_vault_mvp.cms_mysql.sale s
WHERE TRY_TO_DOUBLE(REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, ',(.*)', 1, 1, 'e')) IS NULL
AND REGEXP_SUBSTR(SPLIT(s.map_location, '&')[1]::VARCHAR, ',(.*)', 1, 1, 'e') IS NOT NULL;

SELECT * FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_tmp;

SELECT COUNT(*) FROM hygiene_vault_mvp.cms_mysql.sale s;
SELECT COUNT(*) FROM hygiene_snapshot_vault_mvp.cms_mysql.sale s;


SELECT ss.se_sale_id,
       se.data.POSA_CATEGORY_FROM_TERRITORY(ssa.posa_territory)
FROM se.data.se_sale_attributes ssa



airflow backfill --start_date '2020-10-05 03:00:00' --end_date '2020-10-05 03:00:00' --task_regex '.*' cms_mysql_snapshot_wave3__daily_at_03h00

