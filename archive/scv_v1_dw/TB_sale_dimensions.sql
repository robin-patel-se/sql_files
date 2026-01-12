create or replace table TRAVELBIRD_MYSQL.OFFERS_OFFER
(
    DATASET_NAME                   VARCHAR,
    DATASET_SOURCE                 VARCHAR,
    SCHEDULE_INTERVAL              VARCHAR,
    SCHEDULE_TSTAMP                TIMESTAMP,
    RUN_TSTAMP                     TIMESTAMP,
    LOADED_AT                      TIMESTAMP,
    FILENAME                       VARCHAR,
    FILE_ROW_NUMBER                NUMBER,

    id                             NUMBER,
    voucher_release                NUMBER,
    thumbnail                      VARCHAR,
    thumbnail_options              VARCHAR,
    home_image                     VARCHAR,
    home_image_options             VARCHAR,
    title                          VARCHAR,
    short_title                    VARCHAR,
    banner_title                   VARCHAR,
    seo_title                      VARCHAR,
    descriptive_title              VARCHAR,
    slug                           VARCHAR,
    internal_name                  VARCHAR,
    exclude_from_feeds             NUMBER,
    pub_date                       DATE,
    category_date_start            TIMESTAMP,
    category_date_end              TIMESTAMP,
    end_date                       TIMESTAMP,
    active                         NUMBER,
    in_use                         NUMBER,
    hide_from_search               NUMBER,
    no_adwords                     NUMBER,
    price_rounding                 VARCHAR,
    quantization                   NUMBER,
    package_price_amount_of_adults NUMBER,
    price                          DOUBLE,
    price_title                    VARCHAR,
    old_price                      DOUBLE,
    per_person_price               DOUBLE,
    payment_option                 VARCHAR,
    payment_method_description     VARCHAR,
    booking_fee                    DOUBLE,
    booking_fee_for_person         NUMBER,
    down_payment                   DOUBLE,
    disable_instalments            NUMBER,
    fixed_down_payment_fee         NUMBER,
    down_payment_for_person        NUMBER,
    offer_unit                     VARCHAR,
    place_description              VARCHAR,
    included                       VARCHAR,
    excluded                       VARCHAR,
    excluded_short                 VARCHAR,
    features                       VARCHAR,
    details                        VARCHAR,
    editor_tip                     VARCHAR,
    editor_tip_picture             VARCHAR,
    tags                           VARCHAR,
    priority                       NUMBER,
    label                          NUMBER,
    allow_multiple_units           NUMBER,
    participants_fields            VARCHAR,
    customer_fields                VARCHAR,
    birthdate_required             NUMBER,
    category_id                    NUMBER,
    site_id                        NUMBER,
    target_group_id                NUMBER,
    transportation_id              NUMBER,
    hero_id                        VARCHAR,
    hero_options                   VARCHAR,
    partner_id                     NUMBER,
    external_reference             VARCHAR,
    concept_id                     NUMBER,
    package_price_per_night        NUMBER,
    product_line                   VARCHAR
);

CREATE SCHEMA RAW_VAULT_MVP_DEV_ROBIN.TRAVELBIRD_MYSQL;
CREATE OR REPLACE TABLE RAW_VAULT_MVP_DEV_ROBIN.TRAVELBIRD_MYSQL.OFFERS_OFFER CLONE RAW_VAULT_MVP.TRAVELBIRD_MYSQL.OFFERS_OFFER;

SELECT ID, SALE_ID, external_reference
FROM DATA_VAULT_MVP_DEV_ROBIN.TRAVELBIRD_MYSQL_HYGIENE_SNAPSHOTS.OFFERS_OFFER;

DROP TABLE HYGIENE_VAULT_MVP_DEV_ROBIN.TRAVELBIRD_MYSQL.OFFERS_OFFER;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.TRAVELBIRD_MYSQL_HYGIENE_SNAPSHOTS.OFFERS_OFFER;

SELECT se_sale_id,
       site_id,
       product_line,

       CASE
           WHEN product_line = 'flash'
               THEN 'Package'
           WHEN product_line = 'catalogue' AND site_id != 46
               THEN 'Package'
           WHEN product_line = 'catalogue' AND site_id = 46
               THEN 'Hotel'
           END
                                as sale_product, -- type

       CASE
           WHEN product_line = 'flash'
               THEN 'IHP - Dynamic'
           WHEN product_line = 'catalogue' AND site_id != 46
               THEN 'Catalogue'
           WHEN product_line = 'catalogue' AND site_id = 46
               THEN 'Hotel Only'
           END
                                as sale_type,    --sale_dimension

       CASE
           WHEN product_line = 'flash'
               THEN 'Package'
           WHEN product_line = 'catalogue' AND site_id != 46
               THEN 'Package'
           WHEN product_line = 'catalogue' AND site_id = 46
               THEN 'Hotel'
           END
                                as product_type,

       CASE
           WHEN product_line = 'flash'
               THEN 'IHP - Dynamic'
           WHEN product_line = 'catalogue' AND site_id != 46
               THEN 'Package'
           WHEN product_line = 'catalogue' AND site_id = 46
               THEN 'Catalogue'
           END
                                as product_configuration,

       CASE
           WHEN product_line = 'flash'
               THEN 'Flash'
           WHEN product_line = 'catalogue' AND site_id != 46
               THEN 'Catalogue'
           WHEN product_line = 'catalogue' AND site_id = 46
               THEN 'Catalogue'
           END
                                as product_line,

       CASE
           WHEN LEFT(se_sale_id, 1) = 'A'
               THEN 'New Model'
           ELSE 'Old Model' END as sale_model

FROM DATA_VAULT_MVP_DEV_ROBIN.TRAVELBIRD_MYSQL_HYGIENE_SNAPSHOTS.OFFERS_OFFER
WHERE se_sale_id IS NOT NULL;

