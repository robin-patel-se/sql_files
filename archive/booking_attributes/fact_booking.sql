CREATE OR REPLACE TABLE scratch.robinpatel.fact_booking AS
SELECT *
FROM se.data.fact_booking fb;


SELECT GET_DDL('table', 'scratch.robinpatel.fact_booking');

CREATE OR REPLACE TABLE fact_booking
(
    booking_id                                 VARCHAR PRIMARY KEY NOT NULL,
    booking_status                             VARCHAR,
    booking_status_type                        VARCHAR,
    se_sale_id                                 VARCHAR,
    shiro_user_id                              NUMBER,
    check_in_date                              DATE,
    check_out_date                             DATE,
    booking_lead_time_days                     NUMBER,
    booking_created_date                       DATE,
    booking_completed_date                     DATE,
    booking_transaction_completed_date         TIMESTAMP,
    currency                                   VARCHAR,
    gross_revenue_cc                           FLOAT,
    margin_gross_of_toms_cc                    FLOAT,
    gross_revenue_gbp                          FLOAT,
    gross_revenue_gbp_constant_currency        FLOAT,
    gross_revenue_eur_constant_currency        FLOAT,
    customer_total_price_gbp                   FLOAT,
    customer_total_price_gbp_constant_currency FLOAT,
    gross_booking_value_gbp                    FLOAT,
    commission_ex_vat_gbp                      FLOAT,
    booking_fee_net_rate_gbp                   FLOAT,
    payment_surcharge_net_rate_gbp             FLOAT,
    insurance_commission_gbp                   FLOAT,
    margin_gross_of_toms_gbp                   FLOAT,
    margin_gross_of_toms_gbp_constant_currency FLOAT,
    margin_gross_of_toms_eur_constant_currency FLOAT,
    no_nights                                  NUMBER,
    adult_guests                               NUMBER,
    child_guests                               NUMBER,
    infant_guests                              NUMBER,
    price_per_night                            FLOAT,
    price_per_person_per_night                 FLOAT,
    rooms                                      NUMBER,
    device_platform                            VARCHAR,
    booking_full_payment_complete              BOOLEAN,
    cancellation_date                          DATE,
    cancellation_reason                        VARCHAR,
    territory                                  VARCHAR,
    travel_type                                VARCHAR,
    tech_platform                              VARCHAR
);

CREATE OR REPLACE TABLE scratch.robinpatel.dim_sale AS
SELECT *
FROM se.data.dim_sale ds

SELECT GET_DDL('table', 'scratch.robinpatel.dim_sale');

CREATE OR REPLACE TABLE dim_sale
(
    se_sale_id              VARCHAR,
    sale_name               VARCHAR,
    sale_product            VARCHAR,
    sale_type               VARCHAR,
    product_type            VARCHAR,
    product_configuration   VARCHAR,
    product_line            VARCHAR,
    data_model              VARCHAR,
    sale_start_date         TIMESTAMP,
    sale_end_date           TIMESTAMP,
    sale_active             BOOLEAN,
    posa_territory          VARCHAR,
    posa_country            VARCHAR,
    posu_country            VARCHAR,
    posu_division           VARCHAR,
    posu_city               VARCHAR,
    travel_type             VARCHAR,
    target_account_list     VARCHAR,
    posu_sub_region         VARCHAR,
    posu_region             VARCHAR,
    posu_cluster            VARCHAR,
    posu_cluster_region     VARCHAR,
    posu_cluster_sub_region VARCHAR,
    tech_platform           VARCHAR
);

SELECT MIN(es.event_date)
FROM raw_vault_mvp.sfmc.events_sends es
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation;


self_describing_task --include 'dv/dwh/transactional/dim_sale.py'  --method 'run' --start '2021-02-04 00:00:00' --end '2021-02-04 00:00:00'



SELECT *
FROM data_vault_mvp_dev_robin.dwh.dim_sale
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id) > 1;


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;

self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2021-02-04 00:00:00' --end '2021-02-04 00:00:00'

SELECT * FROM se.data.tb_offer t WHERE t.se_sale_id ='A1839';


SELECT *
FROM data_vault_mvp_dev_robin.dwh.dim_sale
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id) > 1;


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.offers_offerconcept_snapshot CLONE data_vault_mvp.travelbird_cms.offers_offerconcept_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;

self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2021-02-04 00:00:00' --end '2021-02-04 00:00:00'

SELECT * FROM se.data.tb_offer t WHERE t.se_sale_id ='A1839';


DROP TABLE HYGIENE_SNAPSHOT_VAULT_MVP_DEV_ROBIN.SFMC.EVENTS_CLICKS;
DROP TABLE HYGIENE_SNAPSHOT_VAULT_MVP_DEV_ROBIN.SFMC.EVENTS_OPENS_PLUS_INFERRED;
DROP TABLE HYGIENE_SNAPSHOT_VAULT_MVP_DEV_ROBIN.SFMC.EVENTS_SENDS;
DROP TABLE RAW_VAULT_MVP_DEV_ROBIN.SFMC.EVENTS_OPENS_PLUS_INFERRED;
DROP TABLE RAW_VAULT_MVP_DEV_ROBIN.SFMC.EVENTS_SENDS;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHED_SPVS;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHED_SPVS_BKUP;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFIABLE_EVENTS;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_ATTRIBUTION;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_MARKETING_CHANNEL;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG_BAK.MODULE_TOUCH_BASIC_ATTRIBUTES__20210202T030000__DAILY_AT_03H00;

SELECT msbl.booking_id
FROM se.data.master_se_booking_list msbl;