--upload cube export from chiasma sql server to collab space
USE WAREHOUSE PIPE_LARGE;
USE SCHEMA COLLAB.MUSE_DATA_MODELLING;

/*
 --sql from joe:

 SELECT DISTINCT
 DB.transaction_id
,DB.booking_id
,DS.status AS booking_status
,FB.key_date_booked AS booking_date
,FB.commission_ex_vat
,FB.booking_fee_net_rate
,FB.insurance_net_rate
,FB.insurance_net_rate_constant_curremcy
,FB.payment_surcharge_net_rate
,FB.margin
,FB.margin_constant_currency
,FB.margin_gross_of_toms
,FB.margin_gross_of_toms_constant_currency
,FB.margin_local_currency
,DC.customer_id
,DSA.sale_id
,OS.source_name
,BU.business_name AS territory
FROM dbo.dim_bookings DB
INNER JOIN dbo.dim_status DS ON DB.key_status = DS.key_status
INNER JOIN dbo.fact_bookings_v FB ON DB.key_booking = FB.key_booking
INNER JOIN dbo.business_units BU ON FB.key_current_business_unit_id = BU.business_unit_id
INNER JOIN dbo.dim_customers DC ON FB.key_customer = DC.key_customer
INNER JOIN dbo.dim_sales DSA ON FB.key_sale = DSA.key_sale
INNER JOIN dbo.original_sources OS ON FB.key_source = OS.source_id

 */

CREATE OR REPLACE TABLE COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS
(
    transaction_id                         VARCHAR,
    booking_id                             VARCHAR,
    status                                 VARCHAR,
    key_date_booked                        DATE,
    commission_ex_vat                      FLOAT,
    booking_fee_net_rate                   FLOAT,
    insurance_net_rate                     FLOAT,
    insurance_net_rate_constant_currency   FLOAT,
    payment_surcharge_net_rate             FLOAT,
    margin                                 FLOAT,
    margin_constant_currency               FLOAT,
    margin_gross_of_toms                   FLOAT,
    margin_gross_of_toms_constant_currency FLOAT,
    margin_local_currency                  FLOAT,
    customer_id                            VARCHAR,
    sale_id                                VARCHAR,
    source_name                            VARCHAR,
    business_name                          VARCHAR
);

USE SCHEMA collab.muse_data_modelling;

put file:///Users/robin/sqls/cube_bookings_upload/Cube_Bookings_as_of_2020-02-14.csv @%CUBE_BOOKINGS;

copy into COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS
    file_format = (
        type = csv
            field_delimiter = ','
            skip_header = 1
            field_optionally_enclosed_by = '\"'
            record_delimiter = '\\n'
        );

GRANT SELECT ON TABLE COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS TO ROLE PERSONAL_ROLE__EGLEGRUBLYTE;

ALTER TABLE COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS ALTER CUSTOMER_ID SET DATA TYPE VARCHAR;

SELECT * FROM COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS;
