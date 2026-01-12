SubscriberKey,"LastOpenDate","LastClickDate","LastSPVDate","LastPurchaseDate"

1000347,"Oct 15 2021  3:05PM","","Oct 21 2021  7:54PM",""
10003495,"Oct 24 2021  9:09AM","","",""
10003503,"Oct  7 2021 12:40PM","","",""
10003521,"Oct 12 2021  3:46AM","Oct  2 2021  6:40AM","Oct  2 2021 12:40PM",""
10003523,"Oct 20 2021 12:46AM","Oct  4 2021 12:47AM","Oct  4 2021  6:47AM",""
10003528,"Oct 19 2021 12:25PM","","",""
10003534,"Oct 25 2021 12:29AM","","",""
10003536,"Oct 22 2021 12:07AM","","",""
10003546,"Oct 17 2021  4:50AM","","",""
10003549,"Oct 22 2021  6:52AM","","",""
10003550,"Oct 18 2021  6:28PM","","",""
10003560,"Oct 17 2021  2:50AM","","",""
10003575,"Oct 20 2021  2:24AM","Oct  2 2021  3:35AM","",""
10003584,"Oct 25 2021  1:49AM","","",""
10003617,"","","Oct  7 2021  8:43PM",""
10003620,"Oct 24 2021  5:03PM","","",""
10003625,"Oct 17 2021  2:12AM","","",""




--upload cube export from chiasma sql server to collab space
USE WAREHOUSE pipe_large;
USE SCHEMA collab.muse_data_modelling;

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

-- archive.sfmc

CREATE OR REPLACE TABLE scratch.robinpatel.user_activity
(
    subscriberkey    VARCHAR,
    lastopendate     VARCHAR,
    lastclickdate    VARCHAR,
    lastspvdate      VARCHAR,
    lastpurchasedate VARCHAR
);

USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/sql_files/Iterable/sfmc_activity_validation.csv @%user_activity;


COPY INTO scratch.robinpatel.user_activity
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        );



SELECT TRY_TO_NUMBER(sfmc_ua.subscriberkey)                                                       AS subscriber_key,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastopendate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy')     AS last_open_date,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastclickdate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy')    AS last_click_date,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastspvdate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy')      AS last_spv_date,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastpurchasedate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy') AS last_purchase_date,
       sfmc_ua.subscriberkey                                                                      AS subscriberkey__o,
       sfmc_ua.lastopendate                                                                       AS lastopendate__o,
       sfmc_ua.lastclickdate                                                                      AS lastclickdate__o,
       sfmc_ua.lastspvdate                                                                        AS lastspvdate__o,
       sfmc_ua.lastpurchasedate                                                                   AS lastpurchasedate__o
FROM scratch.robinpatel.user_activity sfmc_ua;

------------------------------------------------------------------------------------------------------------------------
--run in pipeline runner

CREATE OR REPLACE TABLE archive.sfmc.user_activity
(
    subscriberkey       NUMBER PRIMARY KEY NOT NULL,
    last_open_date      DATE,
    last_click_date     DATE,
    last_spv_date       DATE,
    last_purchase_date  DATE,
    lastopendate__o     VARCHAR,
    lastclickdate__o    VARCHAR,
    lastspvdate__o      VARCHAR,
    lastpurchasedate__o VARCHAR

);


INSERT INTO archive.sfmc.user_activity
SELECT sfmc_ua.subscriberkey,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastopendate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy')     AS last_open_date,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastclickdate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy')    AS last_click_date,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastspvdate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy')      AS last_spv_date,
       TRY_TO_DATE(REGEXP_SUBSTR(sfmc_ua.lastpurchasedate, '(.*)  .*', 1, 1, 'e'), 'mon dd yyyy') AS last_purchase_date,
       sfmc_ua.lastopendate                                                                       AS lastopendate__o,
       sfmc_ua.lastclickdate                                                                      AS lastclickdate__o,
       sfmc_ua.lastspvdate                                                                        AS lastspvdate__o,
       sfmc_ua.lastpurchasedate                                                                   AS lastpurchasedate__o
FROM scratch.robinpatel.user_activity sfmc_ua;

SELECT *
FROM archive.sfmc.user_activity;

