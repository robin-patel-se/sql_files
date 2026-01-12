--create a version of the netsuite report from CMS


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.cms_netsuite_report
(
    contractor                               VARCHAR,
    salesforceid                             VARCHAR,
    saleid                                   VARCHAR,
    salename                                 VARCHAR,
    offername                                VARCHAR,
    departureairportcode                     VARCHAR,
    adults                                   NUMBER,
    children                                 NUMBER,
    infants                                  NUMBER,
    datebooked                               VARCHAR,
    timebooked                               VARCHAR,
    checkin                                  VARCHAR,
    checkout                                 VARCHAR,
    startdate                                VARCHAR,
    enddate                                  VARCHAR,
    nonights                                 NUMBER,
    rooms                                    NUMBER,
    currency                                 VARCHAR,
    territory                                VARCHAR,
    grossbookingvalueincurrency              FLOAT,
    totalsellrateincurrency                  FLOAT,
    totalcustomtaxincurrency                 FLOAT,
    bookingfeeincurrency                     FLOAT,
    vatonbookingfeeincurrency                FLOAT,
    paymentsurchargeincurrency               FLOAT,
    vatonpaymentsurchargeincurrency          FLOAT,
    roomerincurrency                         FLOAT,
    totaltobepaidbyuser                      FLOAT,
    creditsusedincurrency                    FLOAT,
    noncashcredit                            FLOAT,
    totalreceivedfromuser                    FLOAT,
    paymenttype                              VARCHAR,
    type                                     VARCHAR,
    transactionid                            VARCHAR,
    bundleid                                 NUMBER,
    supplier                                 VARCHAR,
    countryofhotel                           VARCHAR,
    suppliercurrency                         VARCHAR,
    grossbookingvalueinsuppliercurrency      FLOAT,
    totalpriceinsuppliercurrency             FLOAT,
    commissionexvatinsuppliercurrency        FLOAT,
    vatoncommissioninsuppliercurrency        FLOAT,
    totalcustomtaxinsuppliercurrency         FLOAT,
    totalpayablewithtaxinsuppliercurrency    VARCHAR,
    salestartdate                            VARCHAR,
    saleenddate                              VARCHAR,
    providername                             VARCHAR,
    roomerpriceusd                           FLOAT,
    linetype                                 VARCHAR,
    refundmethod                             VARCHAR,
    flightbuyrate                            FLOAT,
    flightsellrate                           FLOAT,
    flightcommissionnetrate                  FLOAT,
    vatonflightcommission                    FLOAT,
    flightcommission                         FLOAT,
    baggagebuyrate                           FLOAT,
    baggagesellrate                          FLOAT,
    baggagecommissionnetrate                 FLOAT,
    vatonbaggagecommission                   FLOAT,
    baggagecommission                        FLOAT,
    outbounddeparturedate                    VARCHAR,
    inboundarrivaldate                       VARCHAR,
    carrier                                  VARCHAR,
    countryofcarrier                         VARCHAR,
    carriercurrency                          VARCHAR,
    atolfee                                  FLOAT,
    insurancename                            VARCHAR,
    insurancetype                            VARCHAR,
    insurancepolicy                          VARCHAR,
    insuranceincustomercurrency              FLOAT,
    insuranceinsuppliercurrency              FLOAT,
    netinsurancecommissioninsuppliercurrency FLOAT,
    dynamicflightbooked                      VARCHAR,
    flightinvoicenumber                      VARCHAR,
    creditnotenumber                         VARCHAR,
    uniquetransactionreference               VARCHAR,
    sfoppid                                  VARCHAR,
    whocoverscost                            VARCHAR,
    reason                                   VARCHAR,
    fault                                    VARCHAR,
    vccenabled                               BOOLEAN,
    vccreference                             VARCHAR,
    vcccreationmessage                       VARCHAR
);

USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/sql_files/travel_trust/netsuite/netSuite.csv @%cms_netsuite_report;

COPY INTO scratch.robinpatel.cms_netsuite_report
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT *
FROM scratch.robinpatel.cms_netsuite_report;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE raw_vault_mvp.cms_mongodb.booking_summary;
DROP TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;
self_describing_task --include 'staging/hygiene/cms_mongodb/booking_summary.py'  --method 'run' --start '2021-08-26 00:00:00' --end '2021-08-26 00:00:00'

--https://github.com/secretescapes/one-data-pipeline/pull/1980
------------------------------------------------------------------------------------------------------------------------
--found issue with booking_category
SELECT sb.transaction_id,
       sb.booking_category,
       sb.sale_product,
       sb.has_flights,
       sb.sale_type,
       sb.supplier_name
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
WHERE sb.booking_completed_date = '2021-08-25'
  AND sb.booking_status IN ('COMPLETE', 'REFUNDED') --including refunded as they will appear in the report before they cancel
ORDER BY transaction_id;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;

SELECT has_flights, COUNT(*)
FROM se.data.se_booking sb
GROUP BY 1;
SELECT bs.sale_type, COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--events that occurred on the 25th of August 2021
--line type helps differentiate between bookings and cancellations 'completed_booking', 'partial_refund' and 'full_refund'

SELECT cnr.contractor,
       cnr.salesforceid,
       cnr.saleid,
       cnr.salename,
       cnr.offername,
       cnr.departureairportcode,
       cnr.adults,
       cnr.children,
       cnr.infants,
       cnr.datebooked,
       cnr.timebooked,
       cnr.checkin,
       cnr.checkout,
       cnr.startdate,
       cnr.enddate,
       cnr.nonights,
       cnr.rooms,
       cnr.currency,
       cnr.territory,
       cnr.grossbookingvalueincurrency,
       cnr.totalsellrateincurrency,
       cnr.totalcustomtaxincurrency,
       cnr.bookingfeeincurrency,
       cnr.vatonbookingfeeincurrency,
       cnr.paymentsurchargeincurrency,
       cnr.vatonpaymentsurchargeincurrency,
       cnr.roomerincurrency,
       cnr.totaltobepaidbyuser,
       cnr.creditsusedincurrency,
       cnr.noncashcredit,
       cnr.totalreceivedfromuser,
       cnr.paymenttype,
       cnr.type,
       cnr.transactionid,
       cnr.bundleid,
       cnr.supplier,
       cnr.countryofhotel,
       cnr.suppliercurrency,
       cnr.grossbookingvalueinsuppliercurrency,
       cnr.totalpriceinsuppliercurrency,
       cnr.commissionexvatinsuppliercurrency,
       cnr.vatoncommissioninsuppliercurrency,
       cnr.totalcustomtaxinsuppliercurrency,
       cnr.totalpayablewithtaxinsuppliercurrency,
       cnr.salestartdate,
       cnr.saleenddate,
       cnr.providername,
       cnr.roomerpriceusd,
       cnr.linetype,
       cnr.refundmethod,
       cnr.flightbuyrate,
       cnr.flightsellrate,
       cnr.flightcommissionnetrate,
       cnr.vatonflightcommission,
       cnr.flightcommission,
       cnr.baggagebuyrate,
       cnr.baggagesellrate,
       cnr.baggagecommissionnetrate,
       cnr.vatonbaggagecommission,
       cnr.baggagecommission,
       cnr.outbounddeparturedate,
       cnr.inboundarrivaldate,
       cnr.carrier,
       cnr.countryofcarrier,
       cnr.carriercurrency,
       cnr.atolfee,
       cnr.insurancename,
       cnr.insurancetype,
       cnr.insurancepolicy,
       cnr.insuranceincustomercurrency,
       cnr.insuranceinsuppliercurrency,
       cnr.netinsurancecommissioninsuppliercurrency,
       cnr.dynamicflightbooked,
       cnr.flightinvoicenumber,
       cnr.creditnotenumber,
       cnr.uniquetransactionreference,
       cnr.sfoppid,
       cnr.whocoverscost,
       cnr.reason,
       cnr.fault,
       cnr.vccenabled,
       cnr.vccreference,
       cnr.vcccreationmessage
FROM scratch.robinpatel.cms_netsuite_report cnr;


------------------------------------------------------------------------------------------------------------------------
--replicate netsuite report for completed bookings first
--
-- WITH netsuite_bookings AS (
--     SELECT DISTINCT cnr.transactionid
--     FROM scratch.robinpatel.cms_netsuite_report cnr
--     WHERE cnr.linetype = 'completed_booking'
-- )
-- SELECT DISTINCT sb.booking_completed_date,
--        sb.booking§_status
-- FROM data_vault_mvp.dwh.se_booking sb
-- INNER JOIN netsuite_bookings nb ON sb.transaction_id = nb.transactionid

--dwh netsuite report confirmed bookings
CREATE OR REPLACE VIEW collab.travel_trust.netsuite_cms_report_booking AS
(
SELECT sb.booking_completed_date                                  AS view_date,
       ss.original_contractor_name,
       ss.salesforce_account_id,
       ss.se_sale_id,
       SPLIT_PART(ss.sale_name, ' | ', 1)                         AS sale_name,
       sb.offer_name,
       sb.departure_airport_code,
       sb.adult_guests,
       sb.child_guests,
       sb.infant_guests,
       sb.booking_completed_date,
       sb.booking_completed_timestamp,
       sb.cancellation_date,
       sb.check_in_date,
       sb.check_out_date,
       sb.outbound_flight_departure_date                          AS booking_start_date,
       sb.inbound_flight_arrival_date                             AS booking_end_date,
       sb.no_nights,
       sb.rooms,
       sb.currency,
       sb.territory,
       sb.gross_booking_value_cc,
       sb.total_sell_rate_cc,
       sb.total_custom_tax_cc,
       sb.booking_fee_cc,
       --vat on booking fee cc
       sb.payment_surcharge_cc,
       --vat on payment_surcharge
       sb.insurance_price_cc                                      AS roomer_cc,
       sb.total_sell_rate_cc
           + sb.booking_fee_cc
           + sb.payment_surcharge_cc
           + sb.total_custom_tax_sc
           + sb.insurance_price_sc
           + IFF(sb.sale_product IN ('IHP - Connected', 'IHP - dynamic'), sb.atol_fee_cc, 0)
                                                                  AS total_to_be_paid_by_user_cc, -- check this is correct
       sb.credits_used_cc,
       sb.non_cash_credits_used_cc,
       sb.total_received_from_user_cc,
       sb.payment_type,
       sb.booking_category                                        AS type,                        --type
       sb.transaction_id,
       sb.bundle_id,
       sb.supplier_name,
       ss.posu_country,
       sb.sale_base_currency,
       sb.gross_booking_value_sc,
       sb.customer_total_price_cc                                 AS total_price_sc,              -- check this is correct
       sb.commission_ex_vat_sc,
       sb.vat_on_commission_cc,
       sb.total_custom_tax_sc,
       sb.total_custom_tax_sc
           + (total_price_sc
           - (sb.commission_ex_vat_sc + sb.vat_on_commission_sc)) AS total_payable_with_tax_sc,
       ss.start_date                                              AS sale_start_date,
       ss.end_date                                                AS sale_end_date,
       --providername
       --roomerpriceusd
       'booking_completed'                                        AS line_type,
       NULL                                                       AS refund_method,               --this will be populated for refunds
       sb.flight_buy_rate_cc,
       sb.flight_only_price_cc                                    AS flight_sell_rate_cc,
       --flightcommissionnetrate
       sb.flight_commission_cc - sb.flight_vat_on_commission_cc   AS flght_commission_net_rate_cc,
       sb.flight_vat_on_commission_cc,
       sb.flight_commission_cc,
       -- baggagebuyrate
       -- baggagesellrate
       -- baggagecommissionnetrate
       -- vatonbaggagecommission
       -- baggagecommission
       sb.outbound_flight_departure_date,                                                         -- check if this is correct
       sb.inbound_flight_arrival_date,                                                            -- check if this is correct
       sb.flight_carrier,
       -- countryofcarrier
       -- carriercurrency
       sb.atol_fee_cc,
       sb.insurance_provider,
       -- insurancetype
       -- insurancepolicy
       sb.insurance_price_cc,
       sb.insurance_price_sc,
       sb.insurance_commission_sc                                 AS net_insurance_commission_sc,
       sb.has_flights,
       sb.flight_invoice_number,
       -- creditnotenumber
       sb.unique_transaction_reference,
       ss.salesforce_opportunity_id_full,                                                         --check this is correct
       NULL                                                       AS who_covers_cost,             --this will be populated for refunds
       NULL                                                       AS reason,                      --this will be populated for refunds
       NULL                                                       AS fault,                       --this will be populated for refunds
       --vccenabled
       sb.vcc_reference
       --vcccreationmessage
FROM data_vault_mvp.dwh.se_booking sb
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED') --including refunded as they will appear in the report before they cancel
    );


SELECT DISTINCT sale_type
FROM se.data.se_booking sb;
--cms netsuite report
SELECT *
FROM scratch.robinpatel.cms_netsuite_report cnr
WHERE cnr.linetype = 'completed_booking'
ORDER BY transactionid;

------------------------------------------------------------------------------------------------------------------------
--partial cancellations
--cms netsuite partial cancellations
SELECT *
FROM scratch.robinpatel.cms_netsuite_report cnr
WHERE cnr.linetype = 'partial_refund'
ORDER BY transactionid;

--dwh netsuite report partial cancellations
CREATE OR REPLACE VIEW collab.travel_trust.netsuite_cms_report_partial_canx AS
(
SELECT bc.date_created::DATE                                                         AS view_date,
       ss.original_contractor_name,
       ss.salesforce_account_id,
       ss.se_sale_id,
       SPLIT_PART(ss.sale_name, ' | ', 1)                                            AS sale_name,
       sb.offer_name,
       sb.departure_airport_code,
       sb.adult_guests,
       sb.child_guests,
       sb.infant_guests,
       sb.booking_completed_date,
       sb.booking_completed_timestamp,
       sb.cancellation_date,
       sb.check_in_date,
       sb.check_out_date,
       sb.outbound_flight_departure_date                                             AS booking_start_date,
       sb.inbound_flight_arrival_date                                                AS booking_end_date,
       sb.no_nights,
       sb.rooms,
       sb.currency,
       sb.territory,
       0                                                                             AS gross_booking_value_cc,
       (bc.hotel_good_will_cc + bc.se_good_will_cc)                                  AS total_sell_rate_calc,
       -total_sell_rate_calc                                                         AS total_sell_rate_cc,
       0                                                                             AS total_custom_tax_cc,
       -bc.booking_fee_cc                                                            AS booking_fee_cc,
       --vat on booking fee cc
       0                                                                             AS payment_surcharge_cc,
       --vat on payment_surcharge
       0                                                                             AS roomer_cc,
       - (total_sell_rate_calc
           + sb.booking_fee_cc
           + (bc.booking_fee_cc - sb.booking_fee_net_rate_cc) --vat on booking fee
           + sb.flight_only_price_cc
           -- + baggage sell rate, always 0
           + sb.atol_fee_cc)                                                         AS total_to_be_paid_by_user_cc,
       ---
       sb.credits_used_cc,
       sb.non_cash_credits_used_cc,
       sb.total_received_from_user_cc,
       sb.payment_type,
       sb.sale_product AS type,
       sb.transaction_id,
       sb.bundle_id,
       sb.supplier_name,
       ss.posu_country,
       sb.sale_base_currency,
       0                                                                             AS gross_booking_value_sc,
       CASE
           WHEN bc.who_pays = 'SE_COST' THEN 0
           WHEN bc.who_pays IN ('SHARED_COST', 'HOTEL_COST') AND bc.reason IN ('ROOMER_CHAPKA', 'FLIGHT_DUE_TO_DELAY_CHANGE_CANX') THEN 0
           WHEN bc.who_pays IN ('SHARED_COST', 'HOTEL_COST') THEN -(bc.hotel_good_will_cc * sb.cc_rate_to_sc)
           ELSE - sb.gross_booking_value_sc
           END                                                                       AS total_price_sc,
       CASE
           WHEN bc.who_pays = 'SHARED_COST' AND bc.se_good_will_cc = 0 THEN total_price_sc * COALESCE(bo.commission, o.commission, ss.commission, h.commission)
           ELSE 0
           END                                                                       AS commission_ex_vat_sc_calc,
       commission_ex_vat_sc_calc                                                     AS commission_ex_vat_sc,
       commission_ex_vat_sc_calc * 0.2                                               AS vat_on_commission_sc, -- need to check this
       0                                                                             AS total_custom_tax_sc,
       total_price_sc + commission_ex_vat_sc_calc + (sb.vat_on_commission_sc * 0.2)  AS total_payable_with_tax_sc,
       ss.start_date                                                                 AS sale_start_date,
       ss.end_date                                                                   AS sale_end_date,
       --providername
       --rooomerpriceusd
       'partial_refund'                                                              AS line_type,
       bc.refund_channel,
       sb.flight_only_price_cc
           - (sb.flight_commission_cc
           + sb.flight_vat_on_commission_cc) --flight_gross_commission
                                                                                     AS flight_buy_rate_cc,
       sb.flight_only_price_cc                                                       AS flight_sell_rate_cc,
       sb.flight_commission_cc - sb.flight_vat_on_commission_cc                      AS flight_commission_net_rate_cc,
       sb.flight_vat_on_commission_cc,
       sb.flight_commission_cc,
       -- baggagebuyrate
       -- baggagesellrate
       -- baggagecommissionnetrate
       -- vatonbaggagecommission
       -- baggagecommission
       sb.outbound_flight_departure_date,                                                                     -- check if this is correct
       sb.inbound_flight_arrival_date,                                                                        -- check if this is correct
       sb.flight_carrier,
       -- countryofcarrier
       -- carriercurrency
       sb.atol_fee_cc,
       sb.insurance_provider,
       -- insurancetype
       -- insurancepolicy
       IFF(bc.reason = 'ROOMER_CHAPKA', bc.hotel_good_will_cc, 0)                    AS insurance_cc,
       IFF(bc.reason = 'ROOMER_CHAPKA', bc.hotel_good_will_cc * sb.cc_rate_to_sc, 0) AS insurance_sc,
       0                                                                             AS net_insurance_commission_sc,
       sb.has_flights,
       sb.flight_invoice_number,
       --creditnotenumber
       sb.unique_transaction_reference,
       ss.salesforce_opportunity_id_full,
       bc.who_pays                                                                   AS who_covers_cost,
       bc.reason,
       bc.fault,
       --vccenabled
       sb.vcc_reference
       --vccreationmessage

FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
    INNER JOIN data_vault_mvp.dwh.se_booking sb ON bc.booking_id = sb.booking_id
    LEFT JOIN  data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
                   -- these joins are used to calculate commission_ex_vat_sc
    LEFT JOIN  hygiene_snapshot_vault_mvp.cms_mysql.base_offer bo ON sb.offer_id = 'A' || bo.id AND LEFT(sb.offer_id, 1) = 'A'
    LEFT JOIN  hygiene_snapshot_vault_mvp.cms_mysql.offer o ON sb.offer_id = o.id::VARCHAR AND LEFT(sb.offer_id, 1) IS DISTINCT FROM 'A'
    LEFT JOIN  hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON ss.hotel_id = h.id
WHERE bc.refund_type IN ('PARTIAL', 'WP_REFUND')
    );


------------------------------------------------------------------------------------------------------------------------
--full cancellations
--cms netsuite full cancellations

SELECT *
FROM scratch.robinpatel.cms_netsuite_report cnr
WHERE cnr.linetype = 'full_refund'
ORDER BY transactionid;


--dwh netsuite report full cancellations
CREATE OR REPLACE VIEW collab.travel_trust.netsuite_cms_report_full_canx AS
(
SELECT bc.date_created::DATE                                                         AS view_date,
       ss.original_contractor_name,
       ss.salesforce_account_id,
       ss.se_sale_id,
       SPLIT_PART(ss.sale_name, ' | ', 1)                                            AS sale_name,
       sb.offer_name,
       sb.departure_airport_code,
       sb.adult_guests,
       sb.child_guests,
       sb.infant_guests,
       sb.booking_completed_date,
       sb.booking_completed_timestamp,
       sb.cancellation_date,
       sb.check_in_date,
       sb.check_out_date,
       sb.outbound_flight_departure_date                                             AS booking_start_date,
       sb.inbound_flight_arrival_date                                                AS booking_end_date,
       sb.no_nights,
       sb.rooms,
       sb.currency,
       sb.territory,
       -sb.gross_booking_value_cc                                                    AS gross_booking_value_cc,
       -sb.total_sell_rate_cc                                                        AS total_sell_rate_cc,
       0                                                                             AS total_custom_tax_cc,
       -sb.booking_fee_cc                                                            AS booking_fee_cc,
       --vat on booking fee cc
       -sb.payment_surcharge_cc                                                      AS payment_surcharge_cc,
       --vat on payment_surcharge
       0                                                                             AS roomer_cc,
       -(sb.total_sell_rate_cc
           + sb.booking_fee_cc
           + sb.payment_surcharge_cc
           + sb.total_custom_tax_sc
           + sb.insurance_price_sc
           + IFF(sb.sale_product IN ('IHP - Connected', 'IHP - dynamic'),
                 sb.atol_fee_cc, 0))
                                                                                     AS total_to_be_paid_by_user_cc,
       -sb.credits_used_cc                                                           AS credits_used_cc,
       -sb.non_cash_credits_used_cc                                                  AS non_cash_credits_used_cc,
       -sb.total_received_from_user_cc                                               AS total_received_from_user_cc,
       sb.payment_type,
       sb.booking_category                                                           AS type,
       sb.transaction_id,
       sb.bundle_id,
       sb.supplier_name,
       ss.posu_country,
       sb.sale_base_currency,
       -sb.gross_booking_value_sc                                                    AS gross_booking_value_sc,
       -sb.customer_total_price_sc                                                   AS total_price_sc, -- check this is correct
       -sb.commission_ex_vat_sc                                                      AS commission_ex_vat_sc,
       -sb.vat_on_commission_sc                                                      AS vat_on_commission_sc,
       -sb.total_custom_tax_sc                                                       AS total_custom_tax_sc,
       -(sb.total_custom_tax_sc
           + (sb.customer_total_price_cc
               - (sb.commission_ex_vat_sc + sb.vat_on_commission_sc)))               AS total_payable_with_tax_sc,
       ss.start_date                                                                 AS sale_start_date,
       ss.end_date                                                                   AS sale_end_date,
       --providername
       --rooomerpriceusd
       'full_refund'                                                                 AS line_type,
       bc.refund_channel,
       sb.flight_only_price_cc
           - (sb.flight_commission_cc
           + sb.flight_vat_on_commission_cc) --flight_gross_commission
                                                                                     AS flight_buy_rate_cc,
       sb.flight_only_price_cc                                                       AS flight_sell_rate_cc,
       sb.flight_commission_cc - sb.flight_vat_on_commission_cc                      AS flight_commission_net_rate_cc,
       sb.flight_vat_on_commission_cc,
       sb.flight_commission_cc,
       -- baggagebuyrate
       -- baggagesellrate
       -- baggagecommissionnetrate
       -- vatonbaggagecommission
       -- baggagecommission
       sb.outbound_flight_departure_date,                                                               -- check if this is correct
       sb.inbound_flight_arrival_date,                                                                  -- check if this is correct
       sb.flight_carrier,
       -- countryofcarrier
       -- carriercurrency
       sb.atol_fee_cc,
       sb.insurance_provider,
       -- insurancetype
       -- insurancepolicy
       IFF(bc.reason = 'ROOMER_CHAPKA', bc.hotel_good_will_cc, 0)                    AS insurance_cc,
       IFF(bc.reason = 'ROOMER_CHAPKA', bc.hotel_good_will_cc * sb.cc_rate_to_sc, 0) AS insurance_sc,
       IFF(sb.insurance_provider = 'ROOMER',
           sb.insurance_commission_sc,
           sb.insurance_price_sc - sb.insurance_commission_sc)                       AS net_insurance_commission_sc,
       sb.has_flights,
       sb.flight_invoice_number,
       --creditnotenumber
       sb.unique_transaction_reference,
       ss.salesforce_opportunity_id_full,
       bc.who_pays                                                                   AS who_covers_cost,
       bc.reason,
       bc.fault,
       --vccenabled
       sb.vcc_reference
       --vccreationmessage
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
    INNER JOIN data_vault_mvp.dwh.se_booking sb ON bc.booking_id = sb.booking_id
    LEFT JOIN  data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id

WHERE bc.refund_type = 'FULL'
    );


CREATE OR REPLACE VIEW collab.travel_trust.netsuite_cms_report AS
(
SELECT b.view_date,
       b.original_contractor_name,
       b.salesforce_account_id,
       b.se_sale_id,
       b.sale_name,
       b.offer_name,
       b.departure_airport_code,
       b.adult_guests,
       b.child_guests,
       b.infant_guests,
       b.booking_completed_date,
       b.booking_completed_timestamp,
       b.cancellation_date,
       b.check_in_date,
       b.check_out_date,
       b.booking_start_date,
       b.booking_end_date,
       b.no_nights,
       b.rooms,
       b.currency,
       b.territory,
       b.gross_booking_value_cc,
       b.total_sell_rate_cc,
       b.total_custom_tax_cc,
       b.booking_fee_cc,
       b.payment_surcharge_cc,
       b.roomer_cc,
       b.total_to_be_paid_by_user_cc,
       b.credits_used_cc,
       b.non_cash_credits_used_cc,
       b.total_received_from_user_cc,
       b.payment_type,
       b.type,
       b.transaction_id,
       b.bundle_id,
       b.supplier_name,
       b.posu_country,
       b.sale_base_currency,
       b.gross_booking_value_sc,
       b.total_price_sc,
       b.commission_ex_vat_sc,
       b.vat_on_commission_cc,
       b.total_custom_tax_sc,
       b.total_payable_with_tax_sc,
       b.sale_start_date,
       b.sale_end_date,
       b.line_type,
       b.refund_method,
       b.flight_buy_rate_cc,
       b.flight_sell_rate_cc,
       b.flght_commission_net_rate_cc,
       b.flight_vat_on_commission_cc,
       b.flight_commission_cc,
       b.outbound_flight_departure_date,
       b.inbound_flight_arrival_date,
       b.flight_carrier,
       b.atol_fee_cc,
       b.insurance_provider,
       b.insurance_price_cc,
       b.insurance_price_sc,
       b.net_insurance_commission_sc,
       b.has_flights,
       b.flight_invoice_number,
       b.unique_transaction_reference,
       b.salesforce_opportunity_id_full,
       b.who_covers_cost,
       b.reason,
       b.fault,
       b.vcc_reference
FROM collab.travel_trust.netsuite_cms_report_booking b
UNION ALL
SELECT pc.view_date,
       pc.original_contractor_name,
       pc.salesforce_account_id,
       pc.se_sale_id,
       pc.sale_name,
       pc.offer_name,
       pc.departure_airport_code,
       pc.adult_guests,
       pc.child_guests,
       pc.infant_guests,
       pc.booking_completed_date,
       pc.booking_completed_timestamp,
       pc.cancellation_date,
       pc.check_in_date,
       pc.check_out_date,
       pc.booking_start_date,
       pc.booking_end_date,
       pc.no_nights,
       pc.rooms,
       pc.currency,
       pc.territory,
       pc.gross_booking_value_cc,
--        pc.total_sell_rate_calc,
       pc.total_sell_rate_cc,
       pc.total_custom_tax_cc,
       pc.booking_fee_cc,
       pc.payment_surcharge_cc,
       pc.roomer_cc,
       pc.total_to_be_paid_by_user_cc,
       pc.credits_used_cc,
       pc.non_cash_credits_used_cc,
       pc.total_received_from_user_cc,
       pc.payment_type,
       pc.type,
       pc.transaction_id,
       pc.bundle_id,
       pc.supplier_name,
       pc.posu_country,
       pc.sale_base_currency,
       pc.gross_booking_value_sc,
       pc.total_price_sc,
--        pc.commission_ex_vat_sc_calc,
       pc.commission_ex_vat_sc,
       pc.vat_on_commission_sc,
       pc.total_custom_tax_sc,
       pc.total_payable_with_tax_sc,
       pc.sale_start_date,
       pc.sale_end_date,
       pc.line_type,
       pc.refund_channel,
       pc.flight_buy_rate_cc,
       pc.flight_sell_rate_cc,
       pc.flight_commission_net_rate_cc,
       pc.flight_vat_on_commission_cc,
       pc.flight_commission_cc,
       pc.outbound_flight_departure_date,
       pc.inbound_flight_arrival_date,
       pc.flight_carrier,
       pc.atol_fee_cc,
       pc.insurance_provider,
       pc.insurance_cc,
       pc.insurance_sc,
       pc.net_insurance_commission_sc,
       pc.has_flights,
       pc.flight_invoice_number,
       pc.unique_transaction_reference,
       pc.salesforce_opportunity_id_full,
       pc.who_covers_cost,
       pc.reason,
       pc.fault,
       pc.vcc_reference
FROM collab.travel_trust.netsuite_cms_report_partial_canx pc
UNION ALL
SELECT fc.view_date,
       fc.original_contractor_name,
       fc.salesforce_account_id,
       fc.se_sale_id,
       fc.sale_name,
       fc.offer_name,
       fc.departure_airport_code,
       fc.adult_guests,
       fc.child_guests,
       fc.infant_guests,
       fc.booking_completed_date,
       fc.booking_completed_timestamp,
       fc.cancellation_date,
       fc.check_in_date,
       fc.check_out_date,
       fc.booking_start_date,
       fc.booking_end_date,
       fc.no_nights,
       fc.rooms,
       fc.currency,
       fc.territory,
       fc.gross_booking_value_cc,
       fc.total_sell_rate_cc,
       fc.total_custom_tax_cc,
       fc.booking_fee_cc,
       fc.payment_surcharge_cc,
       fc.roomer_cc,
       fc.total_to_be_paid_by_user_cc,
       fc.credits_used_cc,
       fc.non_cash_credits_used_cc,
       fc.total_received_from_user_cc,
       fc.payment_type,
       fc.type,
       fc.transaction_id,
       fc.bundle_id,
       fc.supplier_name,
       fc.posu_country,
       fc.sale_base_currency,
       fc.gross_booking_value_sc,
       fc.total_price_sc,
       fc.commission_ex_vat_sc,
       fc.vat_on_commission_sc,
       fc.total_custom_tax_sc,
       fc.total_payable_with_tax_sc,
       fc.sale_start_date,
       fc.sale_end_date,
       fc.line_type,
       fc.refund_channel,
       fc.flight_buy_rate_cc,
       fc.flight_sell_rate_cc,
       fc.flight_commission_net_rate_cc,
       fc.flight_vat_on_commission_cc,
       fc.flight_commission_cc,
       fc.outbound_flight_departure_date,
       fc.inbound_flight_arrival_date,
       fc.flight_carrier,
       fc.atol_fee_cc,
       fc.insurance_provider,
       fc.insurance_cc,
       fc.insurance_sc,
       fc.net_insurance_commission_sc,
       fc.has_flights,
       fc.flight_invoice_number,
       fc.unique_transaction_reference,
       fc.salesforce_opportunity_id_full,
       fc.who_covers_cost,
       fc.reason,
       fc.fault,
       fc.vcc_reference
FROM collab.travel_trust.netsuite_cms_report_full_canx fc
    );


SELECT * FROM collab.travel_trust.netsuite_cms_report ncr WHERE ncr.view_date = current_date -1;
GRANT SELECT ON TABLE collab.travel_trust.netsuite_cms_report TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON TABLE collab.travel_trust.netsuite_cms_report TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.travel_trust.netsuite_cms_report TO ROLE personal_role__sebastianmaczka;

SELECT * FROM se.data.se_booking sb;