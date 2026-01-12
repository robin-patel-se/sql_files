SELECT *
FROM se.data.se_booking_summary_extended
WHERE checkout > current_date() - 7
  AND checkout < current_date()
  AND datebooked > '2020-06-01'
  AND refunded = FALSE
  AND cancelled = FALSE;



SET current_date = current_date;
SELECT $current_date AS processed_date,
       sbse.salename,
       sbse.type,
       sbse.company,
       sbse.supplier,
       sbse.country,
       sbse.division,
       sbse.city,
       sbse.providername,
       sbse.customeremail,
       sbse.contractor,
       sbse.saleid,
       sbse.offername,
       sbse.departureairportcode,
       sbse.departureairportname,
       sbse.adults,
       sbse.children,
       sbse.infants,
       sbse.county,
       sbse.customername,
       sbse.affiliate,
       sbse.originalacquiringaffiliate,
       sbse.datebooked,
       sbse.datetimebooked,
       sbse.timebooked,
       sbse.checkin,
       sbse.checkout,
       sbse.nonights,
       sbse.rooms,
       sbse.currency,
       sbse.territory,
       sbse.totalsellrateincurrency,
       sbse.ratetogbp,
       sbse.totalsellrate,
       sbse.commissionexvat,
       sbse.vatoncommission,
       sbse.grosscommission,
       sbse.totalnetrate,
       sbse.customertotalprice,
       sbse.customerpayment,
       sbse.creditsused,
       sbse.creditamountdeductiblefromcommission,
       sbse.bookingfeenetrate,
       sbse.vatonbookingfee,
       sbse.bookingfee,
       sbse.paymenttype,
       sbse.paymentsurchargenetrate,
       sbse.vatonpaymentsurcharge,
       sbse.paymentsurcharge,
       sbse.transactionid,
       sbse.topdiscount,
       sbse.totalroomnights,
       sbse.impulse,
       sbse.notes,
       sbse.userjoindate,
       sbse.grossprofit,
       sbse.salestartdate,
       sbse.saleenddate,
       sbse.destinationname,
       sbse.destinationtype,
       sbse.week,
       sbse.month,
       sbse.postcode,
       sbse.citydistrict,
       sbse.totalcustomtax,
       sbse.platformname,
       sbse.appdownloaddate,
       sbse.adxnetwork,
       sbse.adxcreative,
       sbse.useracquisitionplatform,
       sbse.grossbookingvalueincurrency,
       sbse.grossbookingvalue,
       sbse.customerid,
       sbse.numberofflashnights,
       sbse.numberofbackfillednights,
       sbse.flashgrosscommissioninsuppliercurrency,
       sbse.backfillgrosscommissioninsuppliercurrency,
       sbse.usercountry,
       sbse.userstate,
       sbse.bundleid,
       sbse.saledimension,
       sbse.dynamicflightbooked,
       sbse.arrivalairport,
       sbse.flightbuyrate,
       sbse.flightsellrate,
       sbse.carrier,
       sbse.flightcommission,
       sbse.numberofbags,
       sbse.baggagesellrate,
       sbse.atolfee,
       sbse.uniquetransactionreference,
       sbse.insurancename,
       sbse.insurancetype,
       sbse.insurancepolicy,
       sbse.insuranceinsuppliercurrency,
       sbse.insuranceincustomercurrency,
       sbse.netinsurancecommissionincustomercurrency,
       sbse.agentid,
       sbse.booking_id,
       sbse.dwh_booking_id,
       sbse.lifetime_bookings,
       sbse.lifetime_margin,
       sbse.bookings_less_13m,
       sbse.bookings_more_13m,
       sbse.booker_segment,
       sbse.cancelled,
       sbse.refunded
FROM se.data.se_booking_summary_extended sbse
WHERE checkout > $current_date - 7
  AND checkout < $current_date
  AND datebooked > '2020-06-01'
  AND refunded = FALSE
  AND cancelled = FALSE;

------------------------------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS collab.refund_credits;
CREATE OR REPLACE TABLE refund_credits.processed_bookings
(
    processed_date                            DATE,
    salename                                  VARCHAR,
    type                                      VARCHAR,
    company                                   VARCHAR,
    supplier                                  VARCHAR,
    country                                   VARCHAR,
    division                                  VARCHAR,
    city                                      VARCHAR,
    providername                              VARCHAR,
    customeremail                             VARCHAR,
    contractor                                VARCHAR,
    saleid                                    VARCHAR,
    offername                                 VARCHAR,
    departureairportcode                      VARCHAR,
    departureairportname                      VARCHAR,
    adults                                    NUMBER,
    children                                  NUMBER,
    infants                                   NUMBER,
    county                                    VARCHAR,
    customername                              VARCHAR,
    affiliate                                 VARCHAR,
    originalacquiringaffiliate                VARCHAR,
    datebooked                                DATE,
    datetimebooked                            TIMESTAMP,
    timebooked                                VARCHAR,
    checkin                                   DATE,
    checkout                                  DATE,
    nonights                                  NUMBER,
    rooms                                     NUMBER,
    currency                                  VARCHAR,
    territory                                 VARCHAR,
    totalsellrateincurrency                   DOUBLE,
    ratetogbp                                 DOUBLE,
    totalsellrate                             DOUBLE,
    commissionexvat                           DOUBLE,
    vatoncommission                           DOUBLE,
    grosscommission                           DOUBLE,
    totalnetrate                              DOUBLE,
    customertotalprice                        DOUBLE,
    customerpayment                           DOUBLE,
    creditsused                               DOUBLE,
    creditamountdeductiblefromcommission      DOUBLE,
    bookingfeenetrate                         DOUBLE,
    vatonbookingfee                           DOUBLE,
    bookingfee                                DOUBLE,
    paymenttype                               VARCHAR,
    paymentsurchargenetrate                   DOUBLE,
    vatonpaymentsurcharge                     DOUBLE,
    paymentsurcharge                          DOUBLE,
    transactionid                             VARCHAR,
    topdiscount                               VARCHAR,
    totalroomnights                           NUMBER,
    impulse                                   VARCHAR,
    notes                                     VARCHAR,
    userjoindate                              TIMESTAMP,
    grossprofit                               DOUBLE,
    salestartdate                             DATE,
    saleenddate                               DATE,
    destinationname                           VARCHAR,
    destinationtype                           VARCHAR,
    week                                      NUMBER,
    month                                     NUMBER,
    postcode                                  VARCHAR,
    citydistrict                              VARCHAR,
    totalcustomtax                            VARCHAR,
    platformname                              VARCHAR,
    appdownloaddate                           VARCHAR,
    adxnetwork                                VARCHAR,
    adxcreative                               VARCHAR,
    useracquisitionplatform                   VARCHAR,
    grossbookingvalueincurrency               DOUBLE,
    grossbookingvalue                         DOUBLE,
    customerid                                VARCHAR,
    numberofflashnights                       NUMBER,
    numberofbackfillednights                  NUMBER,
    flashgrosscommissioninsuppliercurrency    DOUBLE,
    backfillgrosscommissioninsuppliercurrency DOUBLE,
    usercountry                               VARCHAR,
    userstate                                 VARCHAR,
    bundleid                                  VARCHAR,
    saledimension                             VARCHAR,
    dynamicflightbooked                       VARCHAR,
    arrivalairport                            VARCHAR,
    flightbuyrate                             DOUBLE,
    flightsellrate                            DOUBLE,
    carrier                                   VARCHAR,
    flightcommission                          DOUBLE,
    numberofbags                              NUMBER,
    baggagesellrate                           DOUBLE,
    atolfee                                   DOUBLE,
    uniquetransactionreference                VARCHAR,
    insurancename                             VARCHAR,
    insurancetype                             VARCHAR,
    insurancepolicy                           VARCHAR,
    insuranceinsuppliercurrency               DOUBLE,
    insuranceincustomercurrency               DOUBLE,
    netinsurancecommissionincustomercurrency  DOUBLE,
    agentid                                   VARCHAR,
    booking_id                                VARCHAR,
    dwh_booking_id                            VARCHAR,
    lifetime_bookings                         NUMBER,
    lifetime_margin                           DOUBLE,
    bookings_less_13m                         NUMBER,
    bookings_more_13m                         NUMBER,
    booker_segment                            VARCHAR,
    cancelled                                 BOOLEAN,
    refunded                                  BOOLEAN,
    user_current_territory                    VARCHAR
);

MERGE INTO collab.refund_credits.processed_bookings_test target
    USING (
        SELECT current_date AS processed_date,
               sbse.salename,
               sbse.type,
               sbse.company,
               sbse.supplier,
               sbse.country,
               sbse.division,
               sbse.city,
               sbse.providername,
               sbse.customeremail,
               sbse.contractor,
               sbse.saleid,
               sbse.offername,
               sbse.departureairportcode,
               sbse.departureairportname,
               sbse.adults,
               sbse.children,
               sbse.infants,
               sbse.county,
               sbse.customername,
               sbse.affiliate,
               sbse.originalacquiringaffiliate,
               sbse.datebooked,
               sbse.datetimebooked,
               sbse.timebooked,
               sbse.checkin,
               sbse.checkout,
               sbse.nonights,
               sbse.rooms,
               sbse.currency,
               sbse.territory,
               sbse.totalsellrateincurrency,
               sbse.ratetogbp,
               sbse.totalsellrate,
               sbse.commissionexvat,
               sbse.vatoncommission,
               sbse.grosscommission,
               sbse.totalnetrate,
               sbse.customertotalprice,
               sbse.customerpayment,
               sbse.creditsused,
               sbse.creditamountdeductiblefromcommission,
               sbse.bookingfeenetrate,
               sbse.vatonbookingfee,
               sbse.bookingfee,
               sbse.paymenttype,
               sbse.paymentsurchargenetrate,
               sbse.vatonpaymentsurcharge,
               sbse.paymentsurcharge,
               sbse.transactionid,
               sbse.topdiscount,
               sbse.totalroomnights,
               sbse.impulse,
               sbse.notes,
               sbse.userjoindate,
               sbse.grossprofit,
               sbse.salestartdate,
               sbse.saleenddate,
               sbse.destinationname,
               sbse.destinationtype,
               sbse.week,
               sbse.month,
               sbse.postcode,
               sbse.citydistrict,
               sbse.totalcustomtax,
               sbse.platformname,
               sbse.appdownloaddate,
               sbse.adxnetwork,
               sbse.adxcreative,
               sbse.useracquisitionplatform,
               sbse.grossbookingvalueincurrency,
               sbse.grossbookingvalue,
               sbse.customerid,
               sbse.numberofflashnights,
               sbse.numberofbackfillednights,
               sbse.flashgrosscommissioninsuppliercurrency,
               sbse.backfillgrosscommissioninsuppliercurrency,
               sbse.usercountry,
               sbse.userstate,
               sbse.bundleid,
               sbse.saledimension,
               sbse.dynamicflightbooked,
               sbse.arrivalairport,
               sbse.flightbuyrate,
               sbse.flightsellrate,
               sbse.carrier,
               sbse.flightcommission,
               sbse.numberofbags,
               sbse.baggagesellrate,
               sbse.atolfee,
               sbse.uniquetransactionreference,
               sbse.insurancename,
               sbse.insurancetype,
               sbse.insurancepolicy,
               sbse.insuranceinsuppliercurrency,
               sbse.insuranceincustomercurrency,
               sbse.netinsurancecommissionincustomercurrency,
               sbse.agentid,
               sbse.booking_id,
               sbse.dwh_booking_id,
               sbse.lifetime_bookings,
               sbse.lifetime_margin,
               sbse.bookings_less_13m,
               sbse.bookings_more_13m,
               sbse.booker_segment,
               sbse.cancelled,
               sbse.refunded,
               sua.current_affiliate_territory
        FROM se.data.se_booking_summary_extended sbse
                 LEFT JOIN se.data.se_user_attributes sua ON sbse.customerid::VARCHAR = sua.shiro_user_id::VARCHAR
        WHERE sbse.checkout >= '2020-06-08'
          AND sbse.checkout < current_date
          AND sbse.checkout <= '2020-12-31'
          AND sbse.datebooked >= '2020-06-08'
          AND sbse.datebooked < '2020-09-01'
          AND sbse.refunded = FALSE
          AND sbse.cancelled = FALSE
          AND sua.current_affiliate_territory IN ('UK', 'DE')
          --add check prod for user ids
          AND sbse.customerid::VARCHAR NOT IN (
            SELECT DISTINCT customerid::VARCHAR
            FROM collab.refund_credits.processed_bookings
        )
    ) AS batch
    ON target.booking_id = batch.booking_id
    WHEN MATCHED AND (target.cancelled != batch.cancelled
        OR target.refunded != batch.refunded)
        THEN UPDATE SET
        target.refunded = batch.refunded,
        target.cancelled = batch.cancelled
    WHEN NOT MATCHED
        THEN INSERT VALUES (batch.processed_date,
                            batch.salename,
                            batch.type,
                            batch.company,
                            batch.supplier,
                            batch.country,
                            batch.division,
                            batch.city,
                            batch.providername,
                            batch.customeremail,
                            batch.contractor,
                            batch.saleid,
                            batch.offername,
                            batch.departureairportcode,
                            batch.departureairportname,
                            batch.adults,
                            batch.children,
                            batch.infants,
                            batch.county,
                            batch.customername,
                            batch.affiliate,
                            batch.originalacquiringaffiliate,
                            batch.datebooked,
                            batch.datetimebooked,
                            batch.timebooked,
                            batch.checkin,
                            batch.checkout,
                            batch.nonights,
                            batch.rooms,
                            batch.currency,
                            batch.territory,
                            batch.totalsellrateincurrency,
                            batch.ratetogbp,
                            batch.totalsellrate,
                            batch.commissionexvat,
                            batch.vatoncommission,
                            batch.grosscommission,
                            batch.totalnetrate,
                            batch.customertotalprice,
                            batch.customerpayment,
                            batch.creditsused,
                            batch.creditamountdeductiblefromcommission,
                            batch.bookingfeenetrate,
                            batch.vatonbookingfee,
                            batch.bookingfee,
                            batch.paymenttype,
                            batch.paymentsurchargenetrate,
                            batch.vatonpaymentsurcharge,
                            batch.paymentsurcharge,
                            batch.transactionid,
                            batch.topdiscount,
                            batch.totalroomnights,
                            batch.impulse,
                            batch.notes,
                            batch.userjoindate,
                            batch.grossprofit,
                            batch.salestartdate,
                            batch.saleenddate,
                            batch.destinationname,
                            batch.destinationtype,
                            batch.week,
                            batch.month,
                            batch.postcode,
                            batch.citydistrict,
                            batch.totalcustomtax,
                            batch.platformname,
                            batch.appdownloaddate,
                            batch.adxnetwork,
                            batch.adxcreative,
                            batch.useracquisitionplatform,
                            batch.grossbookingvalueincurrency,
                            batch.grossbookingvalue,
                            batch.customerid,
                            batch.numberofflashnights,
                            batch.numberofbackfillednights,
                            batch.flashgrosscommissioninsuppliercurrency,
                            batch.backfillgrosscommissioninsuppliercurrency,
                            batch.usercountry,
                            batch.userstate,
                            batch.bundleid,
                            batch.saledimension,
                            batch.dynamicflightbooked,
                            batch.arrivalairport,
                            batch.flightbuyrate,
                            batch.flightsellrate,
                            batch.carrier,
                            batch.flightcommission,
                            batch.numberofbags,
                            batch.baggagesellrate,
                            batch.atolfee,
                            batch.uniquetransactionreference,
                            batch.insurancename,
                            batch.insurancetype,
                            batch.insurancepolicy,
                            batch.insuranceinsuppliercurrency,
                            batch.insuranceincustomercurrency,
                            batch.netinsurancecommissionincustomercurrency,
                            batch.agentid,
                            batch.booking_id,
                            batch.dwh_booking_id,
                            batch.lifetime_bookings,
                            batch.lifetime_margin,
                            batch.bookings_less_13m,
                            batch.bookings_more_13m,
                            batch.booker_segment,
                            batch.cancelled,
                            batch.refunded,
                            batch.current_affiliate_territory);

CREATE OR REPLACE TABLE collab.refund_credits.processed_bookings_test CLONE collab.refund_credits.processed_bookings;


SELECT *
FROM processed_bookings pb;
UPDATE collab.refund_credits.processed_bookings pb
SET pb.processed_date = '2020-06-01';

DELETE
FROM processed_bookings pb
WHERE pb.city = 'Munich';

CREATE OR REPLACE PROCEDURE collab.refund_credits.insert_new_data_procedure()
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT AS
$$

        var sql_command = `
        MERGE INTO collab.refund_credits.processed_bookings target
        USING (
        SELECT current_date AS processed_date,
               sbse.salename,
               sbse.type,
               sbse.company,
               sbse.supplier,
               sbse.country,
               sbse.division,
               sbse.city,
               sbse.providername,
               sbse.customeremail,
               sbse.contractor,
               sbse.saleid,
               sbse.offername,
               sbse.departureairportcode,
               sbse.departureairportname,
               sbse.adults,
               sbse.children,
               sbse.infants,
               sbse.county,
               sbse.customername,
               sbse.affiliate,
               sbse.originalacquiringaffiliate,
               sbse.datebooked,
               sbse.datetimebooked,
               sbse.timebooked,
               sbse.checkin,
               sbse.checkout,
               sbse.nonights,
               sbse.rooms,
               sbse.currency,
               sbse.territory,
               sbse.totalsellrateincurrency,
               sbse.ratetogbp,
               sbse.totalsellrate,
               sbse.commissionexvat,
               sbse.vatoncommission,
               sbse.grosscommission,
               sbse.totalnetrate,
               sbse.customertotalprice,
               sbse.customerpayment,
               sbse.creditsused,
               sbse.creditamountdeductiblefromcommission,
               sbse.bookingfeenetrate,
               sbse.vatonbookingfee,
               sbse.bookingfee,
               sbse.paymenttype,
               sbse.paymentsurchargenetrate,
               sbse.vatonpaymentsurcharge,
               sbse.paymentsurcharge,
               sbse.transactionid,
               sbse.topdiscount,
               sbse.totalroomnights,
               sbse.impulse,
               sbse.notes,
               sbse.userjoindate,
               sbse.grossprofit,
               sbse.salestartdate,
               sbse.saleenddate,
               sbse.destinationname,
               sbse.destinationtype,
               sbse.week,
               sbse.month,
               sbse.postcode,
               sbse.citydistrict,
               sbse.totalcustomtax,
               sbse.platformname,
               sbse.appdownloaddate,
               sbse.adxnetwork,
               sbse.adxcreative,
               sbse.useracquisitionplatform,
               sbse.grossbookingvalueincurrency,
               sbse.grossbookingvalue,
               sbse.customerid,
               sbse.numberofflashnights,
               sbse.numberofbackfillednights,
               sbse.flashgrosscommissioninsuppliercurrency,
               sbse.backfillgrosscommissioninsuppliercurrency,
               sbse.usercountry,
               sbse.userstate,
               sbse.bundleid,
               sbse.saledimension,
               sbse.dynamicflightbooked,
               sbse.arrivalairport,
               sbse.flightbuyrate,
               sbse.flightsellrate,
               sbse.carrier,
               sbse.flightcommission,
               sbse.numberofbags,
               sbse.baggagesellrate,
               sbse.atolfee,
               sbse.uniquetransactionreference,
               sbse.insurancename,
               sbse.insurancetype,
               sbse.insurancepolicy,
               sbse.insuranceinsuppliercurrency,
               sbse.insuranceincustomercurrency,
               sbse.netinsurancecommissionincustomercurrency,
               sbse.agentid,
               sbse.booking_id,
               sbse.dwh_booking_id,
               sbse.lifetime_bookings,
               sbse.lifetime_margin,
               sbse.bookings_less_13m,
               sbse.bookings_more_13m,
               sbse.booker_segment,
               sbse.cancelled,
               sbse.refunded,
               sua.current_affiliate_territory
        FROM se.data.se_booking_summary_extended sbse
                 LEFT JOIN se.data.se_user_attributes sua ON sbse.customerid::VARCHAR = sua.shiro_user_id::VARCHAR
        WHERE checkout >= '2020-06-08'
          AND sbse.checkout < current_date
          AND checkout <= '2020-12-31'
          AND datebooked >= (SELECT MAX(processed_date) FROM collab.refund_credits.processed_bookings)
          AND datebooked < '2020-09-01'
          AND refunded = FALSE
          AND cancelled = FALSE
          AND sua.current_affiliate_territory IN ('UK', 'DE')
          --add check prod for user ids
          AND sbse.customerid::VARCHAR NOT IN (
              SELECT DISTINCT customerid::VARCHAR
              FROM collab.refund_credits.processed_bookings
          )
          QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY sbse.checkout ASC) = 1
    ) AS batch
    ON target.booking_id = batch.booking_id
    WHEN MATCHED AND (target.cancelled IS DISTINCT FROM batch.cancelled
        OR target.refunded IS DISTINCT FROM batch.refunded)
        THEN UPDATE SET
        target.refunded = batch.refunded,
        target.cancelled = batch.cancelled
    WHEN NOT MATCHED
        THEN INSERT VALUES (batch.processed_date,
                            batch.salename,
                            batch.type,
                            batch.company,
                            batch.supplier,
                            batch.country,
                            batch.division,
                            batch.city,
                            batch.providername,
                            batch.customeremail,
                            batch.contractor,
                            batch.saleid,
                            batch.offername,
                            batch.departureairportcode,
                            batch.departureairportname,
                            batch.adults,
                            batch.children,
                            batch.infants,
                            batch.county,
                            batch.customername,
                            batch.affiliate,
                            batch.originalacquiringaffiliate,
                            batch.datebooked,
                            batch.datetimebooked,
                            batch.timebooked,
                            batch.checkin,
                            batch.checkout,
                            batch.nonights,
                            batch.rooms,
                            batch.currency,
                            batch.territory,
                            batch.totalsellrateincurrency,
                            batch.ratetogbp,
                            batch.totalsellrate,
                            batch.commissionexvat,
                            batch.vatoncommission,
                            batch.grosscommission,
                            batch.totalnetrate,
                            batch.customertotalprice,
                            batch.customerpayment,
                            batch.creditsused,
                            batch.creditamountdeductiblefromcommission,
                            batch.bookingfeenetrate,
                            batch.vatonbookingfee,
                            batch.bookingfee,
                            batch.paymenttype,
                            batch.paymentsurchargenetrate,
                            batch.vatonpaymentsurcharge,
                            batch.paymentsurcharge,
                            batch.transactionid,
                            batch.topdiscount,
                            batch.totalroomnights,
                            batch.impulse,
                            batch.notes,
                            batch.userjoindate,
                            batch.grossprofit,
                            batch.salestartdate,
                            batch.saleenddate,
                            batch.destinationname,
                            batch.destinationtype,
                            batch.week,
                            batch.month,
                            batch.postcode,
                            batch.citydistrict,
                            batch.totalcustomtax,
                            batch.platformname,
                            batch.appdownloaddate,
                            batch.adxnetwork,
                            batch.adxcreative,
                            batch.useracquisitionplatform,
                            batch.grossbookingvalueincurrency,
                            batch.grossbookingvalue,
                            batch.customerid,
                            batch.numberofflashnights,
                            batch.numberofbackfillednights,
                            batch.flashgrosscommissioninsuppliercurrency,
                            batch.backfillgrosscommissioninsuppliercurrency,
                            batch.usercountry,
                            batch.userstate,
                            batch.bundleid,
                            batch.saledimension,
                            batch.dynamicflightbooked,
                            batch.arrivalairport,
                            batch.flightbuyrate,
                            batch.flightsellrate,
                            batch.carrier,
                            batch.flightcommission,
                            batch.numberofbags,
                            batch.baggagesellrate,
                            batch.atolfee,
                            batch.uniquetransactionreference,
                            batch.insurancename,
                            batch.insurancetype,
                            batch.insurancepolicy,
                            batch.insuranceinsuppliercurrency,
                            batch.insuranceincustomercurrency,
                            batch.netinsurancecommissionincustomercurrency,
                            batch.agentid,
                            batch.booking_id,
                            batch.dwh_booking_id,
                            batch.lifetime_bookings,
                            batch.lifetime_margin,
                            batch.bookings_less_13m,
                            batch.bookings_more_13m,
                            batch.booker_segment,
                            batch.cancelled,
                            batch.refunded,
                            batch.current_affiliate_territory);
        ;`

        var stmt = snowflake.createStatement( {sqlText: sql_command} );
        stmt.execute();
        return "Rows inserted"
    $$;
------------------------------------------------------------------------------------------------------------------------
--process data
CALL collab.refund_credits.insert_new_data_procedure();

------------------------------------------------------------------------------------------------------------------------
--retrieve bookings
SELECT *
FROM collab.refund_credits.processed_bookings pb
WHERE pb.processed_date = current_date;

--retrieve users
WITH user_credits AS (
    SELECT scm.user_id,
           scm.credit_currency,
           SUM(scm.credit_amount)         AS total_credit,
           count(*)                       AS no_credits,
           LISTAGG(scm.credit_type, ', ') AS credit_type_list
    FROM se.data.se_credit_model scm
    WHERE LOWER(scm.credit_status) = 'active'
      AND (scm.credit_expires_on IS NULL OR scm.credit_expires_on >= current_date)
    GROUP BY 1, 2
),
     user_bookings AS (

         SELECT pb.customerid,
                COUNT(pb.booking_id)         AS bookings,
                LISTAGG(pb.booking_id, ', ') AS booking_id_list
         FROM collab.refund_credits.processed_bookings pb
              -- only show users with bookings processed today
         WHERE pb.processed_date = CURRENT_DATE
         GROUP BY 1

     )
SELECT ub.customerid,
       ub.bookings,
       ub.booking_id_list,
       uc.credit_currency,
       COALESCE(uc.total_credit, 0) AS total_credit,
       uc.no_credits,
       uc.credit_type_list
FROM user_bookings ub
         LEFT JOIN user_credits uc ON ub.customerid = uc.user_id;
;

------------------------------------------------------------------------------------------------------------------------

GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__cianweeresinghe;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__cianweeresinghe;

GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__radujosan;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__radujosan;

GRANT USAGE ON SCHEMA collab.refund_credits TO ROLE personal_role__sophieserunjogi;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.refund_credits TO ROLE personal_role__sophieserunjogi;

------------------------------------------------------------------------------------------------------------------------
-- if necessary to repopulate table
TRUNCATE collab.refund_credits.processed_bookings;
INSERT INTO collab.refund_credits.processed_bookings
SELECT '2020-07-20' AS processed_date,
       sbse.salename,
       sbse.type,
       sbse.company,
       sbse.supplier,
       sbse.country,
       sbse.division,
       sbse.city,
       sbse.providername,
       sbse.customeremail,
       sbse.contractor,
       sbse.saleid,
       sbse.offername,
       sbse.departureairportcode,
       sbse.departureairportname,
       sbse.adults,
       sbse.children,
       sbse.infants,
       sbse.county,
       sbse.customername,
       sbse.affiliate,
       sbse.originalacquiringaffiliate,
       sbse.datebooked,
       sbse.datetimebooked,
       sbse.timebooked,
       sbse.checkin,
       sbse.checkout,
       sbse.nonights,
       sbse.rooms,
       sbse.currency,
       sbse.territory,
       sbse.totalsellrateincurrency,
       sbse.ratetogbp,
       sbse.totalsellrate,
       sbse.commissionexvat,
       sbse.vatoncommission,
       sbse.grosscommission,
       sbse.totalnetrate,
       sbse.customertotalprice,
       sbse.customerpayment,
       sbse.creditsused,
       sbse.creditamountdeductiblefromcommission,
       sbse.bookingfeenetrate,
       sbse.vatonbookingfee,
       sbse.bookingfee,
       sbse.paymenttype,
       sbse.paymentsurchargenetrate,
       sbse.vatonpaymentsurcharge,
       sbse.paymentsurcharge,
       sbse.transactionid,
       sbse.topdiscount,
       sbse.totalroomnights,
       sbse.impulse,
       sbse.notes,
       sbse.userjoindate,
       sbse.grossprofit,
       sbse.salestartdate,
       sbse.saleenddate,
       sbse.destinationname,
       sbse.destinationtype,
       sbse.week,
       sbse.month,
       sbse.postcode,
       sbse.citydistrict,
       sbse.totalcustomtax,
       sbse.platformname,
       sbse.appdownloaddate,
       sbse.adxnetwork,
       sbse.adxcreative,
       sbse.useracquisitionplatform,
       sbse.grossbookingvalueincurrency,
       sbse.grossbookingvalue,
       sbse.customerid,
       sbse.numberofflashnights,
       sbse.numberofbackfillednights,
       sbse.flashgrosscommissioninsuppliercurrency,
       sbse.backfillgrosscommissioninsuppliercurrency,
       sbse.usercountry,
       sbse.userstate,
       sbse.bundleid,
       sbse.saledimension,
       sbse.dynamicflightbooked,
       sbse.arrivalairport,
       sbse.flightbuyrate,
       sbse.flightsellrate,
       sbse.carrier,
       sbse.flightcommission,
       sbse.numberofbags,
       sbse.baggagesellrate,
       sbse.atolfee,
       sbse.uniquetransactionreference,
       sbse.insurancename,
       sbse.insurancetype,
       sbse.insurancepolicy,
       sbse.insuranceinsuppliercurrency,
       sbse.insuranceincustomercurrency,
       sbse.netinsurancecommissionincustomercurrency,
       sbse.agentid,
       sbse.booking_id,
       sbse.dwh_booking_id,
       sbse.lifetime_bookings,
       sbse.lifetime_margin,
       sbse.bookings_less_13m,
       sbse.bookings_more_13m,
       sbse.booker_segment,
       sbse.cancelled,
       sbse.refunded,
       sua.current_affiliate_territory
FROM se.data.se_booking_summary_extended sbse
         LEFT JOIN se.data.se_user_attributes sua ON sbse.customerid::VARCHAR = sua.shiro_user_id::VARCHAR
WHERE sbse.checkout >= '2020-06-08'
  AND sbse.checkout < current_date
  AND sbse.checkout <= '2020-12-31'
  AND sbse.datebooked >= '2020-06-08'
  AND sbse.datebooked <= '2020-07-20'
  AND sbse.datebooked >= (
    SELECT MAX(processed_date)
    FROM collab.refund_credits.processed_bookings
)
  AND sbse.datebooked < '2020-09-01'
  AND sbse.refunded = FALSE
  AND sbse.cancelled = FALSE
  AND sua.current_affiliate_territory IN ('UK', 'DE')
  AND sbse.customerid::VARCHAR NOT IN (
    SELECT DISTINCT customerid::VARCHAR
    FROM collab.refund_credits.processed_bookings
)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY sbse.checkout ASC) = 1;


------------------------------------------------------------------------------------------------------------------------
--initial run
--only show users first booking, if they've made multiple bookings within this timeframe
--only show their earliest check out booking

GRANT USAGE ON PROCEDURE collab.refund_credits.insert_new_data_procedure() TO ROLE personal_role__radujosan;


SELECT id,
       username AS email
FROM collab.refund_credits.shiro_user;

CREATE VIEW collab.refund_credits.shiro_user AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot sus;

GRANT SELECT ON VIEW collab.refund_credits.shiro_user TO ROLE personal_role__kirstengrieve;



SELECT min(datebooked), max(checkout), min(checkout)
FROM collab.refund_credits.processed_bookings pb
         LEFT JOIN se.data_pii.se_user_attributes sua ON pb.customerid = sua.shiro_user_id
WHERE sua.original_affiliate_territory = 'DE';
--   AND pb.processed_date = current_date;

DROP VIEW collab.refund_credits.shiro_user;


------------------------------------------------------------------------------------------------------------------------


SELECT customerid, count(*)
FROM (
         SELECT email,
                customerid,
                processed_date,
                datebooked,
                checkin,
                checkout
         FROM collab.refund_credits.processed_bookings pb
                  LEFT JOIN se.data_pii.se_user_attributes sua ON pb.customerid = sua.shiro_user_id
         WHERE sua.original_affiliate_territory = 'UK'
           AND pb.processed_date = current_date
     )
GROUP BY 1
HAVING count(*) > 1;

CREATE OR REPLACE TABLE collab.refund_credits.processed_bookings_20200720 CLONE collab.refund_credits.processed_bookings;
CREATE OR REPLACE TABLE collab.refund_credits.processed_bookings CLONE collab.refund_credits.processed_bookings_20200720;

DELETE
FROM collab.refund_credits.processed_bookings pb
WHERE pb.processed_date = '2020-07-20';

SELECT processed_date, count(*)
FROM collab.refund_credits.processed_bookings pb
GROUP BY 1;

SELECT count(DISTINCT sbse.customerid)
FROM se.data.se_booking_summary_extended sbse
         LEFT JOIN se.data.se_user_attributes sua ON sbse.customerid::VARCHAR = sua.shiro_user_id::VARCHAR
WHERE checkout >= '2020-06-08'
  AND sbse.checkout < current_date
  AND checkout <= '2020-12-31'
  AND sbse.datebooked >= '2020-06-08'
--   AND sbse.datebooked >= (SELECT MAX(processed_date) FROM collab.refund_credits.processed_bookings)
  AND datebooked < '2020-09-01'
  AND refunded = FALSE
  AND cancelled = FALSE
  AND sua.current_affiliate_territory IN ('UK', 'DE');


SELECT *
FROM collab.refund_credits.processed_bookings pb;

SELECT sb.booking_status, refunded, cancelled, count(*)
FROM se.data_pii.se_booking_summary_extended sbse
         LEFT JOIN se.data.se_booking sb ON sbse.transactionid = sb.transaction_id
WHERE (sbse.refunded
    OR sbse.cancelled)
  AND sb.booking_completed_date >= '2020-01-01'
GROUP BY 1, 2, 3;


SELECT sbse.cancelled, sb.*
FROM se.data_pii.se_booking_summary_extended sbse
         LEFT JOIN se.data.se_booking sb ON sbse.transactionid = sb.transaction_id
WHERE sbse.cancelled
  AND sb.booking_status = 'COMPLETE'
  AND sb.booking_completed_date >= '2020-01-01';

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_reports.cancellations
WHERE transactionid = 'A7610-8546-959911';


