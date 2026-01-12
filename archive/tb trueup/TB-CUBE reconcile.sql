--List of TB transactions
alter session set use_cached_result = FALSE;
USE WAREHOUSE PIPE_MEDIUM;
USE role PERSONAL_ROLE__ROBINPATEL;
CREATE SCHEMA TB_RECONCILE;
USE SCHEMA TB_RECONCILE;

DROP TABLE SNOWFLAKE_TB_BOOKINGS;

CREATE OR REPLACE TEMPORARY TABLE SNOWFLAKE_TB_BOOKINGS AS (
    WITH canx_ids AS (
        SELECT original_booking_record_reference AS transaction_id
        FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
        WHERE adjustment_reference NOT IN ( --remove cancellations
            '21866673.1179.1' -- not a real adjustment, caused by change in currency so exclude from calcs
            )
          AND COALESCE(type_of_change2, '') != 'Flight Booked'  -- see ticket DEV-28862
          AND COALESCE(type_of_change2, '') != 'Fare Optimizer' -- see ticket DEV-29062
        GROUP BY original_booking_record_reference
        HAVING MAX(CASE WHEN component_status = 'Cancelled' THEN 1 END) = 1 -- booking has at least one cancelled component
           AND SUM(supplier_component_amount) < 0
    )

    SELECT --ASSUMED an average conversion rate of EUR for transactions in this time period
           DATE_BOOKED,
           TRANSACTION_ID,
           CURRENCY,
           RATE_TO_GBP,
           COMMISSION_EX_VAT,                                                                                           --this is GBP
           COMMISSION_EX_VAT / 0.90565                                                        AS COMMISSION_EX_VAT_EUR, --flat EUR conversion rate for standardised comparison
           TERRITORY,
           BOOKING_FEE_NET_RATE,
           PAYMENT_SURCHARGE_NET_RATE,
           VAT_ON_BOOKING_FEE,                                                                                          --this is GBP
           VAT_ON_BOOKING_FEE / 0.9056                                                        AS VAT_ON_BOOKING_FEE_EUR,
           COMMISSION_EX_VAT + BOOKING_FEE_NET_RATE + PAYMENT_SURCHARGE_NET_RATE              AS MARGIN_GROSS_TOMS_VAT_GBP,
           CASE
               WHEN TRANSACTION_ID IN (SELECT * FROM canx_ids) THEN 0
               ELSE COMMISSION_EX_VAT + BOOKING_FEE_NET_RATE + PAYMENT_SURCHARGE_NET_RATE END AS MARGIN_GROSS_TOMS_VAT_GBP_MINUS_CANX,
           (COMMISSION_EX_VAT + BOOKING_FEE_NET_RATE + PAYMENT_SURCHARGE_NET_RATE) /
           0.90565                                                                            AS MARGIN_GROSS_TOMS_VAT_EUR,

           CASE
               WHEN TRANSACTION_ID IN (SELECT * FROM canx_ids) THEN 0
               ELSE (COMMISSION_EX_VAT + BOOKING_FEE_NET_RATE + PAYMENT_SURCHARGE_NET_RATE) /
                    0.90565 END                                                               AS MARGIN_GROSS_TOMS_VAT_EUR_MINUS_CANX,

           TOTAL_SELL_RATE_IN_CURRENCY,
           TOTAL_SELL_RATE,
           GROSS_BOOKING_VALUE_IN_CURRENCY,
           GROSS_BOOKING_VALUE,
           COALESCE(TOTAL_SELL_RATE_IN_CURRENCY / nullif(TOTAL_SELL_RATE, 0),
                    GROSS_BOOKING_VALUE_IN_CURRENCY / nullif(GROSS_BOOKING_VALUE, 0))         AS DERIVED_EXCHANGE_RATE,
           EXTRACTED_AT,
           LOADED_AT

    FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY
    WHERE DATE_BOOKED BETWEEN '2019-08-16' AND CURRENT_DATE
    ORDER BY 1
);


------------------------------------------------------------------------------------------------------------------------
USE ROLE PERSONAL_ROLE__ROBINPATEL;
SELECT
       DATE_BOOKED,
       TRANSACTION_ID,
       CURRENCY,
       COMMISSION_EX_VAT,
       COMMISSION_EX_VAT_EUR,
       BOOKING_FEE_NET_RATE,
       PAYMENT_SURCHARGE_NET_RATE,
       MARGIN_GROSS_TOMS_VAT_GBP,
       MARGIN_GROSS_TOMS_VAT_GBP_MINUS_CANX,
       MARGIN_GROSS_TOMS_VAT_EUR,
       TOTAL_SELL_RATE_IN_CURRENCY,
       TOTAL_SELL_RATE,
       DERIVED_EXCHANGE_RATE,
       EXTRACTED_AT,
       LOADED_AT

FROM SNOWFLAKE_TB_BOOKINGS
WHERE
--       TRANSACTION_ID='21873501'
-- CURRENCY != 'PLN' AND --we don't send PLN to cube
DATE_BOOKED IN ('2019-09-02',
                '2019-09-11',
                '2019-09-17')
--                 , '2019-09-11'
--                )
;


------------------------------------------------------------------------------------------------------------------------

SELECT -- daily summary
       DATE_BOOKED,
       sum(COMMISSION_EX_VAT)          AS COMMISSION_EX_VAT,
       sum(BOOKING_FEE_NET_RATE)       AS BOOKING_FEE_NET_RATE,
--        sum(PAYMENT_SURCHARGE_NET_RATE) AS PAYMENT_SURCHARGE_NET_RATE,
       sum(COMMISSION_EX_VAT) + sum(BOOKING_FEE_NET_RATE) +
       sum(PAYMENT_SURCHARGE_NET_RATE) AS margin_gross_of_toms,
       round((sum(COMMISSION_EX_VAT) + sum(BOOKING_FEE_NET_RATE) + sum(PAYMENT_SURCHARGE_NET_RATE)) / 0.90565,
             2)                        AS margin_gross_of_toms_eur,
        sum(MARGIN_GROSS_TOMS_VAT_GBP_MINUS_CANX) as MARGIN_GROSS_TOMS_VAT_GBP_MINUS_CANX
FROM SNOWFLAKE_TB_BOOKINGS
WHERE CURRENCY !='PLN'
AND DATE_BOOKED BETWEEN '2019-09-01' AND '2019-09-19'
GROUP BY 1
ORDER BY 1;

--2nd and 11th september
;

-- AND TRANSACTION_ID IN ( -- transactions identified as having high derived exchange rate
-- '21872026',
-- '21872216',
-- '21872161',
-- '21872074',
-- '21871795',
-- '21872223',
-- '21872147',
-- '21872219',
-- '21871921',
-- '21871808',
-- '21871992',
-- '21872002',
-- '21872018',
-- '21872180',
-- '21872198',
-- '21872101',
-- '21871994',
-- '21872111',
-- '21871900',
-- '21872203',
-- '21871780',
-- '21872008',
-- '21872027',
-- '21871784',
-- '21871936',
-- '21872097',
-- '21872064',
-- '21872162',
-- '21871853',
-- '21872204',
-- '21872220',
-- '21872209',
-- '21871829',
-- '21872179',
-- '21871875',
-- '21872196',
-- '21872071',
-- '21871854')

------------------------------------------------------------------------------------------------------------------------
--agg daily summary removing cancellations and PLN bookings

SELECT DATE_BOOKED,
--        CURRENCY,
       SUM(COMMISSION_EX_VAT)                              as COMMISSION_EX_VAT,
       round(SUM(COMMISSION_EX_VAT_EUR), 2)                as COMMISSION_EX_VAT_EUR,
       round(SUM(MARGIN_GROSS_TOMS_VAT_GBP), 2)            as MARGIN_GROSS_TOMS_VAT_GBP,
       round(SUM(MARGIN_GROSS_TOMS_VAT_GBP_MINUS_CANX), 2) as MARGIN_GROSS_TOMS_VAT_GBP_MINUS_CANX,
       round(SUM(MARGIN_GROSS_TOMS_VAT_EUR), 2)            as MARGIN_GROSS_TOMS_VAT_EUR,
       round(SUM(MARGIN_GROSS_TOMS_VAT_EUR_MINUS_CANX), 2) as MARGIN_GROSS_TOMS_VAT_EUR_MINUS_CANX
--        ROUND(SUM(COMMISSION_EX_VAT_GBP), 2) AS COMMISSION_EX_VAT_GBP

FROM SNOWFLAKE_TB_BOOKINGS
WHERE DATE_BOOKED BETWEEN '2019-08-16' AND '2019-09-12'

  AND CURRENCY != 'PLN'
GROUP BY 1--,2
ORDER BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------


--ADJUSTMENTS


------------------------------------------------------------------------------------------------------------------------

-- Check for financially adjusted bookings summary for week
SELECT --adjustment query
       DATE_TRUNC(week, ORIGINAL_BOOKING_DATE)           AS WC,
       TYPE_OF_CHANGE2,
       ROUND(sum(SUPPLIER_COMPONENT_AMOUNT), 2)          as supplier_component_amount,
       count(distinct ORIGINAL_BOOKING_RECORD_REFERENCE) as adjusted_bookings


FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
WHERE --ORIGINAL_BOOKING_RECORD_REFERENCE = '21872120'

        ORIGINAL_BOOKING_RECORD_REFERENCE IN (SELECT ORIGINAL_BOOKING_RECORD_REFERENCE
                                              FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
                                              WHERE SUPPLIER_COMPONENT_AMOUNT != 0)

  AND ORIGINAL_BOOKING_DATE BETWEEN '2019-08-26' AND '2019-09-01'
  AND ORIGINAL_BOOKING_RECORD_REFERENCE NOT IN ( --cancellation query sent to cube
    SELECT distinct original_booking_record_reference AS transaction_id
    FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
    WHERE adjustment_reference NOT IN ( --remove cancellations
        '21866673.1179.1' -- not a real adjustment, caused by change in currency so exclude from calcs
        )
      AND COALESCE(type_of_change2, '') != 'Flight Booked'  -- see ticket DEV-28862
      AND COALESCE(type_of_change2, '') != 'Fare Optimizer' -- see ticket DEV-29062
    GROUP BY original_booking_record_reference
    HAVING MAX(CASE WHEN component_status = 'Cancelled' THEN 1 END) = 1 -- booking has at least one cancelled component
       AND SUM(supplier_component_amount) < 0
)

group by 1, 2
order by 1 DESC, 2;

------------------------------------------------------------------------------------------------------------------------

SELECT -- SUMMARY adjustments at booking level
       fl.ORIGINAL_BOOKING_RECORD_REFERENCE,
--        TYPE_OF_CHANGE2,
--        SUPPLIER_COMPONENT_AMOUNT
       MAX(bs.TERRITORY)                                as territory,
       COUNT(distinct ADJUSTMENT_REFERENCE)             as count_of_changes,
       LISTAGG(fl.TYPE_OF_CHANGE2, ', ')                as changes,
       ABS(ROUND(sum(fl.SUPPLIER_COMPONENT_AMOUNT), 2)) as supplier_component_amount_abs,
       ROUND(sum(fl.SUPPLIER_COMPONENT_AMOUNT), 2)      as supplier_component_amount

FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG AS fl
         LEFT JOIN RAW_VAULT.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY AS bs
                   ON bs.TRANSACTION_ID = fl.ORIGINAL_BOOKING_RECORD_REFERENCE

WHERE
--ORIGINAL_BOOKING_RECORD_REFERENCE = '21871809' AND
    ORIGINAL_BOOKING_DATE BETWEEN '2019-08-26' AND '2019-09-01'
  AND ORIGINAL_BOOKING_RECORD_REFERENCE NOT IN ( --cancellation query sent to cube
    SELECT distinct original_booking_record_reference AS transaction_id
    FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
    WHERE adjustment_reference NOT IN ( --remove cancellations
        '21866673.1179.1' -- not a real adjustment, caused by change in currency so exclude from calcs
        )
      AND COALESCE(type_of_change2, '') != 'Flight Booked'  -- see ticket DEV-28862
      AND COALESCE(type_of_change2, '') != 'Fare Optimizer' -- see ticket DEV-29062
    GROUP BY original_booking_record_reference
    HAVING MAX(CASE WHEN component_status = 'Cancelled' THEN 1 END) = 1 -- booking has at least one cancelled component
       AND SUM(supplier_component_amount) < 0
)
group by 1
order by 5 DESC;


------------------------------------------------------------------------------------------------------------------------


SELECT distinct --ADJUSTMENTS THAT DON'T CONSIST OF A CANCELLATION

                ORIGINAL_BOOKING_RECORD_REFERENCE,
                TYPE_OF_CHANGE,
                TYPE_OF_CHANGE2,
                COMPONENT_STATUS,
                TOTAL_ADJUSTMENT, -- customer price
                SUPPLIER_COMPONENT_AMOUNT,
                ADJUSTMENT_CURRENCY
FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG

WHERE ORIGINAL_BOOKING_DATE BETWEEN '2019-08-16' AND '2019-09-15'
--   AND ORIGINAL_BOOKING_RECORD_REFERENCE = '21873072'
  AND ORIGINAL_BOOKING_RECORD_REFERENCE NOT IN ( --cancellation query sent to cube
    SELECT distinct original_booking_record_reference AS transaction_id
    FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
    WHERE adjustment_reference NOT IN ( --remove cancellations
        '21866673.1179.1' -- not a real adjustment, caused by change in currency so exclude from calcs
        )
      AND COALESCE(type_of_change2, '') != 'Flight Booked'  -- see ticket DEV-28862
      AND COALESCE(type_of_change2, '') != 'Fare Optimizer' -- see ticket DEV-29062
    GROUP BY original_booking_record_reference
    HAVING MAX(CASE WHEN component_status = 'Cancelled' THEN 1 END) = 1 -- booking has at least one cancelled component
       AND SUM(supplier_component_amount) < 0
      )
AND ORIGINAL_BOOKING_RECORD_REFERENCE='21873100'

ORDER BY 1;


SELECT *
FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
WHERE ORIGINAL_BOOKING_RECORD_REFERENCE = '21873100';


SELECT *
FROM RAW_VAULT.CMS_MYSQL.EXCHANGE_RATE
--  WHERE DATE_CREATED > '2019-05-09 00:00:00.000000000'
ORDER BY DATE_CREATED DESC;



--   AND TRANSACTION_ID NOT IN ( --moved this into transient
--       --cancellation query sent to cube
--     SELECT original_booking_record_reference AS transaction_id
--     FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
--     WHERE adjustment_reference NOT IN ( --remove cancellations
--         '21866673.1179.1' -- not a real adjustment, caused by change in currency so exclude from calcs
--         )
--       AND COALESCE(type_of_change2, '') != 'Flight Booked'  -- see ticket DEV-28862
--       AND COALESCE(type_of_change2, '') != 'Fare Optimizer' -- see ticket DEV-29062
--     GROUP BY original_booking_record_reference
--     HAVING MAX(CASE WHEN component_status = 'Cancelled' THEN 1 END) = 1 -- booking has at least one cancelled component
--        AND SUM(supplier_component_amount) < 0
--  )


    SELECT distinct
           original_booking_record_reference AS transaction_id
    FROM RAW_VAULT.TRAVELBIRD_CATALOGUE.FINANCE_CHANGE_LOG
    WHERE adjustment_reference NOT IN ( --remove cancellations
        '21866673.1179.1' -- not a real adjustment, caused by change in currency so exclude from calcs
        )
      AND COALESCE(type_of_change2, '') != 'Flight Booked'  -- see ticket DEV-28862
      AND COALESCE(type_of_change2, '') != 'Fare Optimizer' -- see ticket DEV-29062
    GROUP BY original_booking_record_reference
    HAVING MAX(CASE WHEN component_status = 'Cancelled' THEN 1 END) = 1 -- booking has at least one cancelled component
       AND SUM(supplier_component_amount) < 0
AND ORIGINAL_BOOKING_RECORD_REFERENCE='21873072';