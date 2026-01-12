USE WAREHOUSE PIPE_XLARGE;

select member_id,
       member_age,
       member_cohort_year_month,
       customer_age,
       calendar_date,
       spv_count,
       EMAIL_CLICKS_COUNT,
       email_opens_count,
       booking_completed_count,
       booking_margin,
       booking_cumulative_count
from "DATA_VAULT_MVP"."CUSTOMER_MODEL"."CUSTOMER_MODEL_FULL_UK_DE"
where member_id in (28143583, 64843622, 57138078, 11900033, 17705060, 58341855)
  and booking_completed_count is not null

SELECT RECORD['_id'] ::VARCHAR                                      AS BOOKING_ID,
       RECORD['customerId']                                         AS CUSTOMER_ID,
       RECORD['currency']                                           AS CURRENCY,
       TRY_TO_TIMESTAMP(RECORD['dateTimeBooked']['$date']::VARCHAR) AS DATE_TIME_BOOKED,
       RECORD['checkIn']['$date']                                   AS CHECKIN_DATE,
       RECORD['checkOut']['$date']                                  AS CHECKOUT_DATE,
       RECORD['noNights']                                           AS NO_NIGHTS,
       RECORD['vatOnCommission']                                    AS VAT_ON_COMMISSION,
       RECORD['grossBookingValue']                                  AS GROSS_BOOKING_VALUE,
       RECORD['customerTotalPrice']                                 AS CUSTOMER_TOTAL_PRICE,
       RECORD['commissionExVat']                                    AS COMMISSION_EX_VAT,
       RECORD['bookingFeeNetRate']                                  AS BOOKING_FEE_NET_RATE,
       RECORD['paymentSurchargeNetRate']                            AS PAYMENT_SURCHARGE_NET_RATE,
       RECORD['rateToGbp']                                          AS RATE_TO_GBP,
       RECORD['customerEmail']                                      AS customer_email,
       RECORD['type']                                               AS sale_type,
       RECORD['bookingStatus']                                      AS bookingStatus

FROM RAW_VAULT.CMS_MONGODB.BOOKING_SUMMARY
WHERE record['customerId'] = '28143583'
  AND TRY_TO_TIMESTAMP(RECORD['dateTimeBooked']['$date']::VARCHAR)::DATE = '2019-02-06'
