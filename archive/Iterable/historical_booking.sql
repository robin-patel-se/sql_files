------------------------------------------------------------------------------------------------------------------------
-- need to restructure this job to match an event level grain
-- events being fired are:
-- 1. completed
-- 2. cancelled
-- datasets we are sending these for:
-- a. old data model bookings
-- b. new data model bookings
-- c. external bookings

--------
USE WAREHOUSE pipe_xlarge;

-- completed, new and old data model bookings
SELECT sb.transaction_id,                                                                     -- transactionId
       sb.booking_id,                                                                         -- orderHeaderId
       sb.sale_name,                                                                          -- productName
       sb.sale_id                                               AS se_sale_id,                -- productId
       sb.offer_id,                                                                           -- offerId
       sb.booking_completed_timestamp,                                                        -- bookingTimestamp
       DATEDIFF(WEEK, '2011-01-31', sb.booking_completed_date)  AS booking_week,              -- bookingWeek
       DATEDIFF(MONTH, '2011-01-31', sb.booking_completed_date) AS booking_month,             -- bookingMonth
       sb.check_in_date,                                                                      -- checkInDate
       sb.check_out_date,                                                                     -- checkOutDate
       sb.adult_guests,                                                                       -- numAdults
       sb.child_guests,                                                                       -- numChildren
       sb.infant_guests,                                                                      -- numInfants
       sb.no_nights,                                                                          -- numNights
       sb.rooms,                                                                              -- numRooms
       sb.room_nights,                                                                        -- totalRoomNights
       sb.departure_airport_code,                                                             -- airportCode
       --airportName
       CASE
           WHEN sb.device_platform = 'web' THEN 'WEB'
           WHEN sb.device_platform = 'tablet web' THEN 'TABLET_WEB'
           WHEN sb.device_platform = 'mobile web' THEN 'MOBILE_WEB'
           WHEN sb.device_platform = 'mobile wrap android' THEN 'MOBILE_WRAP_ANDROID'
           WHEN sb.device_platform = 'native app android' THEN 'ANDROID_APP_V3'
           WHEN sb.device_platform = 'mobile wrap ios' THEN 'MOBILE_WRAP_IOS'
           WHEN sb.device_platform = 'native app ios' THEN 'IOS_APP_V3'
           ELSE 'not specified'
           END                                                  AS platform_name,             -- bookingPlatformName
       -- bookingDeviceModel
--        -- bookingDeviceOS
       sb.cc_rate_to_gbp,                                                                     -- exchangeToGBP
       sb.margin_gross_of_toms_gbp,                                                           -- marginAmount
       sb.gross_revenue_gbp,                                                                  -- orderAmount
       sb.total_received_from_user_gbp,                                                       -- customerPayment
       sb.currency,                                                                           -- customerCurrency
       sb.credits_used_gbp,                                                                   -- creditsUsed
       ss.salesforce_opportunity_id,                                                          -- sfSaleId
       sb.has_flights,                                                                        -- hasFlights

       sb.last_updated,                                                                       -- modifiedTimestamp
       ua.email,
       sb.shiro_user_id,                                                                      -- userId
       sb.top_discount,                                                                       -- topDiscount
       sb.payment_type,                                                                       -- paymentType
       sb.booking_status,                                                                     -- bookingStatus
       DATEADD(DAY, -sb.cancellation_policy_number_of_days, sb.check_in_date)
                                                                AS lastest_cancellation_date, -- latestCancellationDate
       ss.destination_name,                                                                   -- destinationName
       a.domain,                                                                              -- applicationDomain
       ss.company_name,                                                                       -- companyName
       ss.company_id,                                                                         -- companyId
       ss.posu_country,                                                                       -- countryName
       ss.posu_city,                                                                          -- cityName
       sb.sale_type,                                                                          -- saleType
       sb.sale_product,                                                                       -- saleProduct
       sb.territory,                                                                          -- territory
       'SECRET_ESCAPES'                                         AS tech_platform

FROM data_vault_mvp.dwh.se_booking sb
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON sb.shiro_user_id = ua.shiro_user_id
    INNER JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.affiliate a ON sb.affiliate_id = a.id
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED', 'CANCELLED')
UNION ALL

SELECT tb.transaction_id,
       tb.order_id::VARCHAR                             AS booking_id,
       t.short_title                                    AS sale_name,                    -- productName
       tb.se_sale_id,                                                                    -- productId
       tb.offer_id::VARCHAR                             AS offer_id,                     -- offerId, check this is right, this is a TB offer not an SE one
       tb.created_at_dts                                AS booking_completed_timestamp,  -- bookingTimestamp
       DATEDIFF(WEEK, '2011-01-31', tb.created_at_dts)  AS booking_week,                 -- bookingWeek
       DATEDIFF(MONTH, '2011-01-31', tb.created_at_dts) AS booking_month,                -- bookingMonth
       tb.accommodation_start_date                      AS check_in_date,                -- checkInDate
       tb.accommodation_end_date                        AS check_out_date,               -- checkOutDate
       tb.adult_guests                                  AS adult_guests,                 -- numAdults
       tb.child_guests                                  AS child_guests,                 -- numChildren
       tb.infant_guests                                 AS infant_guests,                -- numInfants
       tb.no_nights                                     AS no_nights,                    -- numNights
       tb.rooms                                         AS rooms,                        -- numRooms
       tb.room_nights                                   AS room_nights,                  -- totalRoomNights
       NULL                                             AS departure_airport_code,       -- airportCode
       -- airportName
       tb.platform                                      AS platform_name,                -- bookingPlatformName
       -- bookingDeviceModel
       -- bookingDeviceOS
       tb.sold_price_total_cc / tb.sold_price_total_gbp AS cc_rate_to_gbp,               -- exchangeToGBP
       tb.margin_gbp                                    AS margin_gross_of_toms_gbp,     -- marginAmount
       tb.sold_price_total_gbp                          AS gross_revenue_gbp,            -- orderAmount
       tb.sold_price_total_gbp                          AS total_received_from_user_gbp, -- customerPayment
       tb.sold_price_currency                           AS currency,                     -- customerCurrency
       NULL                                             AS credits_used_gbp,             -- creditsUsed
       t.salesforce_opportunity_id                      AS salesforce_opportunity_id,    -- sfSaleId
       tb.booking_includes_flight,                                                       -- hasFlights
       tb.updated_at_dts                                AS last_updated,                 -- modifiedTimestamp
       ua.email,
       tb.shiro_user_id,                                                                 -- userId
       NULL                                             AS top_discount,                 -- topDiscount
       NULL                                             AS payment_type,                 -- paymentType
       tb.payment_status                                AS booking_status,               -- bookingStatus
       NULL                                             AS lastest_cancellation_date,    -- latestCancellationDate
       NULL                                             AS destination_name,             -- destinationName
       NULL                                             AS domain,                       -- applicationDomain
       NULL                                             AS company_name,                 -- companyId
       NULL                                             AS company_id,                   -- companyName
       t.posu_country,                                                                   -- countryName
       t.posu_city,                                                                      -- cityName
       t.sale_type,                                                                      -- saleType
       t.sale_product,                                                                   -- saleProduct
       t.posa_territory,                                                                 -- territory
       'TRAVELBIRD'                                     AS tech_platform
FROM hygiene_snapshot_vault_mvp.cms_mysql.external_booking eb
    INNER JOIN data_vault_mvp.dwh.tb_booking tb ON 'TB-' || eb.external_id = tb.booking_id
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON tb.shiro_user_id = ua.shiro_user_id
    INNER JOIN data_vault_mvp.dwh.tb_offer t ON tb.offer_id = t.id
WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
  AND tb.se_brand = 'SE Brand'
;

-- cancelled new and old data model bookings
SELECT ua.email,
       bc.date_created          AS cancellation_tstamp, -- cancellationTimestamp
       bc.who_pays,                                     -- costPayer
       sb.booking_completed_timestamp,                  -- bookingTimestamp
       bc.booking_fee_cc
           + bc.cc_fee_cc
           + bc.hotel_good_will_cc
           + bc.se_good_will_cc AS refund_amount,       -- refundAmount
       bc.refund_type,                                  -- refundType
       sb.transaction_id,                               -- transactionId
       'SECRET_ESCAPES'         AS tech_platform
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
    INNER JOIN data_vault_mvp.dwh.se_booking sb ON bc.booking_id = sb.booking_id
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON sb.shiro_user_id = ua.shiro_user_id
WHERE bc.refund_type IN ('CANCELLATION', 'FULL')
;
--------
-- cancelled external bookings

SELECT ua.email,
       oos.created_at_dts                        AS cancellation_tstamp,
       NULL                                      AS who_pays,
       tb.created_at_dts                         AS booking_completed_timestamp,
       NULL                                      AS refund_amount,
       oos.event_data:adjustment_reason::VARCHAR AS refund_type,
       tb.transaction_id,
       oos.event_data,
       'TRAVELBIRD'                              AS tech_platform
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderevent oos
    INNER JOIN data_vault_mvp.dwh.tb_booking tb ON oos.order_id = tb.order_id
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON tb.shiro_user_id = ua.shiro_user_id
WHERE oos.event_type = 'ORDER_CANCELLED';


SELECT *
FROM data_vault_mvp.dwh.tb_offer t;

SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;

SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.external_booking eb;

SELECT *
FROM data_vault_mvp.dwh.tb_booking tb
WHERE tb.order_id = 'A6318239';


SELECT *
FROM se.data.fact_booking fb;


self_describing_task --include 'dv/dwh/iterable/order_event.py'  --method 'run' --start '2021-11-02 00:00:00' --end '2021-11-02 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__order_event;

self_describing_task --include 'dv/dwh/iterable/order_cancelled_event.py'  --method 'run' --start '2021-11-02 00:00:00' --end '2021-11-02 00:00:00'

SELECT t.se_brand, COUNT(*)
FROM data_vault_mvp.dwh.tb_offer t
GROUP BY 1;

SELECT tb.*
FROM se.data.tb_booking tb
    INNER JOIN se.data.tb_offer t ON tb.se_sale_id = t.se_sale_id;

SELECT t.se_sale_id,
       t.posa_territory,
       t.se_brand
FROM se.data.tb_offer t;

SELECT *
FROM data_vault_mvp.dwh.tb_offer o;

SELECT se_brand,
       tech_platform,
       COUNT(*)
FROM se.data.fact_complete_booking fcb
GROUP BY 1, 2
    self_describing_task --include 'dv/dwh/iterable/order_complete_event.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'


SELECT COUNT(DISTINCT sb.shiro_user_id)
FROM collab.booking_cancellation_data.booking_cancellation bc
    INNER JOIN se.data.se_booking sb ON bc.booking_id = sb.booking_id
WHERE bc.refund_type IN ('CANCELLATION', 'FULL')
  AND bc.date_created >= '2021-11-01'

UNION ALL

SELECT booking_id
FROM se.data.tb_booking tb
WHERE
;