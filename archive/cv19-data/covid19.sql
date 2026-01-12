SELECT b.booking_id,
       b.booking_status,
       b.sale_id,
       b.check_in_date,
       b.check_out_date,
       b.booking_lead_time_days,
       b.booking_created_date,
       b.booking_completed_date,
       b.commission_ex_vat_gbp,
       b.booking_fee_net_rate_gbp,
       b.payment_surcharge_net_rate_gbp,
       b.insurance_commission_gbp,
       b.margin_gross_of_toms_gbp,
       b.tech_platform,
       s.sale_id,
       s.sale_product,
       s.sale_type,
       s.product_type,
       s.product_configuration,
       s.product_line,
       s.data_model,
       s.tech_platform
FROM se.data.fact_complete_booking b
         INNER JOIN se.data.dim_sale s ON b.sale_id = s.sale_id;

SELECT
--        sale_id,
--        sale_product,
--        sale_type,
--        product_type,
product_configuration,
--        product_line,
--        data_model,
--        tech_platform
count(*)
FROM se.data.dim_sale
GROUP BY 1


SELECT DATE_TRUNC('month', b.booking_completed_date)   AS date,
       SUM(CASE WHEN s.sale_id IS NOT NULL THEN 1 END) AS has_sale_dim,
       SUM(CASE WHEN s.sale_id IS NULL THEN 1 END)     AS no_sale_dim
FROM se.data.fact_complete_booking b
         LEFT JOIN se.data.dim_sale s ON b.sale_id = s.sale_id
GROUP BY 1
ORDER BY 1;

SELECT *
FROM se.data.fact_complete_booking;

--old model bookings that have a flight
SELECT booking_id
FROM raw_vault_mvp.cms_mysql.ancillary_product
WHERE product_type = 'FLIGHT'
GROUP BY 1;

SELECT DISTINCT class
FROM raw_vault_mvp.cms_mysql.product_reservation;

--new model bookings that have a flight
SELECT 'A' || reservation_id AS booking_id
FROM raw_vault_mvp.cms_mysql.product_reservation
WHERE class IN ('com.flashsales.reservation.FlightReservation',
                'com.flashsales.reservation.IhpReservation')
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--TB
--query from andy:
/*SELECT
oo.id
FROM
HYGIENE_SNAPSHOT_VAULT_MVP.TRAVELBIRD_MYSQL.ORDERS_ORDER oo
inner join HYGIENE_SNAPSHOT_VAULT_MVP.TRAVELBIRD_MYSQL.ORDERS_ORDERITEMBASE oib on oo.id = oib.ORDER_ID
inner join HYGIENE_SNAPSHOT_VAULT_MVP.TRAVELBIRD_MYSQL.DJANGO_CONTENT_TYPE dct on oib.POLYMORPHIC_CTYPE_ID = dct.ID
WHERE
dct.MODEL = 'flightorderitem'
group by 1;*/
-- SELECT * FROM raw_vault_mvp.travelbird_mysql.django_content_type WHERE model='flightorderitem';
--431 flightorderitem

--tb bookings that have a flight
SELECT 'TB-' || order_id AS booking_id
FROM raw_vault_mvp.travelbird_mysql.orders_orderitembase
WHERE polymorphic_ctype_id = 431
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW se.data.bookings_with_flights AS
(
--se old model bookings with flights
SELECT booking_id::VARCHAR AS booking_id
FROM raw_vault_mvp.cms_mysql.ancillary_product
WHERE product_type = 'FLIGHT'
GROUP BY 1

UNION

--se new model bookings with flights
SELECT 'A' || reservation_id AS booking_id
FROM raw_vault_mvp.cms_mysql.product_reservation
WHERE class IN ('com.flashsales.reservation.FlightReservation',
                'com.flashsales.reservation.IhpReservation')
GROUP BY 1

UNION

--tb bookings with flights
SELECT 'TB-' || order_id AS booking_id
FROM raw_vault_mvp.travelbird_mysql.orders_orderitembase
WHERE polymorphic_ctype_id = 431 -- id: 431 = flightorderitem
GROUP BY 1);


SELECT *
FROM se.data.bookings_with_flights;


SELECT *
FROM (
         SELECT b.booking_id,
                b.booking_status,
                b.sale_id,
                b.check_in_date,
                b.check_out_date,
                b.booking_lead_time_days,
                b.booking_created_date,
                b.booking_completed_date,
                b.commission_ex_vat_gbp,
                b.booking_fee_net_rate_gbp,
                b.payment_surcharge_net_rate_gbp,
                b.insurance_commission_gbp,
                b.margin_gross_of_toms_gbp,
                b.tech_platform,
                s.sale_product,
                s.sale_type,
                s.product_type,
                s.product_configuration,
                s.product_line,
                s.data_model,
                CASE WHEN f.booking_id IS NOT NULL THEN 1 ELSE 0 END AS has_flights

         FROM se.data.fact_complete_booking AS b
                  LEFT JOIN se.data.dim_sale s ON b.sale_id = s.sale_id
                  LEFT JOIN se.data.bookings_with_flights f ON b.booking_id = f.booking_id
         WHERE s.sale_id IS NOT NULL -- temp filter whilst DT fix sale dimensions
           AND b.check_in_date >= current_timestamp::DATE --checkin date is in the future
     )
WHERE product_configuration = 'Hotel'
  AND has_flights = 1;

SELECT s.sale_id,
       s.type,
       s.hotel_chain_link,
       s.closest_airport_code,
       c.is_able_to_sell_flights,
       s.type      AS sale_product, --known as `product` in cube, and `type` in cms
       CASE
           WHEN s.type IN ('PACKAGE', 'TRAVEL') THEN
               CASE
                   WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                   ELSE
                       CASE
                           WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                               THEN 'IHP - dynamic'
                           ELSE
                               CASE
                                   WHEN s.is_team20package = 1 THEN 'IHP - static'
                                   ELSE '3PP'
                                   END
                           END
                   END
           ELSE CASE
                    WHEN s.type = 'HOTEL' THEN
                        CASE
                            WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                            ELSE
                                CASE
                                    WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                        THEN 'Hotel Plus'
                                    ELSE 'Hotel'
                                    END
                            END
                    ELSE 'N/A'
               END
           END     AS sale_type,    --known as sale_type in cube and sale_dimension in cms

       --new naming convention was created to handle known business reporting issues identified by key stakeholders.
       --resulting document formulated and agreed on to handle the issues:
       --https://docs.google.com/presentation/d/1tP1urQuQAzJ1UBYfx06SuaSIvmfR8AlN-kNMJSSgtlk/edit#slide=id.g70c7fa579c_0_8
       CASE
           WHEN s.type = 'HOTEL' THEN 'Hotel'
           WHEN s.type = 'DAY' THEN 'Day Experience'
           WHEN s.type IN ('PACKAGE', 'TRAVEL') THEN 'Package'
           END     AS product_type,


       CASE
           WHEN s.type IN ('PACKAGE', 'TRAVEL') THEN
               CASE
                   WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                   ELSE
                       CASE
                           WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                               THEN 'IHP - dynamic'
                           ELSE
                               CASE
                                   WHEN s.is_team20package = 1 THEN 'IHP - static'
                                   ELSE '3PP'
                                   END
                           END
                   END
           ELSE CASE
                    WHEN s.type = 'HOTEL' THEN
                        CASE
                            WHEN s.hotel_chain_link IS NOT NULL THEN 'WRD'
                            ELSE
                                CASE
                                    WHEN s.closest_airport_code IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                        THEN 'Hotel Plus'
                                    ELSE 'Hotel'
                                    END
                            END
                    ELSE 'N/A'
               END
           END     AS product_configuration,

       'Flash'     AS product_line,
       'Old Model' AS data_model
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale s
--          LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config c ON s.sale_id = c.sale_id
         LEFT JOIN hygiene_vault_mvp.cms_mysql.sale_flight_config c ON s.sale_id = c.sale_id
WHERE s.sale_id = 105488;


CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_vault_mvp.cms_mysql.sale_flight_config;

SELECT *
FROM se.data.dim_sale;

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config
WHERE sale_id = 105488;

SELECT sale_id,
       is_able_to_sell_flights
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config
WHERE is_able_to_sell_flights = TRUE
GROUP BY 1, 2;

SELECT *
FROM (
         SELECT b.booking_id,
                b.sale_id,
--        b.booking_status,
--        b.sale_id,
--        b.check_in_date,
--        b.check_out_date,
--        b.booking_lead_time_days,
--        b.booking_created_date,
--        b.booking_completed_date,
--        b.commission_ex_vat_gbp,
--        b.booking_fee_net_rate_gbp,
--        b.payment_surcharge_net_rate_gbp,
--        b.insurance_commission_gbp,
--        b.margin_gross_of_toms_gbp,
--        b.tech_platform,
--        s.sale_product,
--        s.sale_type,
--        s.product_type,
--        s.product_configuration,
--        s.product_line,
--        s.data_model,
                s.product_configuration,
                CASE WHEN f.booking_id IS NOT NULL THEN 1 ELSE 0 END AS has_flights

         FROM se.data.fact_complete_booking AS b
                  LEFT JOIN se.data.bookings_with_flights f
                            ON b.booking_id = f.booking_id
                  LEFT JOIN se_dev_robin.data.dim_sale s ON b.sale_id = s.sale_id
         WHERE b.check_in_date >= CURRENT_TIMESTAMP::DATE --checkin date is in the future
     )
WHERE product_configuration = 'Hotel'
  AND has_flights = 1
;


SELECT COUNT(*)
FROM (
         )
WHERE product_configuration IS NULL
;

SELECT count(*)
FROM collab.cube.corvid_data2;


SELECT b.booking_id,
       b.sale_id,
       cu.product_configuration,
       CASE WHEN f.booking_id IS NOT NULL THEN 1 ELSE 0 END AS has_flights

FROM se.data.fact_complete_booking AS b
         LEFT JOIN se.data.bookings_with_flights f
                   ON b.booking_id = f.booking_id
         LEFT JOIN collab.cube.covid_data cu ON cu.sale_id = b.sale_id
WHERE b.check_in_date >= CURRENT_TIMESTAMP::DATE --checkin date is in the future
;

SELECT count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
WHERE touch_mkt_channel = 'PPC - Undefined';


------------------------------------------------------------------------------------------------------------------------
--for kevin

--users with abandoned booking in last 30 days
CREATE OR REPLACE VIEW se.data.aban_bk_users AS
(
    SELECT DISTINCT shiro_user_id
    FROM data_vault_mvp.dwh.se_booking
    WHERE booking_created_date >= DATEADD(DAY, -30, current_date)
      AND booking_status = 'ABANDONED'
);


--users that have a abandoned booking and then went on to make a complete booking
CREATE OR REPLACE VIEW se.data.aban_bk_users_w_cmp_bk AS
(
SELECT shiro_user_id
FROM data_vault_mvp.dwh.se_booking
WHERE booking_created_date >= DATEADD(DAY, -30, current_date)
  AND booking_status = 'COMPLETE'
  AND shiro_user_id IN (SELECT shiro_user_id FROM se.data.aban_bk_users)
);


GRANT SELECT ON VIEW se.data.aban_bk_users TO ROLE personal_role__kevinfrench;
GRANT SELECT ON VIEW se.data.aban_bk_users_w_cmp_bk TO ROLE personal_role__kevinfrench;