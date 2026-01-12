SELECT *
-- FROM collab.covid_pii.dflo_view_booking_summary_and_salesforce
FROM collab.covid_pii.dflo_view_booking_summary_and_salesforce_company_email
WHERE COALESCE(adjusted_check_in_date
          , checkin) >= '2022-03-01'
  AND COALESCE(adjusted_check_in_date
          , checkin)
    < '2022-12-31'


-- swap between the two 'FROM' depending on whether or not we need the supplier email address

-- current data is SE booking level on CMS
-- we don't know if this query hold accurate to hotel+ in the sale type
-- double check that hotel+ logic when booking doesn't include flight is categorised as 'Hotel'
-- Add se packages hosted on Travelbird but not including travelist - SE on Travelbird, TB on Travelbird, NOT TL on Travelbird
-- airport code, airline, pnr


SELECT GET_DDL('table', 'collab.covid_pii.dflo_view_booking_summary_and_salesforce_company_email');

SELECT dvbsasce.saleid,
       dvbsasce.transaction_id,
       dvbsasce.adults,
       dvbsasce.children,
       dvbsasce.infants,
       dvbsasce.customer_name,
       dvbsasce.first_name,
       dvbsasce.rest_of_name,
       dvbsasce.customer_email,
       dvbsasce.datebooked,
       dvbsasce.checkin,
       dvbsasce.checkout,
       dvbsasce.adjusted_check_in_date,
       dvbsasce.adjusted_check_out_date,
       dvbsasce.no_nights,
       dvbsasce.rooms,
       dvbsasce.territory,
       dvbsasce.currency,
       dvbsasce.total_sell_rate,
       dvbsasce.customer_total_price_gbp,
       dvbsasce.customer_total_price_sc,
       dvbsasce.customer_total_price_cc,
       dvbsasce.commission_ex_vat,
       dvbsasce.company,
       dvbsasce.supplier,
       dvbsasce.country,
       dvbsasce.division,
       dvbsasce.city,
       dvbsasce.saledimension,
       dvbsasce.flight_buy_rate,
       dvbsasce.dynamic_flight_booked,
       dvbsasce.carrier,
       dvbsasce.arrival_airport,
       dvbsasce.departure_airport_code,
       dvbsasce.lifetime_bookings,
       dvbsasce.lifetime_margin_gbp,
       dvbsasce.bookings_less_13m,
       dvbsasce.bookings_more_13m,
       dvbsasce.booker_segment,
       dvbsasce.booking_id,
       dvbsasce.voucher_stay_by_date,
       dvbsasce.sf_case_thread_id,
       dvbsasce.sf_case_number,
       dvbsasce.sf_case_id,
       dvbsasce.case_overview_number,
       dvbsasce.sf_case_overview_id,
       dvbsasce.sf_priority_type,
       dvbsasce.sf_priority,
       dvbsasce.sf_status,
       dvbsasce.sf_contact_reason,
       dvbsasce.sf_reason,
       dvbsasce.sf_case_owner_full_name,
       dvbsasce.sf_view,
       dvbsasce.date_time_opened,
       dvbsasce.cancelled,
       dvbsasce.refunded,
       dvbsasce.weekly_summary_receiver_emails,
       dvbsasce.customer_support_email,
       dvbsasce.url
FROM collab.covid_pii.dflo_view_booking_summary_and_salesforce_company_email dvbsasce;

CREATE OR REPLACE VIEW dflo_view_booking_summary_and_salesforce_company_email
AS
SELECT msbl.saleid,
       msbl.transaction_id,
       msbl.adults,
       msbl.children,
       msbl.infants,
       msbl.customer_name,
       TO_CHAR(SPLIT(customer_name, ' ')[0])                  AS first_name,
       REGEXP_SUBSTR(customer_name, '\\S+ (.*)', 1, 1, '', 1) AS rest_of_name,
       msbl.customer_email,
       TO_CHAR(msbl.date_booked, 'YYYY-MM-DD')                AS datebooked,
       TO_CHAR(msbl.check_in_date, 'YYYY-MM-DD')              AS checkin,
       TO_CHAR(msbl.check_out_date, 'YYYY-MM-DD')             AS checkout,
       TO_CHAR(msbl.adjusted_check_in_date, 'YYYY-MM-DD')     AS adjusted_check_in_date,
       TO_CHAR(msbl.adjusted_check_out_date, 'YYYY-MM-DD')    AS adjusted_check_out_date,
       msbl.no_nights,
       msbl.rooms,
       msbl.territory,
       msbl.currency,
       msbl.total_sell_rate,
       sb.customer_total_price_gbp,
       sb.customer_total_price_sc,
       sb.customer_total_price_cc,
       msbl.commission_ex_vat,
       msbl.company,
       msbl.supplier,
       msbl.country,
       msbl.division,
       msbl.city,
       CASE
           WHEN LOWER(msbl.sale_dimension) = 'hotel' THEN 'Hotel'
           WHEN LOWER(msbl.sale_dimension) = 'hotelplus' AND LOWER(msbl.dynamic_flight_booked) = 'n' THEN 'Hotel'
           WHEN LOWER(msbl.sale_dimension) = 'ihp - static' AND LOWER(msbl.supplier) NOT LIKE ('secret escapes%') THEN 'Third Party Package'
           WHEN LOWER(msbl.sale_dimension) IN ('ihp - static', 'ihp - connected', 'ihp - dynamic') THEN 'IHP - Packages'
           ELSE msbl.sale_dimension END                       AS saledimension,
       msbl.flight_buy_rate,
       msbl.dynamic_flight_booked,
       msbl.carrier,
       msbl.arrival_airport,
       msbl.departure_airport_code,
       msbl.lifetime_bookings,
       msbl.lifetime_margin_gbp,
       msbl.bookings_less_13m,
       msbl.bookings_more_13m,
       msbl.booker_segment,
       msbl.booking_id,
       TO_CHAR(msbl.voucher_stay_by_date, 'YYYY-MM-DD')       AS voucher_stay_by_date,

       msbl.sf_case_thread_id,
       msbl.sf_case_number,
       msbl.sf_case_id,
       msbl.sf_case_name                                      AS case_overview_number,
       msbl.sf_case_overview_id,
       msbl.sf_priority_type,
       msbl.sf_priority,
       msbl.sf_status,
       msbl.sf_contact_reason,
       msbl.sf_reason,
       msbl.sf_case_owner_full_name,
       msbl.sf_view,

       TO_CHAR(msbl.sf_date_time_opened, 'YYYY-MM-DD')        AS date_time_opened,
       msbl.cancelled,
       msbl.refunded,
       c.weekly_summary_receiver_emails,
       c.customer_support_email,
       sat.se_api_url                                         AS url
FROM se.data_pii.master_se_booking_list msbl
    LEFT JOIN se.data.se_booking sb ON msbl.booking_id = sb.booking_id
    LEFT JOIN se.data.se_sale_attributes sat ON sat.se_sale_id = msbl.saleid
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.company c ON c.id::VARCHAR = sat.company_id;


------------------------------------------------------------------------------------------------------------------------


SELECT sb.sale_id                           AS se_sale_id,
       sb.transaction_id,
       sb.adult_guests,
       sb.child_guests,
       sb.infant_guests,
       sua.first_name || ' ' || sua.surname AS customer_name,
       sua.first_name,
       sua.surname,
       sua.email,
       sb.booking_completed_date,
       sb.original_check_in_date,
       sb.original_check_out_date,
       sb.check_in_date,
       sb.check_out_date,
       sb.no_nights,
       sb.rooms,
       sb.territory,
       sb.currency,
       sb.total_sell_rate_gbp,
       sb.customer_total_price_gbp,
       sb.customer_total_price_sc,
       sb.customer_total_price_cc,
       sb.commission_ex_vat_gbp,
       ss.company_name,
       sb.supplier_name,
       ss.posu_country,
       ss.posu_division,
       ss.posu_city,
       ss.product_configuration,
       sb.has_flights,
       CASE
           WHEN LOWER(sb.sale_dimension) = 'hotel' THEN 'Hotel'
           WHEN LOWER(sb.sale_dimension) = 'hotelplus' AND LOWER(sb.has_flights) = FALSE THEN 'Hotel'
           WHEN LOWER(sb.sale_dimension) = 'ihp - static' AND LOWER(sb.supplier_name) NOT LIKE ('secret escapes%') THEN 'Third Party Package'
           WHEN LOWER(sb.sale_dimension) LIKE 'ihp%' THEN 'IHP - Packages'
           ELSE COALESCE(sb.sale_dimension, ss.product_configuration)
           END                              AS sale_dimension_type,
       sb.flight_buy_rate_gbp,
       sb.flight_carrier,
       sb.flight_supplier_reference,
       sb.sale_closest_airport_code,
       sb.departure_airport_code,
       us.net_bookings,
       us.net_bookings_less_13m,
       us.net_bookings_more_13m,
       us.booker_segment,
       sb.booking_id,
       sb.voucher_stay_by_date,
       rrc.case_thread_id                   AS sf_case_thread_id,
       rrc.case_number                      AS sf_case_number,
       rrc.case_id                          AS sf_case_id,
       rrc.case_name                        AS sf_case_name,
       rrc.case_overview_id                 AS sf_case_overview_id,
       rrc.priority_type                    AS sf_priority_type,
       rrc.priority                         AS sf_priority,
       rrc.status                           AS sf_status,
       rrc.contact_reason                   AS sf_contact_reason,
       rrc.case_owner_full_name             AS sf_case_owner_full_name,
       rrc."VIEW"                           AS sf_view,
       c.weekly_summary_receiver_emails,
       c.customer_support_email,
       ss.se_api_url
FROM data_vault_mvp.dwh.se_booking sb
    LEFT JOIN data_vault_mvp.dwh.user_attributes sua ON sb.shiro_user_id = sua.shiro_user_id
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
    LEFT JOIN data_vault_mvp.dwh.user_segmentation us ON sb.shiro_user_id = us.shiro_user_id AND us.date = CURRENT_DATE - 1
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases rrc ON sb.transaction_id = rrc.transaction_id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.company c ON c.id::VARCHAR = ss.company_id
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED')

UNION ALL

SELECT tb.se_sale_id,
       tb.transaction_id,
       tb.adult_guests,
       tb.child_guests,
       tb.infant_guests,
       sua.first_name || ' ' || sua.surname           AS customer_name,
       sua.first_name,
       sua.surname,
       sua.email,
       tb.created_at_dts                              AS booking_completed_date,
       tb.order_creation_holiday_start_date           AS original_check_in_date,
       tb.order_creation_holiday_end_date             AS original_check_out_date,
       tb.holiday_start_date                          AS check_in_date,
       tb.holiday_end_date                            AS check_out_date,
       tb.no_nights,
       tb.rooms,
       tb.territory,
       tb.sold_price_currency                         AS currency,
       tb.cost_price_total_gbp                        AS total_sell_rate_gbp,
       tb.sold_price_total_gbp,
       tb.sold_price_total_eur,
       tb.sold_price_total_cc,
       COALESCE(tb.sold_price_total_gbp, 0)
           - COALESCE(tb.cost_price_total_gbp, 0)
           - COALESCE(tb.booking_fee_incl_vat_gbp, 0) AS commission_ex_vat_gbp,
       ss.company_name,
       ss.supplier_name,
       ss.posu_country,
       ss.posu_division,
       ss.posu_city,
       ss.product_configuration,
       tb.booking_includes_flight                     AS has_flights,
       'Catalogue'                                    AS sale_dimension_type,
       tb.flight_cost_price_gbp                       AS flight_buy_rate_gbp,
       tb.flight_partner_name_agg                     AS flight_carrier,
       tb.order_flight_reservation_numbers            AS flight_supplier_reference,
       tb.flight_outbound_arrival_airport,
       tb.flight_outbound_departure_airport,
       us.net_bookings,
       us.net_bookings_less_13m,
       us.net_bookings_more_13m,
       us.booker_segment,
       tb.booking_id,
       NULL                                           AS voucher_stay_by_date,
       rrc.case_thread_id                             AS sf_case_thread_id,
       rrc.case_number                                AS sf_case_number,
       rrc.case_id                                    AS sf_case_id,
       rrc.case_name                                  AS sf_case_name,
       rrc.case_overview_id                           AS sf_case_overview_id,
       rrc.priority_type                              AS sf_priority_type,
       rrc.priority                                   AS sf_priority,
       rrc.status                                     AS sf_status,
       rrc.contact_reason                             AS sf_contact_reason,
       rrc.case_owner_full_name                       AS sf_case_owner_full_name,
       rrc."VIEW"                                     AS sf_view,
       c.weekly_summary_receiver_emails,
       c.customer_support_email,
       ss.se_api_url
FROM data_vault_mvp.dwh.tb_booking tb
    LEFT JOIN data_vault_mvp.dwh.user_attributes sua ON tb.shiro_user_id = sua.shiro_user_id
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON tb.se_sale_id = ss.se_sale_id
    LEFT JOIN data_vault_mvp.dwh.user_segmentation us ON tb.shiro_user_id = us.shiro_user_id AND us.date = CURRENT_DATE - 1
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases rrc ON tb.transaction_id = rrc.transaction_id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.company c ON c.id::VARCHAR = ss.company_id
WHERE tb.se_brand IS DISTINCT FROM 'Travelist'
  AND tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
;


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW collab.covid_pii.dflo_view_booking_summary COPY GRANTS AS
WITH tb_airline_details AS (
    SELECT toi.order_id,
           LISTAGG(COALESCE(se.data.airline_name_from_iata_code(toi.flight_validating_airline_id), toi.flight_validating_airline_id), ', ') AS flight_carrier
    FROM data_vault_mvp.dwh.tb_order_item toi
    GROUP BY 1
)

   , stack AS (
    SELECT sb.sale_id                         AS se_sale_id,
           sb.transaction_id,
           sb.booking_status,
           CASE
               WHEN sb.booking_status = 'COMPLETE' THEN 'live'
               WHEN (YEAR(sb.booking_completed_date) = '2019'
                   AND sb.cancellation_date >= '2020-03-01'
                   AND sb.booking_status IN ('CANCELLED', 'REFUNDED')) THEN 'live'
               WHEN sb.booking_status IN ('CANCELLED', 'REFUNDED') THEN 'cancelled'
               WHEN sb.booking_status = 'ABANDONED' THEN 'abandoned'
               ELSE 'other'
               END                            AS booking_status_type,
           sb.adult_guests,
           sb.child_guests,
           sb.infant_guests,
           sb.booking_completed_date,
           sb.original_check_in_date,
           sb.original_check_out_date,
           sb.check_in_date,
           sb.check_out_date,
           sb.no_nights,
           sb.rooms,
           sb.territory,
           sb.currency,
           sb.customer_total_price_gbp,
           sb.customer_total_price_cc,
           sb.commission_ex_vat_gbp,
           sb.supplier_name,
           sb.has_flights,
           CASE
               WHEN LOWER(sb.sale_dimension) = 'hotel' THEN 'Hotel'
               WHEN LOWER(sb.sale_dimension) = 'hotelplus' AND LOWER(sb.has_flights) = FALSE THEN 'Hotel'
               WHEN LOWER(sb.sale_dimension) = 'ihp - static' AND LOWER(sb.supplier_name) NOT LIKE ('secret escapes%') THEN 'Third Party Package'
               WHEN LOWER(sb.sale_dimension) LIKE 'ihp%' THEN 'IHP - Packages'
               ELSE sb.sale_dimension
               END                            AS sale_dimension_type,
           sb.flight_buy_rate_gbp,
           sb.flight_carrier,
           sb.flight_supplier_reference,
           sb.sale_closest_airport_code,
           sb.departure_airport_code,
           sb.booking_id,
           sb.voucher_stay_by_date,
           sb.shiro_user_id,
           bs.customer_email,
           bs.record__o['firstName']::VARCHAR AS first_name,
           bs.record__o['lastName']::VARCHAR  AS last_name,
           'SECRET_ESCAPES'                   AS tech_platform

    FROM data_vault_mvp.dwh.se_booking sb
        LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.transaction_id = bs.transaction_id
    WHERE sb.se_brand = 'SE Brand'
      AND sb.booking_status IN ('COMPLETE', 'REFUNDED')

    UNION ALL

    SELECT tb.se_sale_id,
           tb.transaction_id,
           tb.payment_status,
           CASE
               WHEN tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE') THEN 'live'
               WHEN (YEAR(tb.created_at_dts) = '2019'
                   AND tb.cancellation_date >= '2020-03-01'
                   AND tb.payment_status = 'CANCELLED') THEN 'live'
               WHEN tb.payment_status = 'CANCELLED' THEN 'cancelled'
               WHEN tb.payment_status = 'FINISHED' THEN 'abandoned'
               ELSE 'other'
               END                                        AS booking_status_type,
           tb.adult_guests,
           tb.child_guests,
           tb.infant_guests,
           tb.created_at_dts                              AS booking_completed_date,
           tb.order_creation_holiday_start_date           AS original_check_in_date,
           tb.order_creation_holiday_end_date             AS original_check_out_date,
           tb.holiday_start_date                          AS check_in_date,
           tb.holiday_end_date                            AS check_out_date,
           tb.no_nights,
           tb.rooms,
           tb.territory,
           tb.sold_price_currency                         AS currency,
           tb.sold_price_total_gbp,
           tb.sold_price_total_cc,
           COALESCE(tb.sold_price_total_gbp, 0)
               - COALESCE(tb.cost_price_total_gbp, 0)
               - COALESCE(tb.booking_fee_incl_vat_gbp, 0) AS commission_ex_vat_gbp,
           tb.order_partners                              AS supplier_name,
           tb.booking_includes_flight                     AS has_flights,
           'Catalogue'                                    AS sale_dimension_type,
           tb.flight_cost_price_gbp                       AS flight_buy_rate_gbp,
           tad.flight_carrier                             AS flight_carrier,
           tb.order_flight_reservation_numbers            AS flight_supplier_reference,
           tb.flight_outbound_arrival_airport,
           tb.flight_outbound_departure_airport,
           tb.booking_id,
           NULL                                           AS voucher_stay_by_date,
           tb.shiro_user_id,
           op.email,
           op.first_name,
           op.last_name,
           'TRAVELBIRD'                                   AS tech_platform
    FROM data_vault_mvp.dwh.tb_booking tb
        LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_person op ON tb.customer_id = op.id
        LEFT JOIN tb_airline_details tad ON tb.order_id = tad.order_id
    WHERE tb.se_brand IS DISTINCT FROM 'Travelist'
      AND tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
)
SELECT s.se_sale_id,
       s.transaction_id,
       s.booking_status,
       s.booking_status_type,
       s.adult_guests,
       s.child_guests,
       s.infant_guests,
       COALESCE(sua.first_name, s.first_name) || ' ' || COALESCE(sua.surname, s.last_name) AS customer_name,
       COALESCE(sua.first_name, s.first_name)                                              AS first_name,
       COALESCE(sua.surname, s.last_name)                                                  AS last_name,
       COALESCE(sua.email, s.customer_email)                                               AS email, -- NOTE: This column is considered PII
       sua.membership_account_status,
       s.booking_completed_date,
       s.original_check_in_date,
       s.original_check_out_date,
       s.check_in_date,
       s.check_out_date,
       s.no_nights,
       s.rooms,
       s.territory,
       s.currency,
       s.customer_total_price_gbp,
       s.customer_total_price_cc,
       s.commission_ex_vat_gbp,
       ss.company_name,
       s.supplier_name,
       ss.posu_country,
       ss.posu_division,
       ss.posu_city,
       ss.product_configuration,
       s.has_flights,
       COALESCE(s.sale_dimension_type, ss.product_configuration)                           AS sale_dimension_type,
       s.flight_buy_rate_gbp,
       s.flight_carrier,
       s.flight_supplier_reference,
       s.sale_closest_airport_code,
       s.departure_airport_code,
       us.margin_segment,
       s.booking_id,
       s.voucher_stay_by_date,
       rrc.case_thread_id                                                                  AS sf_case_thread_id,
       rrc.case_number                                                                     AS sf_case_number,
       rrc.case_id                                                                         AS sf_case_id,
       rrc.case_name                                                                       AS sf_case_name,
       rrc.case_overview_id                                                                AS sf_case_overview_id,
       rrc.priority_type                                                                   AS sf_priority_type,
       rrc.priority                                                                        AS sf_priority,
       rrc.status                                                                          AS sf_status,
       rrc.contact_reason                                                                  AS sf_contact_reason,
       rrc.case_owner_full_name                                                            AS sf_case_owner_full_name,
       rrc."VIEW"                                                                          AS sf_view,
       c.weekly_summary_receiver_emails,-- NOTE: This column is considered PII
       c.customer_support_email,                                                                     -- NOTE: This column is considered PII
       ss.se_api_url,
       s.shiro_user_id,

       s.tech_platform
FROM stack s
    LEFT JOIN data_vault_mvp.dwh.user_attributes sua ON s.shiro_user_id = sua.shiro_user_id
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON s.se_sale_id = ss.se_sale_id
    LEFT JOIN data_vault_mvp.dwh.user_segmentation us ON s.shiro_user_id = us.shiro_user_id AND us.date = CURRENT_DATE - 1
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases rrc ON s.transaction_id = rrc.transaction_id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.company c ON c.id::VARCHAR = ss.company_id;


SELECT *
FROM se.data.user_segmentation us
WHERE us.date = CURRENT_DATE - 1

SELECT *
FROM collab.covid_pii.dflo_view_booking_summary
WHERE check_in_date >= '2022-03-01'
  AND check_in_date < '2022-12-31';


SELECT *
FROM se.data.tb_order_item toi
WHERE toi.order_id = 21911318;

-- confirm if no of nights includes departure/arrival flights


SELECT *
FROM hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines;


SELECT *
FROM se.data.tb_booking tb;