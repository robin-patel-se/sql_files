SELECT *
FROM collab.covid_pii.dflo_view_booking_summary
WHERE tech_platform = 'TRAVELBIRD';

SELECT GET_DDL('table', 'collab.covid_pii.dflo_view_booking_summary');


CREATE OR REPLACE VIEW collab.covid_pii.dflo_view_booking_summary COPY GRANTS AS
WITH tb_airline_details AS (
    SELECT
        toi.order_id,
        LISTAGG(COALESCE(se.data.airline_name_from_iata_code(toi.flight_validating_airline_id), toi.flight_validating_airline_id), ', ') AS flight_carrier
    FROM data_vault_mvp.dwh.tb_order_item toi
    GROUP BY 1
)
   , stack AS (
    SELECT
        sb.sale_id                         AS se_sale_id,
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

    SELECT
        tb.se_sale_id,
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
SELECT
    s.se_sale_id,
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
    LEFT JOIN /*latest_vault.sfsc.rebooking_request_cases*/ data_vault_mvp.dwh.sfsc__rebooking_request_cases rrc ON s.transaction_id = rrc.transaction_id
    LEFT JOIN latest_vault.cms_mysql.company c ON c.id::VARCHAR = ss.company_id;

------------------------------------------------------------------------------------------------------------------------
-- ingest new dataset from tracy
python biapp/
bau/
manifests/
generate_manifest_from_sql_table.py
\
    --connector 'travelbird_mysql' \
    --table_names 'partners_partneremail' \
    --mode 'incremental' \
    --start_date '2022-10-26 00:00:00'

dataset_task --include 'travelbird_mysql.partners_partneremail' --operation LatestRecordsOperation --method 'run' --upstream --start '2022-10-25 00:30:00' --end '2022-10-25 00:30:00'

2022-08-20


SELECT *
FROM latest_vault_dev_robin.travelbird_mysql.partners_partneremail;

airflow dags backfill --start-date '2022-08-20 00:00:00' --end-date '2022-08-21 00:00:00' incoming__travelbird_mysql__partners_partneremail__daily_at_00h30
airflow dags backfill --m --start-date '2022-10-27 00:00:00' --end-date '2022-10-28 00:00:00' incoming__travelbird_mysql__partners_partneremail__daily_at_00h30

SELECT *
FROM latest_vault.travelbird_mysql.partners_partneremail;

SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM data_vault_mvp.dwh.tb_order_item toi
    INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo ON toi.order_id = oo.id
    INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.partners_partner pp ON toi.partner_id = pp.id
    LEFT JOIN  latest_vault.travelbird_mysql.partners_partneremail ppe ON pp.id = ppe.partner_id AND ppe.email_type = 'Customer Service'
WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')


WITH customer_support_email_order_item AS (
    SELECT
        toi.order_id,
        toi.order_item_id,
        toi.order_item_type,
        toi.partner_name,
        pp.email                      AS partner_default_email,
        ppe.email                     AS partner_customer_service_email,
        ppe.email_type,
        COALESCE(ppe.email, pp.email) AS customer_support_email
    FROM data_vault_mvp.dwh.tb_order_item toi
        INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo ON toi.order_id = oo.id
        INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.partners_partner pp ON toi.partner_id = pp.id
        LEFT JOIN  latest_vault.travelbird_mysql.partners_partneremail ppe ON pp.id = ppe.partner_id AND ppe.email_type = 'Customer Service'
    WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
      AND toi.order_item_type IN
          (
           'FLIGHT',
           'ACCOMMODATION',
           'LEISURE',
           'TRANSFER',
           'TOUR_SERVICE',
           'TOUR',
           'CAR')
),
     distinct_partner_details AS (
         SELECT DISTINCT
             cseoi.order_id,
             REPLACE(cseoi.order_item_type, '_', ' ') AS order_item_type,
             cseoi.partner_name,
             cseoi.customer_support_email
         FROM customer_support_email_order_item cseoi
     )
SELECT
    dpd.order_id,
    LISTAGG(INITCAP(dpd.order_item_type) || ': ' || dpd.partner_name || ' - Email: ' || dpd.customer_support_email || CHR(13)) AS customer_support_email
FROM distinct_partner_details dpd
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
-- CREATE OR REPLACE VIEW collab.covid_pii.dflo_view_booking_summary COPY GRANTS AS
WITH tb_airline_details AS (
    SELECT
        toi.order_id,
        LISTAGG(COALESCE(se.data.airline_name_from_iata_code(toi.flight_validating_airline_id), toi.flight_validating_airline_id), ', ') AS flight_carrier
    FROM data_vault_mvp.dwh.tb_order_item toi
    GROUP BY 1
),
     customer_support_email_order_item AS (
         -- obtain the customer support email at order item level from tracy tables
         SELECT
             toi.booking_id,
             toi.order_item_id,
             toi.order_item_type,
             toi.partner_name,
             pp.email                      AS partner_default_email,
             ppe.email                     AS partner_customer_service_email,
             ppe.email_type,
             COALESCE(ppe.email, pp.email) AS customer_support_email
         FROM data_vault_mvp.dwh.tb_order_item toi
             INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo ON toi.order_id = oo.id
             INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.partners_partner pp ON toi.partner_id = pp.id
             LEFT JOIN  latest_vault.travelbird_mysql.partners_partneremail ppe ON pp.id = ppe.partner_id AND ppe.email_type = 'Customer Service'
         WHERE oo.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
           AND toi.order_item_type IN
               (
                'ACCOMMODATION',
                'LEISURE',
                'TRANSFER',
                'TOUR'
                   )
     ),
     distinct_partner_details AS (
         -- remove duplicates amongst an order
         SELECT DISTINCT
             cseoi.booking_id,
             REPLACE(cseoi.order_item_type, '_', ' ') AS order_item_type,
             cseoi.partner_name,
             cseoi.customer_support_email
         FROM customer_support_email_order_item cseoi
     ),
     aggregate_tb_partner_emails AS (
         -- aggregate up to order level with new line breaks
         SELECT
             dpd.booking_id,
             LISTAGG(INITCAP(dpd.order_item_type) || ': ' || dpd.partner_name || ' - Email: ' || dpd.customer_support_email, ' | ') AS customer_support_email
         FROM distinct_partner_details dpd
         GROUP BY 1
     ),
     stack AS (
         SELECT
             sb.sale_id                         AS se_sale_id,
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
         SELECT
             tb.se_sale_id,
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
SELECT
    s.se_sale_id,
    s.transaction_id,
    s.booking_status,
    s.booking_status_type,
    s.adult_guests,
    s.child_guests,
    s.infant_guests,
    COALESCE(sua.first_name, s.first_name) || ' ' || COALESCE(sua.surname, s.last_name) AS customer_name,
    COALESCE(sua.first_name, s.first_name)                                              AS first_name,
    COALESCE(sua.surname, s.last_name)                                                  AS last_name,
    COALESCE(sua.email, s.customer_email)                                               AS email,                  -- NOTE: This column is considered PII
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
    COALESCE(c.customer_support_email, atpe.customer_support_email)                     AS customer_support_email, -- NOTE: This column is considered PII
    ss.se_api_url,
    s.shiro_user_id,

    s.tech_platform
FROM stack s
    LEFT JOIN data_vault_mvp.dwh.user_attributes sua ON s.shiro_user_id = sua.shiro_user_id
    LEFT JOIN data_vault_mvp.dwh.se_sale ss ON s.se_sale_id = ss.se_sale_id
    LEFT JOIN data_vault_mvp.dwh.user_segmentation us ON s.shiro_user_id = us.shiro_user_id AND us.date = CURRENT_DATE - 1
    LEFT JOIN /*latest_vault.sfsc.rebooking_request_cases*/ data_vault_mvp.dwh.sfsc__rebooking_request_cases rrc ON s.transaction_id = rrc.transaction_id
    LEFT JOIN latest_vault.cms_mysql.company c ON c.id::VARCHAR = ss.company_id
    LEFT JOIN aggregate_tb_partner_emails atpe ON s.booking_id = atpe.booking_id
WHERE tech_platform = 'TRAVELBIRD'
;

SELECT * FROM collab.covid_pii.dflo_view_booking_summary dvbs WHERE tech_platform = 'TRAVELBIRD';