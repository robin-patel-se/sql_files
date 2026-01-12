WITH bookings AS (
    SELECT sb.booking_id,
           sb.booking_status,
           sb.sale_id,
           sb.shiro_user_id,

           sb.check_in_date,
           sb.check_out_date,
           sb.booking_lead_time_days,
           sb.booking_created_date,
           sb.booking_completed_date,
           sb.booking_completed_date AS booking_transaction_completed_date,

           sb.customer_total_price_gbp,
           sb.customer_total_price_gbp_constant_currency,
           sb.gross_booking_value_gbp,
           sb.commission_ex_vat_gbp,
           sb.booking_fee_net_rate_gbp,
           sb.payment_surcharge_net_rate_gbp,
           sb.insurance_commission_gbp,

           sb.margin_gross_of_toms_gbp,
           sb.margin_gross_of_toms_gbp_constant_currency,
           sb.margin_gross_of_toms_eur_constant_currency,
           sb.no_nights,
           sb.adult_guests,
           sb.child_guests,
           sb.infant_guests,
           sb.price_per_night,
           sb.price_per_person_per_night,
           sb.rooms,
           sb.cancellation_date,
           TRUE                      AS booking_transaction_complete

    FROM se.data.se_booking sb
--     WHERE sb.booking_status IN ('COMPLETE', 'CANCELLED', 'REFUNDED')

    UNION ALL

    SELECT tb.booking_id,
           tb.payment_status                              AS booking_status,
           tb.se_sale_id                                  AS sale_id,
           tb.shiro_user_id,

           tb.travel_date                                 AS check_in_date,
           tb.return_date                                 AS check_out_date,
           tb.booking_lead_time_days,
           tb.created_at_dts::DATE                        AS booking_created_date,
           tb.created_at_dts::DATE                        AS booking_completed_date,
           tb.complete_date                               AS booking_transaction_completed_date,

           tb.sold_price_total_gbp                        AS customer_total_price_gbp,
           tb.sold_price_total_gbp_constant_currency      AS customer_total_price_gbp_constant_currency,
           tb.sold_price_total_gbp                        AS gross_booking_value_gbp, --TODO need to revisit this logic
           COALESCE(tb.sold_price_total_gbp, 0)
               - COALESCE(tb.cost_price_total_gbp, 0)
               - COALESCE(tb.booking_fee_incl_vat_gbp, 0) AS commission_ex_vat_gbp,
           COALESCE(tb.booking_fee_incl_vat_gbp, 0)
               - COALESCE(tb.booking_fee_vat_gbp, 0)      AS booking_fee_net_rate_gbp,
           0                                              AS payment_surcharge_net_rate_gbp,
           0                                              AS insurance_commission_gbp,

           tb.margin_gbp                                  AS margin_gross_of_toms_gbp,
           tb.margin_gbp_constant_currency                AS margin_gross_of_toms_gbp_constant_currency,
           tb.margin_eur                                  AS margin_gross_of_toms_eur_constant_currency,
           tb.no_nights                                   AS no_nights,
           tb.adult_guests,
           tb.child_guests,
           tb.infant_guests,
           tb.price_per_night,
           tb.price_per_person_per_night,
           1                                              AS rooms,                   --TODO need to revisit logic to calc rooms on TB
           tb.updated_at_dts                              AS cancellation_date,
           IFF(tb.complete_date IS NOT NULL, TRUE, FALSE) AS booking_transaction_complete

    FROM se.data.tb_booking tb
--     WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
),
     union_bookings AS (

         SELECT gb.booking_completed_date                                                                    AS date,
                'gross'                                                                                      AS reporting_status,
                gb.booking_id,
                gb.booking_status                                                                            AS booking_status__o,
                IFF(gb.booking_status IN ('COMPLETE', 'AUTHORISED', 'PARTIAL_PAID', 'LATE'), 'live', 'canx') AS booking_status,
                gb.sale_id,
                gb.shiro_user_id,
                gb.check_in_date,
                gb.check_out_date,
                gb.booking_lead_time_days,
                gb.booking_created_date,
                gb.booking_completed_date,
                gb.booking_transaction_completed_date,
                gb.customer_total_price_gbp,
                gb.customer_total_price_gbp_constant_currency,
                gb.commission_ex_vat_gbp,
                gb.margin_gross_of_toms_gbp,
                gb.margin_gross_of_toms_gbp_constant_currency,
                gb.margin_gross_of_toms_eur_constant_currency,
                gb.no_nights,
                gb.adult_guests,
                gb.child_guests,
                gb.infant_guests,
                gb.price_per_night,
                gb.price_per_person_per_night,
                gb.rooms,
                gb.cancellation_date,
                gb.booking_transaction_complete
         FROM bookings gb
         WHERE gb.booking_status IN ('COMPLETE', 'CANCELLED', 'REFUNDED', 'AUTHORISED', 'PARTIAL_PAID', 'LATE',
                                     'CANCELLED') --all bookings that have ever confirmed

         UNION ALL

         SELECT cb.cancellation_date AS date,
                'cancelled'          AS reporting_status,
                cb.booking_id,
                cb.booking_status    AS booking_status__o,
                NULL                 AS booking_status,
                cb.sale_id,
                cb.shiro_user_id,
                cb.check_in_date,
                cb.check_out_date,
                cb.booking_lead_time_days,
                cb.booking_created_date,
                cb.booking_completed_date,
                cb.booking_transaction_completed_date,
                cb.customer_total_price_gbp,
                cb.customer_total_price_gbp_constant_currency,
                cb.commission_ex_vat_gbp,
                cb.margin_gross_of_toms_gbp,
                cb.margin_gross_of_toms_gbp_constant_currency,
                cb.margin_gross_of_toms_eur_constant_currency,
                cb.no_nights,
                cb.adult_guests,
                cb.child_guests,
                cb.infant_guests,
                cb.price_per_night,
                cb.price_per_person_per_night,
                cb.rooms,
                cb.cancellation_date,
                cb.booking_transaction_complete
         FROM bookings cb
         WHERE cb.booking_status IN ('CANCELLED', 'REFUNDED') --all bookings that are cancelled
     )
SELECT ub.*,
       ds.*
FROM union_bookings ub
         LEFT JOIN se.data.dim_sale ds ON ub.sale_id = ds.se_sale_id;


SELECT booking_status, count(*)
FROM se.data.se_booking
GROUP BY 1;
SELECT tb.payment_status, count(*)
FROM se.data.tb_booking tb
GROUP BY 1;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

self_describing_task --include 'se/data/dwh/fact_booking.py'  --method 'run' --start '2020-11-16 00:00:00' --end '2020-11-16 00:00:00'
self_describing_task --include 'se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2020-11-16 00:00:00' --end '2020-11-16 00:00:00'

SELECT *
FROM se_dev_robin.data.fact_booking;

SELECT COUNT(*),
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency),
       SUM(fcb.no_nights),
       SUM(fcb.margin_gross_of_toms_gbp),
       'prod' AS platform
FROM se.data.fact_complete_booking fcb
UNION ALL
SELECT COUNT(*),
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency),
       SUM(fcb.no_nights),
       SUM(fcb.margin_gross_of_toms_gbp),
       'dev' AS platform
FROM se_dev_robin.data.fact_complete_booking fcb;


SELECT *
FROM se.data.tb_booking tb;

CREATE SCHEMA data_vault_mvp_dev_robin.travelbird_cms;
CREATE SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.django_content_type CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.django_content_type;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.currency_exchangerateupdate CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.currency_exchangerateupdate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.currency_currency_snapshot CLONE data_vault_mvp.travelbird_cms.currency_currency_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_person_snapshot CLONE data_vault_mvp.travelbird_cms.orders_person_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_orderproperty_snapshot CLONE data_vault_mvp.travelbird_cms.orders_orderproperty_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;

------------------------------------------------------------------------------------------------------------------------


WITH bookings AS (
    SELECT sb.booking_id,
           sb.booking_status,
           sb.sale_id,
           sb.shiro_user_id,

           sb.check_in_date,
           sb.check_out_date,
           sb.booking_lead_time_days,
           sb.booking_created_date,
           sb.booking_completed_date,
           sb.booking_completed_date AS booking_transaction_completed_date,

           sb.customer_total_price_gbp,
           sb.customer_total_price_gbp_constant_currency,
           sb.gross_booking_value_gbp,
           sb.commission_ex_vat_gbp,
           sb.booking_fee_net_rate_gbp,
           sb.payment_surcharge_net_rate_gbp,
           sb.insurance_commission_gbp,

           sb.margin_gross_of_toms_gbp,
           sb.margin_gross_of_toms_gbp_constant_currency,
           sb.margin_gross_of_toms_eur_constant_currency,
           sb.no_nights,
           sb.adult_guests,
           sb.child_guests,
           sb.infant_guests,
           sb.price_per_night,
           sb.price_per_person_per_night,
           sb.rooms,
           sb.cancellation_date,
           sb.territory              AS posa_territory,
           TRUE                      AS booking_transaction_complete

    FROM se.data.se_booking sb
--     WHERE sb.booking_status IN ('COMPLETE', 'CANCELLED', 'REFUNDED')

    UNION ALL

    SELECT tb.booking_id,
           tb.payment_status                              AS booking_status,
           tb.se_sale_id                                  AS sale_id,
           tb.shiro_user_id,

           tb.travel_date                                 AS check_in_date,
           tb.return_date                                 AS check_out_date,
           tb.booking_lead_time_days,
           tb.created_at_dts::DATE                        AS booking_created_date,
           tb.created_at_dts::DATE                        AS booking_completed_date,
           tb.complete_date                               AS booking_transaction_completed_date,

           tb.sold_price_total_gbp                        AS customer_total_price_gbp,
           tb.sold_price_total_gbp_constant_currency      AS customer_total_price_gbp_constant_currency,
           tb.sold_price_total_gbp                        AS gross_booking_value_gbp, --TODO need to revisit this logic
           COALESCE(tb.sold_price_total_gbp, 0)
               - COALESCE(tb.cost_price_total_gbp, 0)
               - COALESCE(tb.booking_fee_incl_vat_gbp, 0) AS commission_ex_vat_gbp,
           COALESCE(tb.booking_fee_incl_vat_gbp, 0)
               - COALESCE(tb.booking_fee_vat_gbp, 0)      AS booking_fee_net_rate_gbp,
           0                                              AS payment_surcharge_net_rate_gbp,
           0                                              AS insurance_commission_gbp,

           tb.margin_gbp                                  AS margin_gross_of_toms_gbp,
           tb.margin_gbp_constant_currency                AS margin_gross_of_toms_gbp_constant_currency,
           tb.margin_eur                                  AS margin_gross_of_toms_eur_constant_currency,
           tb.no_nights                                   AS no_nights,
           tb.adult_guests,
           tb.child_guests,
           tb.infant_guests,
           tb.price_per_night,
           tb.price_per_person_per_night,
           1                                              AS rooms,                   --TODO need to revisit logic to calc rooms on TB
           tb.updated_at_dts                              AS cancellation_date,


    FROM se.data.tb_booking tb
--     WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
),

     union_bookings AS (

         SELECT gb.booking_completed_date                                                                    AS date,
                'gross'                                                                                      AS reporting_status,
                gb.booking_id,
                gb.booking_status                                                                            AS booking_status__o,
                IFF(gb.booking_status IN ('COMPLETE', 'AUTHORISED', 'PARTIAL_PAID', 'LATE'), 'live', 'canx') AS booking_status,
                gb.sale_id,
                gb.shiro_user_id,
                gb.check_in_date,
                gb.check_out_date,
                gb.booking_lead_time_days,
                gb.booking_created_date,
                gb.booking_completed_date,
                gb.booking_transaction_completed_date,
                gb.customer_total_price_gbp,
                gb.customer_total_price_gbp_constant_currency,
                gb.commission_ex_vat_gbp,
                gb.margin_gross_of_toms_gbp,
                gb.margin_gross_of_toms_gbp_constant_currency,
                gb.margin_gross_of_toms_eur_constant_currency,
                gb.no_nights,
                gb.adult_guests,
                gb.child_guests,
                gb.infant_guests,
                gb.price_per_night,
                gb.price_per_person_per_night,
                gb.rooms,
                gb.cancellation_date,
                gb.booking_transaction_complete
         FROM bookings gb
         WHERE gb.booking_status IN ('COMPLETE', 'CANCELLED', 'REFUNDED', 'AUTHORISED', 'PARTIAL_PAID', 'LATE',
                                     'CANCELLED') --all bookings that have ever confirmed

         UNION ALL

         SELECT cb.cancellation_date AS date,
                'cancelled'          AS reporting_status,
                cb.booking_id,
                cb.booking_status    AS booking_status__o,
                NULL                 AS booking_status,
                cb.sale_id,
                cb.shiro_user_id,
                cb.check_in_date,
                cb.check_out_date,
                cb.booking_lead_time_days,
                cb.booking_created_date,
                cb.booking_completed_date,
                cb.booking_transaction_completed_date,
                cb.customer_total_price_gbp,
                cb.customer_total_price_gbp_constant_currency,
                cb.commission_ex_vat_gbp,
                cb.margin_gross_of_toms_gbp,
                cb.margin_gross_of_toms_gbp_constant_currency,
                cb.margin_gross_of_toms_eur_constant_currency,
                cb.no_nights,
                cb.adult_guests,
                cb.child_guests,
                cb.infant_guests,
                cb.price_per_night,
                cb.price_per_person_per_night,
                cb.rooms,
                cb.cancellation_date,
                cb.booking_transaction_complete
         FROM bookings cb

         WHERE cb.booking_status IN ('CANCELLED', 'REFUNDED') --all bookings that are cancelled
     )
SELECT ub.*,
       sc.se_week  AS week,
       csc.se_week AS check_in_week
FROM union_bookings ub
         LEFT JOIN se.data.se_calendar sc ON ub.date = sc.date_value
         LEFT JOIN se.data.se_calendar csc ON ub.check_in_date = csc.date_value;


WITH bookings AS (
    SELECT sb.booking_id,
           sb.booking_status,
           sb.sale_id,
           sb.shiro_user_id,

           sb.check_in_date,
           sb.check_out_date,
           sb.booking_lead_time_days,
           sb.booking_created_date,
           sb.booking_completed_date,
           sb.booking_completed_date AS booking_transaction_completed_date,

           sb.customer_total_price_gbp,
           sb.customer_total_price_gbp_constant_currency,
           sb.gross_booking_value_gbp,
           sb.commission_ex_vat_gbp,
           sb.booking_fee_net_rate_gbp,
           sb.payment_surcharge_net_rate_gbp,
           sb.insurance_commission_gbp,

           sb.margin_gross_of_toms_gbp,
           sb.margin_gross_of_toms_gbp_constant_currency,
           sb.margin_gross_of_toms_eur_constant_currency,
           sb.no_nights,
           sb.adult_guests,
           sb.child_guests,
           sb.infant_guests,
           sb.price_per_night,
           sb.price_per_person_per_night,
           sb.rooms,
           sb.cancellation_date,
           TRUE                      AS booking_transaction_complete

    FROM se.data.se_booking sb
--     WHERE sb.booking_status IN ('COMPLETE', 'CANCELLED', 'REFUNDED')

    UNION ALL

    SELECT tb.booking_id,
           tb.payment_status                              AS booking_status,
           tb.se_sale_id                                  AS sale_id,
           tb.shiro_user_id,

           tb.travel_date                                 AS check_in_date,
           tb.return_date                                 AS check_out_date,
           tb.booking_lead_time_days,
           tb.created_at_dts::DATE                        AS booking_created_date,
           tb.created_at_dts::DATE                        AS booking_completed_date,
           tb.complete_date                               AS booking_transaction_completed_date,

           tb.sold_price_total_gbp                        AS customer_total_price_gbp,
           tb.sold_price_total_gbp_constant_currency      AS customer_total_price_gbp_constant_currency,
           tb.sold_price_total_gbp                        AS gross_booking_value_gbp, --TODO need to revisit this logic
           COALESCE(tb.sold_price_total_gbp, 0)
               - COALESCE(tb.cost_price_total_gbp, 0)
               - COALESCE(tb.booking_fee_incl_vat_gbp, 0) AS commission_ex_vat_gbp,
           COALESCE(tb.booking_fee_incl_vat_gbp, 0)
               - COALESCE(tb.booking_fee_vat_gbp, 0)      AS booking_fee_net_rate_gbp,
           0                                              AS payment_surcharge_net_rate_gbp,
           0                                              AS insurance_commission_gbp,

           tb.margin_gbp                                  AS margin_gross_of_toms_gbp,
           tb.margin_gbp_constant_currency                AS margin_gross_of_toms_gbp_constant_currency,
           tb.margin_eur                                  AS margin_gross_of_toms_eur_constant_currency,
           tb.no_nights                                   AS no_nights,
           tb.adult_guests,
           tb.child_guests,
           tb.infant_guests,
           tb.price_per_night,
           tb.price_per_person_per_night,
           1                                              AS rooms,                   --TODO need to revisit logic to calc rooms on TB
           tb.updated_at_dts                              AS cancellation_date,
           IFF(tb.complete_date IS NOT NULL, TRUE, FALSE) AS booking_transaction_complete

    FROM se.data.tb_booking tb
--     WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE', 'CANCELLED')
),
     union_bookings AS (

         SELECT gb.booking_completed_date                                                                    AS date,
                'gross'                                                                                      AS reporting_status,
                gb.booking_id,
                gb.booking_status                                                                            AS booking_status__o,
                IFF(gb.booking_status IN ('COMPLETE', 'AUTHORISED', 'PARTIAL_PAID', 'LATE'), 'live', 'canx') AS booking_status,
                gb.sale_id,
                gb.shiro_user_id,
                gb.check_in_date,
                gb.check_out_date,
                gb.booking_lead_time_days,
                gb.booking_created_date,
                gb.booking_completed_date,
                gb.booking_transaction_completed_date,
                gb.customer_total_price_gbp,
                gb.customer_total_price_gbp_constant_currency,
                gb.commission_ex_vat_gbp,
                gb.margin_gross_of_toms_gbp,
                gb.margin_gross_of_toms_gbp_constant_currency,
                gb.margin_gross_of_toms_eur_constant_currency,
                gb.no_nights,
                gb.adult_guests,
                gb.child_guests,
                gb.infant_guests,
                gb.price_per_night,
                gb.price_per_person_per_night,
                gb.rooms,
                gb.cancellation_date,
                gb.booking_transaction_complete
         FROM bookings gb
         WHERE gb.booking_status IN ('COMPLETE', 'CANCELLED', 'REFUNDED', 'AUTHORISED', 'PARTIAL_PAID', 'LATE',
                                     'CANCELLED') --all bookings that have ever confirmed

         UNION ALL

         SELECT cb.cancellation_date AS date,
                'cancelled'          AS reporting_status,
                cb.booking_id,
                cb.booking_status    AS booking_status__o,
                NULL                 AS booking_status,
                cb.sale_id,
                cb.shiro_user_id,
                cb.check_in_date,
                cb.check_out_date,
                cb.booking_lead_time_days,
                cb.booking_created_date,
                cb.booking_completed_date,
                cb.booking_transaction_completed_date,
                cb.customer_total_price_gbp,
                cb.customer_total_price_gbp_constant_currency,
                cb.commission_ex_vat_gbp,
                cb.margin_gross_of_toms_gbp,
                cb.margin_gross_of_toms_gbp_constant_currency,
                cb.margin_gross_of_toms_eur_constant_currency,
                cb.no_nights,
                cb.adult_guests,
                cb.child_guests,
                cb.infant_guests,
                cb.price_per_night,
                cb.price_per_person_per_night,
                cb.rooms,
                cb.cancellation_date,
                cb.booking_transaction_complete
         FROM bookings cb
         WHERE cb.booking_status IN ('CANCELLED', 'REFUNDED') --all bookings that are cancelled
     )
SELECT ub.*,
       sc.se_week                                              AS week,
       csc.se_week                                             AS check_in_week,
       ds.posa_territory,
       se.data.posa_category_from_territory(ds.posa_territory) AS posa_category
FROM union_bookings ub
         LEFT JOIN se.data.dim_sale ds ON ub.sale_id = ds.se_sale_id
         LEFT JOIN se.data.se_calendar sc ON ub.date = sc.date_value
         LEFT JOIN se.data.se_calendar csc ON ub.check_in_date = csc.date_value;



SELECT ds.se_sale_id,
       ds.sale_name,
       ds.sale_product,
       ds.sale_type,
       ds.product_type,
       ds.product_configuration,
       ds.product_line,
       ds.data_model,
       ds.sale_start_date,
       ds.sale_end_date,
       ds.sale_active,
       ds.posa_territory,
       ds.posa_country,
       ds.posu_country,
       ds.posu_division,
       ds.posu_city,
       ds.travel_type,
       ds.target_account_list,
       ds.posu_sub_region,
       ds.posu_region,
       ds.posu_cluster,
       ds.tech_platform,
       se.data.posa_category_from_territory(ds.posa_territory) AS posa_category
FROM se.data.dim_sale ds

------------------------------------------------------------------------------------------------------------------------


WITH union_bookings AS (

    SELECT gb.booking_completed_date AS date,
           'gross'                   AS reporting_status,
           gb.booking_id,
           gb.booking_status,
           gb.booking_status_type,
           gb.se_sale_id,
           gb.shiro_user_id,
           gb.check_in_date,
           gb.check_out_date,
           gb.booking_lead_time_days,
           gb.booking_created_date,
           gb.booking_completed_date,
           gb.booking_transaction_completed_date,
           gb.gross_revenue_gbp,
           gb.gross_revenue_gbp_constant_currency,
           gb.gross_revenue_eur_constant_currency,
           gb.customer_total_price_gbp,
           gb.customer_total_price_gbp_constant_currency,
           gb.gross_booking_value_gbp,
           gb.commission_ex_vat_gbp,
           gb.booking_fee_net_rate_gbp,
           gb.payment_surcharge_net_rate_gbp,
           gb.insurance_commission_gbp,
           gb.margin_gross_of_toms_gbp,
           gb.margin_gross_of_toms_gbp_constant_currency,
           gb.margin_gross_of_toms_eur_constant_currency,
           gb.no_nights,
           gb.adult_guests,
           gb.child_guests,
           gb.infant_guests,
           gb.price_per_night,
           gb.price_per_person_per_night,
           gb.rooms,
           gb.device_platform,
           gb.booking_full_payment_complete,
           gb.cancellation_date,
           gb.cancellation_reason,
           gb.territory,
           gb.travel_type,
           gb.tech_platform
    FROM se.data.fact_booking gb
    WHERE gb.booking_status_type IN ('live', 'cancelled') --all bookings that have ever confirmed

    UNION ALL

    SELECT cb.cancellation_date AS date,
           'cancelled'          AS reporting_status,
           cb.booking_id,
           NULL                 AS booking_status,
           NULL                 AS booking_status_type,
           cb.se_sale_id,
           cb.shiro_user_id,
           cb.check_in_date,
           cb.check_out_date,
           cb.booking_lead_time_days,
           cb.booking_created_date,
           cb.booking_completed_date,
           cb.booking_transaction_completed_date,
           cb.gross_revenue_gbp,
           cb.gross_revenue_gbp_constant_currency,
           cb.gross_revenue_eur_constant_currency,
           cb.customer_total_price_gbp,
           cb.customer_total_price_gbp_constant_currency,
           cb.gross_booking_value_gbp,
           cb.commission_ex_vat_gbp,
           cb.booking_fee_net_rate_gbp,
           cb.payment_surcharge_net_rate_gbp,
           cb.insurance_commission_gbp,
           cb.margin_gross_of_toms_gbp,
           cb.margin_gross_of_toms_gbp_constant_currency,
           cb.margin_gross_of_toms_eur_constant_currency,
           cb.no_nights,
           cb.adult_guests,
           cb.child_guests,
           cb.infant_guests,
           cb.price_per_night,
           cb.price_per_person_per_night,
           cb.rooms,
           cb.device_platform,
           cb.booking_full_payment_complete,
           cb.cancellation_date,
           cb.cancellation_reason,
           cb.territory,
           cb.travel_type,
           cb.tech_platform
    FROM se.data.fact_booking cb
    WHERE cb.booking_status_type = 'cancelled' --all bookings that are cancelled
)
SELECT ub.*,
       se.data.posa_category_from_territory(ub.territory) AS posa_category,
       sc.se_week                                         AS week,
       csc.se_week                                        AS check_in_week
FROM union_bookings ub
         LEFT JOIN se.data.se_calendar sc ON ub.date = sc.date_value
         LEFT JOIN se.data.se_calendar csc ON ub.check_in_date = csc.date_value
