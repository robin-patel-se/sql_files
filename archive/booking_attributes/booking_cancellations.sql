-- Hey, Is there a code in hand that's pulls deal count split by week, territory, Domestic and international split and plus by sale types i.e. hotel, WRD etc?

SELECT sa.view_date,
       sa.se_sale_id,
       sa.sale_active,
       sa.sale_id,
       sa.base_sale_id,
       sa.tb_offer_id,
       sa.sale_start_date,
       sa.sale_end_date,
       sa.active,
       sa.tech_platform
FROM data_vault_mvp.dwh.sale_active sa

/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
se/
DATA/
sale_active.py
self_describing_task --include 'se/data/sale_active.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
e
SELECT *
FROM se_dev_robin.data.sale_active;

SELECT *
FROM se.data.se_hotel_rooms_and_rates

SELECT fcb.booking_id,
       fcb.booking_status,
       fcb.sale_id,
       fcb.shiro_user_id,
       fcb.check_in_date,
       fcb.check_out_date,
       fcb.booking_lead_time_days,
       fcb.booking_created_date,
       fcb.booking_completed_date,
       fcb.customer_total_price_gbp,
       fcb.customer_total_price_gbp_constant_currency,
       fcb.gross_booking_value_gbp,
       fcb.commission_ex_vat_gbp,
       fcb.booking_fee_net_rate_gbp,
       fcb.payment_surcharge_net_rate_gbp,
       fcb.insurance_commission_gbp,
       fcb.margin_gross_of_toms_gbp,
       fcb.margin_gross_of_toms_gbp_constant_currency,
       fcb.no_nights,
       fcb.adult_guests,
       fcb.child_guests,
       fcb.infant_guests,
       fcb.price_per_night,
       fcb.price_per_person_per_night,
       fcb.tech_platform
FROM se.data.fact_complete_booking fcb;


CREATE OR REPLACE VIEW collab.cms_mysql.se_booking_cancellation COPY GRANTS AS
(
SELECT bcs.id,
       COALESCE(bcs.booking_id::VARCHAR, 'A' || bcs.reservation_id) AS booking_id,
--        bcs.booking_id,
       bcs.date_created,
       bcs.last_updated,
       bcs.fault,
       bcs.reason,
       bcs.booking_fee,
       bcs.cc_fee,
       bcs.hotel_good_will,
       bcs.refund_channel,
       bcs.refund_type,
       bcs.se_good_will,
       bcs.who_pays,
--        bcs.reservation_id,
       bcs.cancel_with_provider,
       bcs.extract_metadata
FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot bcs
    );


CREATE SCHEMA collab.cms_mysql;
GRANT USAGE ON SCHEMA collab.cms_mysql TO ROLE personal_role__raquelhipolito;
GRANT SELECT ON VIEW collab.cms_mysql.se_booking_cancellation TO ROLE personal_role__raquelhipolito;

SELECT * FROM collab.cms_mysql.se_booking_cancellation sbc;

