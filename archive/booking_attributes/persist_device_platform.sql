self_describing_task --include 'dv/dwh/transactional/se_booking'  --method 'run' --start '2020-02-28 03:00:00' --end '2020-02-28 03:00:00'

SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking; --2020-02-28 09:57:08.705000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation; --2020-02-28 09:58:13.350000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary; --2020-02-28 09:57:17.145000000

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;


SELECT * FROM data_vault_mvp_dev_robin.dwh.se_booking;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking (

    -- (lineage) metadata for the current job
        schedule_tstamp TIMESTAMP,
        run_tstamp TIMESTAMP,
        operation_id VARCHAR,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,

    --Base field and unique IDs
        booking_id VARCHAR NOT NULL,
        transaction_id VARCHAR,
        unique_transaction_reference VARCHAR,

    --Last update
        last_updated TIMESTAMP,
        last_updated_booking_summary TIMESTAMP,
        last_updated_bookings TIMESTAMP,
        last_updated_reservations TIMESTAMP,

    --Key dimensions
        territory VARCHAR,
        booking_status VARCHAR,
        currency VARCHAR,
        booking_completed_date DATE,
        booking_created_date DATE,
        booking_completed_timestamp TIMESTAMP,
        booking_created_timestamp TIMESTAMP,

    --User Info
        shiro_user_id NUMBER,
        affiliate_user_id NUMBER,
        device_platform VARCHAR,

    --Financials
        rate_to_gbp FLOAT,
        gross_booking_value_cc FLOAT,
        vat_on_commission_cc FLOAT,
        commission_ex_vat_cc FLOAT,
        booking_fee_net_rate_cc FLOAT,
        payment_surcharge_net_rate_cc FLOAT,
        insurance_commission_cc FLOAT,
        flight_commission_cc FLOAT,
        gross_booking_value_gbp FLOAT,
        vat_on_commission_gbp FLOAT,
        commission_ex_vat_gbp FLOAT,
        booking_fee_net_rate_gbp FLOAT,
        payment_surcharge_net_rate_gbp FLOAT,
        insurance_commission_gbp FLOAT,
        flight_commission_gbp FLOAT,
        margin_gross_of_toms_gbp FLOAT,

    --Booking Detail
        sale_id VARCHAR,
        offer_id VARCHAR,
        bundle_id VARCHAR,
        check_in_timestamp TIMESTAMP,
        check_in_date DATE,
        check_out_timestamp TIMESTAMP,
        check_out_date DATE,
        booking_lead_time_days NUMBER,
        booking_type VARCHAR,
        no_nights NUMBER,
        adult_guests NUMBER,
        child_guests NUMBER,
        infant_guests NUMBER,
        sale_type VARCHAR,
        has_flights VARCHAR,

    --Misc
        is_new_model_booking NUMBER,
        affiliate_id VARCHAR,
        affiliate VARCHAR,
        affiliate_domain VARCHAR,
        agent_id VARCHAR,
        payment_id NUMBER,
        hold_id NUMBER
    );


INSERT INTO data_vault_mvp_dev_robin.dwh.se_booking
SELECT b.schedule_tstamp,
       b.run_tstamp,
       b.operation_id,
       b.created_at,
       b.updated_at,
       b.booking_id,
       b.transaction_id,
       b.unique_transaction_reference,
       b.last_updated,
       b.last_updated_booking_summary,
       b.last_updated_bookings,
       b.last_updated_reservations,
       b.territory,
       b.booking_status,
       b.currency,
       b.booking_completed_date,
       b.booking_created_date,
       b.booking_completed_timestamp,
       b.booking_created_timestamp,
       b.shiro_user_id,
       b.affiliate_user_id,
        CASE
                WHEN bs.record__o['platformName']::VARCHAR = 'IOS_APP' THEN 'native app'
                WHEN bs.record__o['platformName']::VARCHAR = 'WEB' THEN 'web'
                WHEN bs.record__o['platformName']::VARCHAR = 'TABLET_WEB' THEN 'tablet web'
                WHEN bs.record__o['platformName']::VARCHAR = 'MOBILE_WEB' THEN 'mobile web'
                WHEN bs.record__o['platformName']::VARCHAR = 'MOBILE_WRAP_IOS' THEN 'mobile wrap ios'
                WHEN bs.record__o['platformName']::VARCHAR = 'MOBILE_WRAP_ANDROID' THEN 'mobile wrap android'
                WHEN bs.record__o['platformName']::VARCHAR = 'ANDROID_APP' THEN 'mobile wrap android'
                WHEN bs.record__o['platformName']::VARCHAR = 'IOS_APP_V3' THEN 'native app'
                ELSE 'not specified'
        END AS device_platform,
       b.rate_to_gbp,
       b.gross_booking_value_cc,
       b.vat_on_commission_cc,
       b.commission_ex_vat_cc,
       b.booking_fee_net_rate_cc,
       b.payment_surcharge_net_rate_cc,
       b.insurance_commission_cc,
       b.flight_commission_cc,
       b.gross_booking_value_gbp,
       b.vat_on_commission_gbp,
       b.commission_ex_vat_gbp,
       b.booking_fee_net_rate_gbp,
       b.payment_surcharge_net_rate_gbp,
       b.insurance_commission_gbp,
       b.flight_commission_gbp,
       b.margin_gross_of_toms_gbp,
       b.sale_id,
       b.offer_id,
       b.bundle_id,
       b.check_in_timestamp,
       b.check_in_date,
       b.check_out_timestamp,
       b.check_out_date,
       b.booking_lead_time_days,
       b.booking_type,
       b.no_nights,
       b.adult_guests,
       b.child_guests,
       b.infant_guests,
       b.sale_type,
       b.has_flights,
       b.is_new_model_booking,
       b.affiliate_id,
       b.affiliate,
       b.affiliate_domain,
       b.agent_id,
       b.payment_id,
       b.hold_id
FROM data_vault_mvp.dwh.se_booking b
LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON b.booking_id = bs.booking_id ;


--run in prod
CREATE OR REPLACE TABLE data_vault_mvp.dwh.se_booking clone data_vault_mvp_dev_robin.dwh.se_booking;

SELECT * FROM data_vault_mvp.dwh.se_booking;
