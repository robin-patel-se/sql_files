SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking
WHERE status = 'REFUNDED';

SELECT *
FROM hygiene_vault_mvp.cms_mongodb.booking_summary
WHERE booking_id = '54457870';


SELECT date_trunc(MONTH, booking_created_date), booking_status, count(*)
FROM data_vault_mvp.dwh.se_booking
WHERE sale_id IS NULL
  AND booking_created_date >= '2013-01-01'
GROUP BY 1, 2;


SELECT *
FROM data_vault_mvp.dwh.se_booking
WHERE sale_id IS NULL
  AND created_at >= '2020-01-01'
  AND booking_status = 'COMPLETE';

SELECT *
FROM hygiene_vault_mvp.cms_mongodb.booking_summary
WHERE booking_id IN ('54635423',
                     '54293317',
                     '54283444',
                     '54298532',
                     '54294219',
                     '54293342',
                     '54289025',
                     '54272310',
                     '54294784',
                     '54259083',
                     '54267477',
                     '54218519',
                     '54197110',
                     '54136874'
    );

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary
WHERE booking_id IN ('52769860',
                     '51974591',
                     '54259083'
    );


--updating the entire table
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;


self_describing_task --include 'dv/dwh_rec/transactional/se_booking'  --method 'run' --start '2020-02-27 00:00:00' --end '2020-02-27 00:00:00'

SELECT min(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking
SELECT min(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation
SELECT min(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary

--found instances where the mongo data is not polated in the se_booking table for complete bookings even though its in the hygiene snapshot for booking summary

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary
WHERE booking_id IN
      (SELECT booking_id
       FROM data_vault_mvp.dwh.se_booking
       WHERE booking_status = 'COMPLETE'
         AND last_updated_booking_summary IS NULL)

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking
WHERE booking_id IN
      (SELECT booking_id
       FROM data_vault_mvp.dwh.se_booking
       WHERE booking_status = 'COMPLETE'
         AND last_updated_booking_summary IS NULL);

CREATE OR REPLACE TABLE update_se_booking AS (
    SELECT *
    FROM data_vault_mvp_dev_robin.dwh.se_booking
    WHERE booking_id IN
          (SELECT booking_id
           FROM data_vault_mvp.dwh.se_booking
           WHERE booking_status = 'COMPLETE'
             AND last_updated_booking_summary IS NULL)
      AND last_updated_booking_summary IS NOT NULL
);

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking_updated CLONE data_vault_mvp.dwh.se_booking;

DELETE
FROM data_vault_mvp_dev_robin.dwh.se_booking_updated
WHERE booking_id IN (SELECT booking_id FROM update_se_booking);

INSERT INTO data_vault_mvp_dev_robin.dwh.se_booking_updated
SELECT schedule_tstamp,
       run_tstamp,
       operation_id,
       created_at,
       current_timestamp::TIMESTAMP as updated_at,
       booking_id,
       transaction_id,
       unique_transaction_reference,
       last_updated,
       last_updated_booking_summary,
       last_updated_bookings,
       last_updated_reservations,
       territory,
       booking_status,
       currency,
       booking_completed_date,
       booking_created_date,
       booking_completed_timestamp,
       booking_created_timestamp,
       shiro_user_id,
       affiliate_user_id,
       rate_to_gbp,
       gross_booking_value_cc,
       vat_on_commission_cc,
       commission_ex_vat_cc,
       booking_fee_net_rate_cc,
       payment_surcharge_net_rate_cc,
       insurance_commission_cc,
       flight_commission_cc,
       gross_booking_value_gbp,
       vat_on_commission_gbp,
       commission_ex_vat_gbp,
       booking_fee_net_rate_gbp,
       payment_surcharge_net_rate_gbp,
       insurance_commission_gbp,
       flight_commission_gbp,
       margin_gross_of_toms_gbp,
       sale_id,
       offer_id,
       bundle_id,
       check_in_timestamp,
       check_in_date,
       check_out_timestamp,
       check_out_date,
       booking_lead_time_days,
       booking_type,
       no_nights,
       adult_guests,
       child_guests,
       infant_guests,
       sale_type,
       has_flights,
       is_new_model_booking,
       affiliate_id,
       affiliate,
       affiliate_domain,
       agent_id,
       payment_id,
       hold_id
FROM update_se_booking;



SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking_updated
WHERE booking_status = 'COMPLETE'
  AND last_updated_booking_summary IS NULL;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking_bkup clone data_vault_mvp.dwh.se_booking;

CREATE OR REPLACE TABLE data_vault_mvp.dwh.se_booking clone data_vault_mvp_dev_robin.dwh.se_booking_updated;

SELECT *
FROM data_vault_mvp.dwh.se_booking
WHERE booking_status = 'COMPLETE'
  AND last_updated_booking_summary IS NULL;