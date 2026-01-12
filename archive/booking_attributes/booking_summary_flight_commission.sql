SELECT MIN(loaded_at) FROM raw_vault_mvp.cms_mongodb.booking_summary bs; --2020-01-15 17:14:57.464234000

airflow clear --start_date '2020-01-15 01:00:00' --end_date '2020-01-15 01:00:00' --task_regex '.*' hygiene_snapshots__cms_mongodb__booking_summary__daily_at_01h00
airflow backfill --start_date '2020-01-15 01:00:00' --end_date '2020-01-15 01:00:00' --task_regex '.*' hygiene_snapshots__cms_mongodb__booking_summary__daily_at_01h00


SELECT sb.sale_product,
       sb.has_flights,
       sb.flight_commission_gbp,
       sb.adult_guests,
       sb.child_guests,
       sb.infant_guests
FROM se.data.se_booking sb WHERE sb.transaction_id IN ('A7075-7775-885869', 'A9726-11342-1275487');


SELECT sb.sale_product,
       sb.has_flights,
       sb.flight_commission_gbp,
       sb.adult_guests,
       sb.child_guests,
       sb.infant_guests
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary sb WHERE sb.transaction_id IN ('A7075-7775-885869', 'A9726-11342-1275487');


SELECT sb.sale_product,
       sb.has_flights,
       sb.flight_commission_gbp,
       sb.adult_guests,
       sb.child_guests,
       sb.infant_guests
FROM data_vault_mvp.dwh.se_booking sb WHERE sb.transaction_id IN ('A7075-7775-885869', 'A9726-11342-1275487');

