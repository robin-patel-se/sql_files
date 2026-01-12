SELECT * FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs WHERE bs.date_time_booked >= current_date -1
;


SELECT sb.booking_created_date,
       sb.commission_ex_vat_cc,
       sb.booking_fee_net_rate_cc
       FROM data_vault_mvp.dwh.se_booking sb WHERE sb.booking_status = 'COMPLETE'
AND sb.booking_created_date >= '2020-07-01'
AND sb.booking_created_date <= '2020-07-20';

--2020-07-05 last date where commission ex vat is in the wrong place


SELECT MIN(bs.updated_at)
FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs; -- 2020-02-28 09:44:26.241000000

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary clone hygiene_vault_mvp.cms_mongodb.booking_summary;

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;

self_describing_task --include 'hygiene_snapshots/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-02-28 00:00:00' --end '2020-02-28 00:00:00'

SELECT bs.date_time_booked,
       commission_ex_vat_cc,
       bs.booking_fee_net_rate_cc,
       bs.record__o:commissionExVat/100,
       bs.record__o:bookingFeeNetRate/100,
       bs.record__o
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
WHERE date_time_booked >= '2020-07-01'
AND date_time_booked <= '2020-07-20';

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary clone hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

--2020-07-06 adjust mongo records prior to this date: 2020-07-06

USE WAREHOUSE pipe_xlarge;
UPDATE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary target
SET target.commission_ex_vat_cc = batch.record__o:commissionExVat/100,
    target.booking_fee_net_rate_cc = batch.record__o:bookingFeeNetRate/100
from hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary  batch
WHERE target.date_time_booked <= '2020-07-06';



GRANT USAGE ON SCHEMA COLLAB.MY_SCHEMA TO ROLE personal_role__robinpatel;
GRANT SELECT ON ALL TABLES IN SCHEMA COLLAB.MY_SCHEMA TO ROLE personal_role__robinpatel;
GRANT SELECT ON ALL TABLES IN VIEWS COLLAB.MY_SCHEMA TO ROLE personal_role__robinpatel;


SELECT COUNT(*) FROm hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;
SELECT COUNT(*) FROm hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;


SELECT bs.date_time_booked,
       commission_ex_vat_cc,
       bs.booking_fee_net_rate_cc,
       bs.record__o:commissionExVat/100,
       bs.record__o:bookingFeeNetRate/100,
       bs.record__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE date_time_booked >= '2020-07-01'
AND date_time_booked <= '2020-07-20';


SELECT bs.date_time_booked,
       commission_ex_vat_cc,
       bs.booking_fee_net_rate_cc,
       bs.record__o:commissionExVat/100,
       bs.record__o:bookingFeeNetRate/100,
       bs.record__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE date_time_booked >= '2020-10-08';

SELECT * FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE date_time_booked >= '2020-07-01'
AND date_time_booked <= '2020-07-20';


SELECT bs.date_time_booked,
       commission_ex_vat_cc,
       bs.booking_fee_net_rate_cc,
       bs.record__o:commissionExVat/100,
       bs.record__o:bookingFeeNetRate/100,
       bs.record__o
FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs
WHERE date_time_booked >= '2020-07-01'
AND date_time_booked <= '2020-07-20'
ORDER BY date_time_booked;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE raw_vault_mvp.cms_mongodb.booking_summary;
SELECT MIN(LOADED_AT) fROM raw_vault_mvp_dev_robin.cms_mongodb.booking_summary bs; --2020-01-15 17:14:57.464234000

self_describing_task --include 'hygiene_snapshots/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-01-15 00:00:00' --end '2020-01-15 00:00:00'
DROP TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;


SELECT bs.date_time_booked,
       commission_ex_vat_cc,
       bs.booking_fee_net_rate_cc,
       bs.record__o:commissionExVat/100,
       bs.record__o:bookingFeeNetRate/100,
       bs.record__o
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE date_time_booked >= '2020-07-01'
AND date_time_booked <= '2020-07-20'
ORDER BY date_time_booked;
