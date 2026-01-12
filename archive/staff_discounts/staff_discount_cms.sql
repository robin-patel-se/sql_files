-- JC is looking at how often it’s being used
-- Spoke to Mike:
-- credit table
-- reason == Staff Booking
-- (case sensitive)
-- and status USED for those which have been used
-- and date > 14/07/2020
-- since that is when the change was released
-- as a note - I'm seeing zero uses sweat_smile
-- one sec
-- reason == Staff Booking [Actioned by% will be the charm
-- if you want to be thorough, check that the "Actioned by" username matches that of the User that has the credit
-- but I think the above will get what you need and filter out the manually done ones (since the manually done ones seem to have commission notes)


SELECT sc.redeemed_se_booking_id,
       LISTAGG(DISTINCT IFF(sc.credit_reason LIKE 'Staff Booking [Actioned by:%', 'Automated', 'Manual'),
               ', ')                AS staff_discount_type,
       COUNT(DISTINCT sc.credit_id) AS credits_used
FROM se.data_pii.se_credit sc
WHERE LOWER(sc.credit_reason) LIKE '%staff booking%'
  AND sc.redeemed_se_booking_id IS NOT NULL
GROUP BY 1
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.amendment_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_reservation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.booking_cancellation CLONE data_vault_mvp.dwh.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.days_before_policy_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.days_before_policy_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_exchange_rate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.reservation_exchange_rate_snapshot;

SELECT NULL = TRUE

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-02-18 00:00:00' --end '2021-02-18 00:00:00'

