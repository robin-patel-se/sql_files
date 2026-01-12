CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation CLONE hygiene.cms_mysql.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.booking_cancellation CLONE raw.cms_mysql.booking_cancellation;

ALTER TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN booking_fee_gbp TO booking_fee_cc;
ALTER TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN cc_fee_gbp TO cc_fee_cc;
ALTER TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN hotel_good_will_gbp TO hotel_good_will_cc;
ALTER TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN se_good_will_gbp TO se_good_will_cc;

self_describing_task --include 'staging/hygiene/cms_mysql/booking_cancellation.py'  --method 'run' --start '2021-05-12 00:00:00' --end '2021-05-12 00:00:00'


ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN booking_fee_gbp TO booking_fee_cc;
ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN cc_fee_gbp TO cc_fee_cc;
ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN hotel_good_will_gbp TO hotel_good_will_cc;
ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    RENAME COLUMN se_good_will_gbp TO se_good_will_cc;

self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/booking_cancellation.py'  --method 'run' --start '2021-05-12 00:00:00' --end '2021-05-12 00:00:00'

self_describing_task --include 'dv/dwh/transactional/se_booking_cancellation.py'  --method 'run' --start '2021-05-12 00:00:00' --end '2021-05-12 00:00:00'

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-05-12 00:00:00' --end '2021-05-12 00:00:00';


SELECT sb.currency,
       sb.gross_revenue_cc,
       sb.total_refunded_cc,
       sb.gross_revenue_gbp,
       sb.total_refunded_gbp,
       sb.sale_base_currency,
       sb.gross_revenue_sc,
       sb.total_refunded_sc,
       sb.refund_type
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.refund_type = 'REFUNDED';

DROP TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot;
DROP TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_exchange_rate_snapshot;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.amendment_snapshot;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.product_reservation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.days_before_policy_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.days_before_policy_snapshot;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_exchange_rate_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.reservation_exchange_rate_snapshot;


SELECT SUM(sb.total_refunded_cc)  AS total_refunded_cc,
       SUM(sb.total_refunded_gbp) AS total_refunded_gbp,
       SUM(sb.total_refunded_sc)  AS total_refunded_sc
FROM data_vault_mvp_dev_robin.dwh.se_booking sb;

SELECT SUM(sb.total_refunded_cc)  AS total_refunded_cc,
       SUM(sb.total_refunded_gbp) AS total_refunded_gbp,
       SUM(sb.total_refunded_sc)  AS total_refunded_sc
FROM data_vault_mvp.dwh.se_booking sb;