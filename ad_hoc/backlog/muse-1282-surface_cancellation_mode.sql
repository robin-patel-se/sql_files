CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation CLONE hygiene_snapshot_vault_mvp_mvp.cms_mysql.booking_cancellation;

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation;

self_describing_task --include 'dv/dwh/transactional/se_booking_cancellation.py'  --method 'run' --start '2021-11-17 00:00:00' --end '2021-11-17 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation;
SELECT requested_by_domain,
       COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation
GROUP BY 1
ORDER BY 2 DESC;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.allocation CLONE hygiene_snapshot_vault_mvp.cms_mysql.allocation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.amendment CLONE hygiene_snapshot_vault_mvp.cms_mysql.amendment;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.booking_allocations_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.booking_allocations_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.days_before_policy CLONE hygiene_snapshot_vault_mvp.cms_mysql.days_before_policy;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.product_reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.product_reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_base_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_base_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_exchange_rate CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_exchange_rate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-11-17 00:00:00' --end '2021-11-17 00:00:00'

SELECT soa.offer_name_object,
       soa.offer_name_object['de_DE']::VARCHAR as de_de_offer_name
       FROM se.data.se_offer_attributes soa
;


