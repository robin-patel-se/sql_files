SELECT sb.transaction_id,
       pps.name
FROM data_vault_mvp.dwh.se_booking sb
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.base_offer bo ON sb.offer_id = 'A' || bo.id
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bops ON bo.id = bops.base_offer_products_id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.product p ON bops.product_id = p.id AND p.class = 'com.flashsales.product.HotelProduct'
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot pps ON p.product_provider_id = pps.id
WHERE sb.booking_status = 'COMPLETE'
  AND LEFT(sb.booking_id, 1) = 'A';


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.allocation CLONE hygiene_snapshot_vault_mvp.cms_mysql.allocation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.amendment CLONE hygiene_snapshot_vault_mvp.cms_mysql.amendment;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.booking_allocations_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.booking_allocations_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.booking_cancellation CLONE data_vault_mvp.dwh.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.days_before_policy CLONE hygiene_snapshot_vault_mvp.cms_mysql.days_before_policy;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mari_reservation_information CLONE data_vault_mvp.dwh.mari_reservation_information;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.product CLONE hygiene_snapshot_vault_mvp.cms_mysql.product;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_reservation CLONE latest_vault.cms_mysql.product_reservation;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation CLONE latest_vault.cms_mysql.reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_base_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_base_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_exchange_rate CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_exchange_rate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_details;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_provider_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.flightservice__order_orderchange CLONE data_vault_mvp.dwh.flightservice__order_orderchange;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2022-04-06 00:00:00' --end '2022-04-06 00:00:00'



SELECT *
FROM se.data.scv_touched_searches sts;

SELECT *
FROM se.data.se_credit sc;



SELECT ssssssb.booking_id,
       pps.name
FROM data_vault_mvp.dwh.se_booking ssssssb
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.base_offer bo ON ssssssb.offer_id = 'A' || bo.id
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bops ON bo.id = bops.base_offer_products_id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.product p ON bops.product_id = p.id AND p.class = 'com.flashsales.product.HotelProduct'
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot pps ON p.product_provider_id = pps.id
WHERE pps.name IS NOT NULL
    QUALIFY COUNT(*) OVER (PARTITION BY ssssssb.booking_id) > 1


SELECT * FROM data_vault_mvp.data_quality.data_quality_checks dqc WHERE dqc.date = current_date-1;

SELECT * FROM se.data.se_booking sb WHERE sb.booking_completed_date = current_date-1 AND sb.booking_status = 'COMPLETE'