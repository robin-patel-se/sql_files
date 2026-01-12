--bonding type calc field in tableau
-- {fixed [Booking ID] : MIN(if([NAME (ORDERS_ORDERPROPERTY_SNAPSHOT)]=='bonding_type' and ISNULL([VALUE])=FALSE)
-- then
-- [VALUE]
-- END) }

SELECT oos.order_id,
       MIN(oos.value)
FROM data_vault_mvp.travelbird_cms.orders_orderproperty_snapshot oos
WHERE name = 'bonding_type'
  AND oos.value IS NOT NULL
GROUP BY 1;

--for each order id take the minimumn

SELECT oos.order_id,
       MIN(oos.value)     AS mini,
       LISTAGG(oos.value) AS lista
FROM data_vault_mvp.travelbird_cms.orders_orderproperty_snapshot oos
WHERE name = 'bonding_type'
  AND oos.value IS NOT NULL
GROUP BY 1;


self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2020-11-23 00:00:00' --end '2020-11-23 00:00:00'

self_describing_task --include 'se/data/dwh/tb_booking.py'  --method 'run' --start '2020-11-23 00:00:00' --end '2020-11-23 00:00:00'
self_describing_task --include 'dv/dwh/master_booking_list/master_tb_booking_list.py'  --method 'run' --start '2020-11-23 00:00:00' --end '2020-11-23 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.credit_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.booking_credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.booking_credit_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.reservation_credit_snapshot;
create or replace TRANSIENT table DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS.BILLING_SNAPSHOT clone data_vault_mvp.CMS_MYSQL_SNAPSHOTS.BILLING_SNAPSHOT;
CREATE or REPLACE TRANSIENT TABLE DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS.SHIRO_USER_SNAPSHOT clone data_vault_mvp.CMS_MYSQL_SNAPSHOTS.SHIRO_USER_SNAPSHOT;
CREATE OR REPLACE TRANSIENT TABLE DATA_VAULT_MVP_DEV_ROBIN.CMS_MYSQL_SNAPSHOTS.TIME_LIMITED_CREDIT_SNAPSHOT CLONE data_vault_mvp.CMS_MYSQL_SNAPSHOTS.TIME_LIMITED_CREDIT_SNAPSHOT;
CREATE OR REPLACE TRANSIENT TABLE DATA_VAULT_MVP_DEV_ROBIN.TRAVELBIRD_CMS.ORDERS_PAYMENTMETHOD_SNAPSHOT CLONE data_vault_mvp.TRAVELBIRD_CMS.ORDERS_PAYMENTMETHOD_SNAPSHOT;

self_describing_task --include 'se/data/masterlist/master_tb_booking_list.py'  --method 'run' --start '2020-11-23 00:00:00' --end '2020-11-23 00:00:00'
self_describing_task --include 'se/data_pii/master_tb_booking_list.py'  --method 'run' --start '2020-11-23 00:00:00' --end '2020-11-23 00:00:00'