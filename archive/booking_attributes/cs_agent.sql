SELECT *
FROM raw_vault_mvp.cms_mysql.reservation r;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.reservation_snapshot rs;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.booking_snapshot bs;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2020-11-23 00:00:00' --end '2020-11-23 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot CLONE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_reservation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.days_before_policy_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.days_before_policy_snapshot;

SELECT DISTINCT cs_agent_booking
FROM data_vault_mvp_dev_robin.dwh.se_booking sb;

self_describing_task --include 'se/data/dwh/se_booking.py'  --method 'run' --start '2020-11-23 00:00:00' --end '2020-11-23 00:00:00'

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE e.collector_tstamp >= current_date - 1
  AND e.event_name IN ('transaction',
                       'transaction_item'
    )
and e.is_server_side_event