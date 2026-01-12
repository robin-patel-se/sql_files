USE WAREHOUSE pipe_medium;
--module 3

SELECT rp.room_type_id                AS room_type_id,
       rp.id                          AS rate_plan_id,
       rp.name                        AS rate_plan_name,
       rp.code                        AS rate_plan_code,
       rp.rack_code                   AS rate_plan_rack_code,
       rp.code || ':' || rp.rack_code AS rate_plan_code_rack_code, -- this is the field to join to the SE CMS
       rp.free_children,
       rp.free_infants,
       rp.cts_commission              AS cash_to_settle_commission,
       rp.currency,
       r.id                           AS rate_id,
       r.date                         AS date,
       r.rate,
       r.rack_rate,
       r.single_rate,
       r.child_rate,
       r.infant_rate,
       r.min_los                      AS min_length_of_stay,
       r.max_los                      AS max_length_of_stay,
       c.id                           AS cash_to_settle_rate_id,
       c.cts_rate,
       c.cts_single_rate,
       c.cts_infant_rate,
       c.cts_child_rate
FROM data_vault_mvp_dev_robin.mari_snapshots.rate_plan_snapshot rp
         INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.rate_snapshot r ON r.rate_plan_id = rp.id
         LEFT JOIN data_vault_mvp_dev_robin.mari_snapshots.cash_to_settle_rate_snapshot c ON r.id = c.rate_id
ORDER BY 1, 2, 11;

SELECT count(*)
FROM data_vault_mvp_dev_robin.mari_snapshots.hotel_snapshot;

SELECT count(*)
FROM data_vault_mvp.cms_mysql_snapshots.hotel_snapshot;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_sale_offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot;

SELECT *
FROM data_vault_mvp.dwh.se_offer so;

self_describing_task --include 'dv/dwh/transactional/se_offer'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'