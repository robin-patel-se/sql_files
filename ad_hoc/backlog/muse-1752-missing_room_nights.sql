-- A6889 A10381

SELECT *
FROM se.data.fact_booking fb
WHERE fb.se_sale_id = 'A10341';

SELECT *
FROM se.data.tb_order_item toi
WHERE toi.order_id = 21905350;
SELECT *
FROM se.data.tb_order_item toi
WHERE toi.order_id = 21913750;

SELECT *
FROM se.data.dim_sale ds
WHERE ds.se_sale_id = 'A6889';

SELECT *
FROM se.data.dim_sale ds;
WHERE ds.sale_active
  AND ds.product_type = 'Package';



SELECT toi.order_id,
       toi.booking_id,
       toi.order_item_id,
       toi.order_item_type,
       toi.start_date,
       toi.end_date,

       fcb.se_sale_id,
       fcb.room_nights,
       fcb.rooms,
       ds.*
FROM se.data.tb_order_item toi
    INNER JOIN se.data.fact_complete_booking fcb ON toi.booking_id = fcb.booking_id
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE ds.sale_active
  AND ds.product_type = 'Package';



SELECT DISTINCT ds.se_sale_id
FROM se.data.tb_order_item toi
    INNER JOIN se.data.fact_complete_booking fcb ON toi.booking_id = fcb.booking_id
    INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE ds.sale_active
  AND ds.product_type = 'Package'
  AND fcb.room_nights IS NULL


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog CLONE data_vault_mvp.dwh.tb_order_item_changelog;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item CLONE data_vault_mvp.dwh.tb_order_item;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates CLONE data_vault_mvp.fx.tb_rates;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_person CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_person;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderevent CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderevent;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty;

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2022-02-13 00:00:00' --end '2022-02-13 00:00:00'


SELECT booking_id,
       rooms,
       room_nights
FROM data_vault_mvp_dev_robin.dwh.tb_booking
WHERE booking_id = 'TB-21917968';

SELECT booking_id,
       rooms,
       room_nights
FROM data_vault_mvp.dwh.tb_booking
WHERE booking_id = 'TB-21917968';

SELECT *
FROM data_vault_mvp.dwh.tb_order_item toi
WHERE toi.booking_id = 'TB-21917968';