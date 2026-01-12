WITH original_allocation AS (
    --dedupe allocation based on the earliest snapshot of it
    SELECT *
    FROM data_vault_mvp.dwh.hotel_room_inventory hri
        QUALIFY rank() OVER (PARTITION BY hri.hotel_code ORDER BY hri.view_date) = 1
)
--aggregate allocation up to hotel
SELECT oa.hotel_code,
       oa.inventory_date,
       oa.view_date AS original_view_date,
       SUM(oa.no_total_rooms)     AS no_total_rooms,
       SUM(oa.no_available_rooms) AS no_available_rooms,
       SUM(oa.no_booked_rooms)    AS no_booked_rooms,
       SUM(oa.no_closedout_rooms) AS no_closedout_rooms
FROM original_allocation oa
WHERE oa.hotel_code = '001w000001DVHS5'
GROUP BY 1, 2, 3
;

self_describing_task --include 'se/data/se_hotel_availability_original.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/se/data/se_hotel_availability_original.py

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.hotel_room_inventory clone data_vault_mvp.dwh.hotel_room_inventory;
SELECT min(original_view_date) FROM se.data.se_hotel_availability_original;


SELECT * FROM se.data.scv_touch_marketing_channel stmc

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/dv/dwh/transactional/se_booking.py


SELECT * FROM data_vault_mvp.travelbird_cms.orders_order_snapshot oos

dataset_task --include 'travelbird_mysql.orders_hotelorderitem' --operation ProductionIngestOperation --method 'run' --upstream --start '2019-04-01 00:30:00' --end '2019-04-01 00:30:00'
airflow backfill --start_date '2019-04-01 00:30:00' --end_date '2019-04-01 00:30:00' --task_regex '.*' incoming__travelbird_mysql__orders_hotelorderitem__daily_at_00h30
airflow backfill --start_date '2020-09-08 00:30:00' --end_date '2020-09-08 00:30:00' --task_regex '.*' incoming__travelbird_mysql__orders_hotelorderitem__daily_at_00h30

SELECT * FROM raw_vault_mvp_dev_robin.travelbird_mysql.orders_hotelorderitem;


SELECT * FROM se.data.se_sale_tags sst WHERE sst.tag_name = 'ski';