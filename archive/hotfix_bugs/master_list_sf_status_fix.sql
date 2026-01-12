self_describing_task --include 'dv/dwh/master_booking_list/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/master_booking_list/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
dv/
dwh/
master_booking_list/
master_se_booking_list.py
SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl;


self_describing_task --include 'se/data_pii/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data_pii/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data_pii/master_all_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_tb_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/master_all_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data_pii.master_se_booking_list msbl;
SELECT *
FROM se_dev_robin.data_pii.master_tb_booking_list msbl;
SELECT *
FROM se_dev_robin.data_pii.master_all_booking_list msbl;
SELECT *
FROM se_dev_robin.data.master_se_booking_list msbl;
SELECT *
FROM se_dev_robin.data.master_tb_booking_list msbl;
SELECT *
FROM se_dev_robin.data.master_all_booking_list msbl;

SELECT *
FROM se.data.se_hotel_rooms_and_rates;

SELECT sb.booking_status, count(*)
FROM data_vault_mvp.dwh.se_booking sb
GROUP BY 1;

SELECT tb.payment_status, count(*)
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY 1;